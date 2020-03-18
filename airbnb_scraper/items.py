# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import json
import arrow
import math
import scrapy
from scrapy.loader.processors import MapCompose, TakeFirst, Join
from scrapy.exporters import BaseItemExporter
from airbnb_scraper.settings import PROJECT_VERSION
from datetime import datetime

ID_KEY = '_id'
MISSING_VALUE_SENTINEL = object()
BLOCKED_DAYS_FAR_THRESHOLD = 30


def date_serializer(x):
    return arrow.get(x).datetime if bool(x) else None

def naive_date_serializer(x):
    return arrow.get(x).replace(tzinfo='UTC').datetime if bool(x) else None

def remove_unicode(value):
    return value.replace(u"\u201c", '').replace(u"\u201d", '').replace(u"\2764", '').replace(u"\ufe0f")


class AirbnbItem(scrapy.Item):

    _item_type = ''
    _collection_name = ''
    _stale_interval = math.inf

    _id = scrapy.Field()
    item_type = scrapy.Field()
    creation_date = scrapy.Field(serializer=date_serializer)
    update_date = scrapy.Field(serializer=date_serializer)
    version = scrapy.Field()
    # hash_value = scrapy.Field()
    # _changes = scrapy.Field()

    def __init__(self, *args, _persisted_values=None, **kwargs):
        super().__init__(*args, **kwargs)
        self['item_type'] = type(self)._item_type or self.get('item_type', str(type(self).__name__))
        self['version'] = PROJECT_VERSION
        # self['hash_value'] = ''
        self._persisted_values = _persisted_values or {}

    def __repr__(self):
        type_str = type(self)._item_type or type(self).__name__
        id_str = self.get(ID_KEY)
        return f'{type_str}[{id_str}]'

    @property
    def is_stale(self):
        now = arrow.get()
        update_date = self.get_date_value('update_date', now)
        time_passed = (now - update_date).total_seconds()
        return time_passed > type(self)._stale_interval

    @classmethod
    def create(cls, *args, **kwargs):
        item = cls(*args, **kwargs)
        now = arrow.get()
        if not bool(item.get('creation_date')):
            item['creation_date'] = now
        if not bool(item.get('update_date')):
            item['update_date'] = now
        return item

    @classmethod
    def get_collection(cls):
        """
        Returns a MongoDB collection. Note that
        objects returned by this collection are
        not instances of the item. Use `with_db_entry()`
        to instantiate.
        """
        from airbnb_scraper.db import AirbnbMongoDB
        client = AirbnbMongoDB.shared()
        return client.db[cls._collection_name]

    @classmethod
    def load(cls, id):
        matches = list(cls.get_collection().find({ID_KEY: id}))
        if len(matches) == 0:
            return None
        elif len(matches) == 1:
            return cls.with_db_entry(matches[0])
        else:
            raise Exception(f'Multiple objects found for id {id}')

    @classmethod
    def with_db_entry(cls, data):
        # Get only the current values and filter
        # out legacy keys
        values = {}
        for key in cls.fields:
            if key in data:
                v = data[key]
                if isinstance(v, datetime):
                    v = arrow.get(v, tzinfo='UTC')
                values[key] = v
        assert bool(values[ID_KEY]), 'Database entry is missing an ID'
        return cls(_persisted_values=data, **values)
    
    @classmethod
    def save_many(cls,  items):
        # TODO: optimise with bulk insert
        # https://api.mongodb.com/python/current/tutorial.html#bulk-inserts
        for item in items:
            item.save()
    
    @classmethod
    def find(cls, *query, sort=None):
        if bool(query):
            query = query[0]
        else:
            query = {}
        col = cls.get_collection()
        docs = col.find(query)
        if sort:
            if isinstance(sort, dict):
                sort = [(k, v) for k, v in sort.items()]
            docs = docs.sort(sort)
        return map(cls.with_db_entry, docs)

    @classmethod
    def get_immutable_keys(cls):
        return [ID_KEY, 'creation_date']

    def get_id(self):
        return self.get(ID_KEY)

    def get_date_value(self, key, default=None):
        x = self.get(key)
        if not bool(x):
            return default
        return arrow.get(x)

    def validate(self):
        pass

    def serialize(self):
        return MongoDBItemExporter().export_item(self)
        # doc = {}
        # for key, field in type(self).fields.items():
        #     value = self[key]
        #     value = self.serialize_field(field, key, value)
        #     doc[key] = value
        # return doc

    def save(self, force=False, validate=True):
        if validate:
            self.validate()

        doc = self.serialize()

        changes = {}
        if not force or validate:
            changes = self.get_changes(_serialized_values=doc)
            if not bool(changes):
                return

        if validate:
            assert bool(changes)
            immutable_keys = type(self).get_immutable_keys()
            for key, change in changes.items():
                if key in immutable_keys and change['old'] is not None:
                    raise AttributeError(f'Key is read-only: {key}')

        collection = type(self).get_collection()
        collection.update_one(
            {ID_KEY: self[ID_KEY]},
            {'$set': doc},
            upsert=True
        )
        self._persisted_values = doc

    def get_changes(self, _serialized_values=None):
        olds = self._persisted_values
        news = _serialized_values or self.serialize()
        changes = {}
        for key in self.keys():
            old = olds[key] if key in olds else None
            new = news[key] if key in news else None
            if isinstance(old, datetime) and isinstance(new, datetime):
                changed = arrow.get(new) != arrow.get(old)
            else:
                changed = new != old
            if changed:
                changes[key] = {
                    'new' : new,
                    'old': old
                }
        return changes

    # def update_hash(self, id_sensitive=False):
    #     d = dict(self)
    #     del d.get_date_value('creation_date')
    #     del d['version']
    #     if 'hash_value' in d:
    #         del d['hash_value']
    #     if not id_sensitive and ID_KEY in d:
    #         del d[ID_KEY]
    #     self['hash_value'] = hash(json.dumps(d, sort_keys=True))
    #     return self['hash_value']


class AirbnbListing(AirbnbItem):

    _item_type = 'listing'
    _collection_name = 'listings'
    _stale_interval = 86400.0

    # source_hash = scrapy.Field()

    # Host Fields
    is_superhost = scrapy.Field()
    host_id = scrapy.Field()

    # Room Fields
    listing_id = scrapy.Field()
    public_address = scrapy.Field()
    rate = scrapy.Field()
    rate_with_service_fee = scrapy.Field()
    url = scrapy.Field()
    picture_url = scrapy.Field()
    is_business_travel_ready = scrapy.Field()
    is_fully_refundable = scrapy.Field()
    is_new_listing = scrapy.Field()
    lat = scrapy.Field()
    lng = scrapy.Field()
    time_zone = scrapy.Field()
    localized_city = scrapy.Field()
    localized_neighborhood = scrapy.Field()
    name = scrapy.Field(input_processor=MapCompose(remove_unicode))
    person_capacity = scrapy.Field()
    picture_count = scrapy.Field()
    reviews_count = scrapy.Field()
    min_nights = scrapy.Field()
    max_nights = scrapy.Field()
    property_type_id = scrapy.Field()
    room_type_category = scrapy.Field()
    room_and_property_type = scrapy.Field()
    star_rating = scrapy.Field() # Rounded to .5 or .0 Avg Rating
    avg_rating = scrapy.Field()
    can_instant_book = scrapy.Field()
    monthly_price_factor = scrapy.Field()
    currency = scrapy.Field()
    rate_type = scrapy.Field()
    weekly_price_factor = scrapy.Field()
    bathrooms = scrapy.Field()
    bedrooms = scrapy.Field()
    beds = scrapy.Field()
    amenity_ids = scrapy.Field()
    # accuracy = scrapy.Field()
    # communication = scrapy.Field()
    # cleanliness = scrapy.Field()
    # location = scrapy.Field()
    # checkin = scrapy.Field()
    # value = scrapy.Field()
    # guest_satisfication = scrapy.Field()
    # host_reviews = scrapy.Field()
    # response_rate = scrapy.Field()
    # response_time = scrapy.Field()

    @classmethod
    def create_id(cls, listing_id=''):
        if not bool(listing_id):
            raise ValueError('Missing ID parameter')
        return listing_id

    def update_id(self):
        self[ID_KEY] = type(self).create_id(listing_id=self['listing_id'])

# class AirbnbListingCalendar(AirbnbItem):

#     _item_type = 'calendar'

#     listing_id = scrapy.Field()
#     time_zone = scrapy.Field()
#     currency = scrapy.Field()
#     months = scrapy.Field()
#     start_date = scrapy.Field(serializer=naive_date_serializer)
#     end_date = scrapy.Field(serializer=naive_date_serializer)
#     # dates = scrapy.Field()
#     availability = scrapy.Field()
#     prices = scrapy.Field()

#     @classmethod
#     def create_id(cls, listing_id='', months=1, tzinfo='UTC'):
#         now_local = arrow.utcnow().to(tzinfo)
#         today_local = now_local.floor('day')
#         subid = today_local.format('YYYY-MM-DD')
#         return f'{listing_id}/{cls._item_type}/{subid}'


class AirbnbListingCalendarMonth(AirbnbItem):

    _item_type = 'month'
    _collection_name = 'months'
    _stale_interval = 3600.0

    # source_hash = scrapy.Field()

    listing_id = scrapy.Field()
    time_zone = scrapy.Field()
    currency = scrapy.Field()
    month = scrapy.Field()
    year = scrapy.Field()
    start_date = scrapy.Field(serializer=naive_date_serializer)
    data_start_date = scrapy.Field(serializer=naive_date_serializer)
    end_date = scrapy.Field(serializer=naive_date_serializer)
    availability = scrapy.Field()
    cancellation_rate = scrapy.Field()
    block_rate = scrapy.Field()
    revenue = scrapy.Field()
    partial_revenue = scrapy.Field()
    future_revenue = scrapy.Field()
    average_price = scrapy.Field()
    median_price = scrapy.Field()
    lowest_price = scrapy.Field()
    highest_price = scrapy.Field()
    errors = scrapy.Field(serializer=lambda a: [str(x) for x in list(a)])

    @classmethod
    def create_id(cls, listing_id='', date='', tzinfo='UTC'):
        if not bool(listing_id) or not bool(date):
            raise ValueError('Missing ID parameter')
        start_date = arrow.get(date, tzinfo=tzinfo).floor('month')
        subid = start_date.format('YYYY-MM')
        return f'{listing_id}/{cls._item_type}/{subid}'

    def update_id(self):
        self[ID_KEY] = type(self).create_id(
            listing_id=self.get('listing_id'),
            date=self.get_date_value('start_date'),
            tzinfo=self.get('time_zone')
        )

    def update_with_days(self, days):
        time_zone = self['time_zone']
        now = self.get_date_value('update_date')
        now_local = now.to(time_zone)
        today_local = now_local.floor('day')
        date = None
        data_start_date = None

        booked_or_cancelled_days = 0
        available_future_days = 0
        future_days = 0
        total_days = 0
        cancelled_days = 0
        blocked_days = 0
        is_data_complete = True
        errors = []
        prices = []
        revenue = 0.0
        future_revenue = 0.0
        _id = self.get_id()

        for day in days:
            assert day['month_id'] == _id, 'Mismatched day'
            is_past = day.is_past
            is_available = day.is_available
            is_booked = day.is_booked
            is_cancelled = day.is_cancelled
            is_blocked = day.is_blocked

            if day.is_data_complete:
                if not bool(data_start_date):
                    data_start_date = day.get_date_value('date')
                    self['data_start_date'] = data_start_date
            else:
                is_data_complete = False

            # Price
            price = day.get('price', 0.0)
            if bool(price):
                prices.append(price)
                if is_booked:
                    if is_past:
                        revenue += price
                    else:
                        future_revenue += price
            else:
                # Handle missing price
                errors.append(f'Price missing at {day.date.format("YYYY-MM-DD")}')

            total_days += 1
            assert total_days <= 31, 'Too many days'
            if not is_past:
                future_days += 1
                if is_available:
                    available_future_days += 1

            if is_cancelled:
                cancelled_days += 1
            if is_booked or is_cancelled:
                booked_or_cancelled_days += 1
            if is_blocked:
                blocked_days += 1

        if future_days > 0:
            availability = round(float(available_future_days) / float(future_days) * 100.0) / 100.0
        else:
            availability = 0.0
        self['availability'] = availability

        if booked_or_cancelled_days > 0:
            cancellation_rate = round(float(cancelled_days) / float(booked_or_cancelled_days) * 100.0) / 100.0
        else:
            cancellation_rate = 0.0
        self['cancellation_rate'] = cancellation_rate

        self['block_rate'] = round(float(blocked_days) / float(total_days) * 100.0) / 100.0

        if not bool(errors):
            prices.sort()

            if is_data_complete:
                self['revenue'] = revenue
            else:
                self['revenue'] = None

            self['partial_revenue'] = revenue
            self['future_revenue'] = future_revenue
            self['average_price'] = round(sum(prices) / float(total_days) * 100.0) / 100.0
            self['median_price'] = prices[int(total_days / 2)]
            self['lowest_price'] = prices[0]
            self['highest_price'] = prices[-1]
        else:
            self['revenue'] = None
            self['partial_revenue'] = None
            self['future_revenue'] = None
            self['average_price'] = None
            self['median_price'] = None
            self['lowest_price'] = None
            self['highest_price'] = None
        
        self['errors'] = errors


class AirbnbListingCalendarDay(AirbnbItem):

    _item_type = 'day'
    _collection_name = 'days'
    _stale_interval = 3600.0

    # source_hash = scrapy.Field()

    listing_id = scrapy.Field()
    month_id = scrapy.Field()
    time_zone = scrapy.Field()
    price = scrapy.Field()
    currency = scrapy.Field()
    date = scrapy.Field(serializer=naive_date_serializer)
    last_available_seen_date = scrapy.Field(serializer=date_serializer)
    first_unavailable_seen_date = scrapy.Field(serializer=date_serializer)
    available = scrapy.Field()
    booking_date = scrapy.Field(serializer=date_serializer)
    blocked = scrapy.Field()
    cancellations = scrapy.Field()
    cancellation_date = scrapy.Field(serializer=date_serializer)

    @classmethod
    def create_id(cls, listing_id='', date='', tzinfo='UTC'):
        if not bool(listing_id) or not bool(date):
            raise ValueError('Missing ID parameter')
        subid = arrow.get(date, tzinfo=tzinfo).format('YYYY-MM-DD')
        return f'{listing_id}/{cls._item_type}/{subid}'

    def update_id(self):
        self[ID_KEY] = type(self).create_id(
            listing_id=self['listing_id'],
            date=self.get_date_value('date'),
            tzinfo=self['time_zone']
        )

    @property
    def is_blocked(self):
        return bool(self.get('blocked'))

    @property
    def is_available(self):
        return bool(self.get('available'))

    @property
    def is_booked(self):
        if self.is_blocked:
            return False
        if self.is_past:
            booking_date = self.get_date_value('booking_date')
            if not bool(booking_date):
                return False
            cancellation_date = self.get_date_value('cancellation_date')
            if not bool(cancellation_date):
                return True
            return booking_date > cancellation_date
        else:
            return not self.is_available

    @property
    def is_cancelled(self):
        if self.is_blocked:
            return False
        cancellation_date = self.get_date_value('cancellation_date')
        if not bool(cancellation_date):
            return False
        booking_date = self.get_date_value('booking_date')
        if not bool(booking_date):
            return True
        return cancellation_date > booking_date

    @property
    def is_data_complete(self):
        if not self.is_past:
            return True
        if bool(self.get_date_value('last_available_seen_date')):
            return True
        return False

    @property
    def local_date(self):
        return arrow.get(self.get_date_value('date'), tzinfo=self['time_zone']).replace(tzinfo=self['time_zone'])

    @property
    def is_past(self):
        return arrow.get(self.get_date_value('update_date')) > self.local_date.ceil('day').shift(hours=-2)

    def validate(self):
        creation_date = self.get_date_value('creation_date')

        if creation_date is None:
            return

        av_date = self.get_date_value('last_available_seen_date')
        if av_date is not None and av_date < creation_date:
            raise ValueError(f'Day {self} was available ({av_date}) before created ({creation_date})')

        unav_date = self.get_date_value('first_unavailable_seen_date')
        if unav_date is not None and unav_date < creation_date:
            raise ValueError(f'Day {self} was unavailable ({unav_date}) before created ({creation_date})')

    def update_inferred(self):
        """Updates dependent properties."""
        now = self.get_date_value('update_date')
        is_past = self.is_past

        self['month_id'] = AirbnbListingCalendarMonth.create_id(
            listing_id=self.get('listing_id'),
            date=self.get_date_value('date'),
            tzinfo=self.get('time_zone')
        )

        if self.is_available:
            self['last_available_seen_date'] = now
            self['blocked'] = False
        elif not is_past and self.get_date_value('first_unavailable_seen_date') is None:
            self['first_unavailable_seen_date'] = now
        
        av_date = self.get_date_value('last_available_seen_date')
        unav_date = self.get_date_value('first_unavailable_seen_date')
        if bool(av_date) and bool(unav_date):
            mid_date = arrow.get(round((av_date.timestamp + unav_date.timestamp) / 2))
            if av_date < unav_date:
                if not self.is_past:
                    # Save estimated booking date
                    self['booking_date'] = mid_date
            elif self.get('cancellation_date') != mid_date:
                # Save cancellation date
                self['cancellation_date'] = mid_date
                self['cancellations'] = self.get('cancellations', 0) + 1

    @classmethod
    def update_group(cls, days):
        """
        If there is a large unavailable period at the
        end of the day set, consider the unavailable period
        as blocked.

        If a blocked day is found at the end of the day set,
        the unavailable period can be of any size.
        """
        unavailable_tail_days = []
        found_blocked = False
        if not isinstance(days, list):
            days = list(days)
        for day in reversed(days):
            if not found_blocked and day.is_blocked:
                found_blocked = True
            if day.is_available:
                break
            unavailable_tail_days.append(day)
        
        if found_blocked or len(unavailable_tail_days) >= BLOCKED_DAYS_FAR_THRESHOLD:
            # consider the unavailable period as blocked
            for day in unavailable_tail_days:
                day['blocked'] = True
        
        return unavailable_tail_days


class MongoDBItemExporter(BaseItemExporter):

    def export_item(self, item):
        doc = {}
        for key, field in type(item).fields.items():
            value = item.get(key, MISSING_VALUE_SENTINEL)
            if value == MISSING_VALUE_SENTINEL:
                if self.export_empty_fields:
                    value = None
                else:
                    continue
            value = self.serialize_field(field, key, value)
            doc[key] = value
        return doc
