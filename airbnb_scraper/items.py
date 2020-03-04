# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import json
import arrow
import scrapy
from scrapy.loader.processors import MapCompose, TakeFirst, Join
from scrapy.exporters import BaseItemExporter
from airbnb_scraper.settings import PROJECT_VERSION

ID_KEY = '_id'
STALE_INTERVAL = 60.0
MISSING_VALUE_SENTINEL = object()


def datetime_serializer(x):
    return arrow.get(x).replace(tzinfo='UTC').datetime if bool(x) else None

def naive_datetime_serializer(x):
    return arrow.get(x).replace(tzinfo='UTC').datetime if bool(x) else None

def remove_unicode(value):
    return value.replace(u"\u201c", '').replace(u"\u201d", '').replace(u"\2764", '').replace(u"\ufe0f")


class AirbnbItem(scrapy.Item):

    _item_type = ''
    _collection_name = ''

    _id = scrapy.Field()
    item_type = scrapy.Field()
    creation_date = scrapy.Field(serializer=datetime_serializer)
    update_date = scrapy.Field(serializer=datetime_serializer)
    version = scrapy.Field()
    # hash_value = scrapy.Field()
    # _changes = scrapy.Field()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self['item_type'] = type(self)._item_type or self.get('item_type', str(type(self).__name__))
        self['version'] = PROJECT_VERSION
        # self['hash_value'] = ''

    def __repr__(self):
        type_str = type(self)._item_type or type(self).__name__
        id_str = self.get(ID_KEY)
        return f'{type_str}[{id_str}]'

    @property
    def is_stale(self):
        now = arrow.get()
        update_date = self.get('update_date', now)
        time_passed = now - update_date
        return time_passed > STALE_INTERVAL

    @classmethod
    def create(cls, *args, **kwargs):
        item = cls(*args, **kwargs)
        date = arrow.get()
        item['creation_date'] = item.get('creation_date', date)
        item['update_date'] = item.get('update_date', date)
        return item

    @classmethod
    def get_collection(cls):
        from airbnb_scraper.db import AirbnbMongoDB
        client = AirbnbMongoDB.shared()
        return client.db[cls._collection_name]

    @classmethod
    def load(cls, id):
        matches = list(cls.get_collection().find({ID_KEY: id}))
        if len(matches) == 0:
            return None
        elif len(matches) == 1:
            match = matches[0]
            assert match[ID_KEY] == id
            # Get only the current values and filter
            # out legacy keys
            values = {}
            for key in cls.fields:
                if key in match:
                    values[key] = match[key]
            return cls(**values)
        else:
            raise Exception(f'Multiple objects found for id {id}')
    
    @classmethod
    def save_many(cls,  items):
        # TODO: optimise with bulk insert
        # https://api.mongodb.com/python/current/tutorial.html#bulk-inserts
        for item in items:
            item.save()

    def serialize(self):
        return MongoDBItemExporter().export_item(self)
        # doc = {}
        # for key, field in type(self).fields.items():
        #     value = self[key]
        #     value = self.serialize_field(field, key, value)
        #     doc[key] = value
        # return doc

    def save(self):
        doc = self.serialize()
        collection = type(self).get_collection()
        collection.update_one(
            {ID_KEY: self[ID_KEY]},
            {'$set': doc},
            upsert=True
        )

    # def update_hash(self, id_sensitive=False):
    #     d = dict(self)
    #     del d['creation_date']
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
        return listing_id

    def update_id(self):
        self[ID_KEY] = type(self).create_id(listing_id=self['listing_id'])

# class AirbnbListingCalendar(AirbnbItem):

#     _item_type = 'calendar'

#     listing_id = scrapy.Field()
#     time_zone = scrapy.Field()
#     currency = scrapy.Field()
#     months = scrapy.Field()
#     start_date = scrapy.Field(serializer=naive_datetime_serializer)
#     end_date = scrapy.Field(serializer=naive_datetime_serializer)
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

    # source_hash = scrapy.Field()

    listing_id = scrapy.Field()
    time_zone = scrapy.Field()
    currency = scrapy.Field()
    earliest_data_date = scrapy.Field(serializer=datetime_serializer)
    month = scrapy.Field()
    year = scrapy.Field()
    start_date = scrapy.Field(serializer=naive_datetime_serializer)
    end_date = scrapy.Field(serializer=naive_datetime_serializer)
    availability = scrapy.Field()
    revenue = scrapy.Field()
    future_revenue = scrapy.Field()
    average_price = scrapy.Field()
    median_price = scrapy.Field()
    lowest_price = scrapy.Field()
    highest_price = scrapy.Field()

    @classmethod
    def create_id(cls, listing_id='', date='', tzinfo='UTC'):
        start_date = arrow.get(date, tzinfo=tzinfo).floor('month')
        subid = start_date.format('YYYY-MM')
        return f'{listing_id}/{cls._item_type}/{subid}'

    def update_id(self):
        self[ID_KEY] = type(self).create_id(
            listing_id=self['listing_id'],
            date=self['date'],
            tzinfo=self['time_zone']
        )

    def update_with_days(self, days):
        time_zone = self['time_zone']
        now_local = arrow.utcnow().to(time_zone)
        today_local = now_local.floor('day')
        date = None
        earliest_data_date = None

        booked_days = 0
        available_future_days = 0
        future_days = 0
        total_days = 0
        is_data_complete = True
        has_price_error = False
        prices = []
        revenue = 0.0
        future_revenue = 0.0

        for day in days:
            if not bool(earliest_data_date):
                earliest_data_date = day['creation_date']
                self['earliest_data_date'] = earliest_data_date

            is_future = day.is_future
            is_available = day.is_available
            is_booked = day.is_booked

            if is_data_complete and not day.is_data_complete:
                is_data_complete = False

            # Price
            price = day.get('price', 0.0)
            if bool(price):
                prices.append(price)
                if is_booked:
                    if is_future:
                        future_revenue += price
                    else:
                        revenue += price
            else:
                # Handle missing price
                has_price_error = True

            total_days += 1
            if is_future:
                future_days += 1
                if bool(is_available):
                    available_future_days += 1

        if future_days > 0:
            availability = round(float(available_future_days) / float(future_days) * 100.0) / 100.0
        else:
            availability = 0.0
        
        self['availability'] = availability

        if not has_price_error:
            prices.sort()

            if is_data_complete:
                self['revenue'] = revenue
            else:
                self['revenue'] = None

            self['future_revenue'] = future_revenue
            self['average_price'] = round(sum(prices) / float(total_days) * 10.0) / 10.0
            self['median_price'] = prices[int(total_days / 2)]
            self['lowest_price'] = prices[0]
            self['highest_price'] = prices[-1]
        else:
            self['revenue'] = None
            self['future_revenue'] = None
            self['average_price'] = None
            self['median_price'] = None
            self['lowest_price'] = None
            self['highest_price'] = None


class AirbnbListingCalendarDay(AirbnbItem):

    _item_type = 'day'
    _collection_name = 'days'

    # source_hash = scrapy.Field()

    listing_id = scrapy.Field()
    time_zone = scrapy.Field()
    currency = scrapy.Field()
    date = scrapy.Field(serializer=naive_datetime_serializer)
    last_available_seen_date = scrapy.Field(serializer=datetime_serializer)
    first_unavailable_seen_date = scrapy.Field(serializer=datetime_serializer)
    available = scrapy.Field()
    booking_date = scrapy.Field(serializer=datetime_serializer)
    price = scrapy.Field()

    @classmethod
    def create_id(cls, listing_id='', date='', tzinfo='UTC'):
        subid = arrow.get(date, tzinfo=tzinfo).format('YYYY-MM-DD')
        return f'{listing_id}/{cls._item_type}/{subid}'

    def update_id(self):
        self[ID_KEY] = type(self).create_id(
            listing_id=self['listing_id'],
            date=self['date'],
            tzinfo=self['time_zone']
        )

    @property
    def is_available(self):
        return bool(self.get('available'))

    @property
    def is_booked(self):
        return bool(self.get('booking_date'))

    @property
    def is_data_complete(self):
        av_date = self.get('last_available_seen_date')
        unav_date = self.get('first_unavailable_seen_date')
        return bool(av_date) and bool(unav_date)

    @property
    def local_date(self):
        return arrow.get(self.get('date'), tzinfo=self['time_zone'])

    @property
    def is_past(self):
        return arrow.get(self.get('update_date')) < self.local_date.ceil('day').shift(hours=-2)

    def update_inferred(self):
        now = arrow.get()
        if self.is_available:
            self['last_available_seen_date'] = now
            self['first_unavailable_seen_date'] = None
            self['booking_date'] = None
        elif not self.is_past:
            # Only check availability in the future
            if self.get('first_unavailable_seen_date') is None:
                self['first_unavailable_seen_date'] = now
        
        av_date = self.get('last_available_seen_date')
        unav_date = self.get('first_unavailable_seen_date')
        if bool(av_date) and bool(unav_date):
            # Estimate booking date
            self['booking_date'] = arrow.get(round((av_date.timestamp + unav_date.timestamp) / 2))


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
