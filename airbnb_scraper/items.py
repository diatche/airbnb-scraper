# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import arrow
import scrapy
from scrapy.loader.processors import MapCompose, TakeFirst, Join
from airbnb_scraper import constants


def remove_unicode(value):
    return value.replace(u"\u201c", '').replace(u"\u201d", '').replace(u"\2764", '').replace(u"\ufe0f")


class AirbnbItem(scrapy.Item):

    item_type = scrapy.Field()
    creation_date = scrapy.Field()
    app_version = scrapy.Field()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self['item_type'] = str(type(self).__name__)
        self['creation_date'] = arrow.get().format()
        self['app_version'] = constants.APP_VERSION


class AirbnbListing(AirbnbItem):

    # Host Fields
    is_superhost = scrapy.Field()
    host_id = scrapy.Field()

    # Room Fields
    listing_id = scrapy.Field()
    public_address = scrapy.Field()
    price = scrapy.Field()
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
    listing_name = scrapy.Field(input_processor=MapCompose(remove_unicode))
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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self['item_type'] = 'listing'


class AirbnbListingCalendar(AirbnbItem):

    listing_id = scrapy.Field()
    time_zone = scrapy.Field()
    currency = scrapy.Field()
    months = scrapy.Field()
    # dates = scrapy.Field()
    availability = scrapy.Field()
    prices = scrapy.Field()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self['item_type'] = 'calendar'

