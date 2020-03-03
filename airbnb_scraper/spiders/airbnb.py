# -*- coding: utf-8 -*-
import json
import collections
import re
import numpy as np
import logging
import sys
import scrapy
import math
import arrow
import dateutil
from scrapy_splash import SplashRequest
from scrapy.exceptions import CloseSpider
from airbnb_scraper.items import AirbnbListing, AirbnbListingCalendar
from timezonefinder import TimezoneFinder

AVAILABILITY_MONTHS = 6
EXPLORE_BASE_URL = 'https://www.airbnb.com/api/v2/explore_tabs'
LISTING_BASE_URL = 'https://www.airbnb.com/rooms/'
CALENDAR_BASE_URL = 'https://www.airbnb.com/api/v2/homes_pdp_availability_calendar'
KEY = 'd306zoyjsyarp7ifhu67rjxn52tv0t20'
LOCALE = 'en'


# ********************************************************************************************
# Important: Run -> docker run -p 8050:8050 scrapinghub/splash in background before crawling *
# ********************************************************************************************


# *********************************************************************************************
# Run crawler with -> scrapy crawl airbnb -o 21to25.json -a price_lb='' -a price_ub=''        *
# *********************************************************************************************

class AirbnbSpider(scrapy.Spider):
    name = 'airbnb'
    allowed_domains = ['www.airbnb.com']

    """
    You don't have to override __init__ each time and can simply use self.parameter (See https://bit.ly/2Wxbkd9),
    but I find this way much more readable.
    """
    def __init__(self, city='',price_lb='', price_ub='', currency='', *args,**kwargs):
        super(AirbnbSpider, self).__init__(*args, **kwargs)
        self.city = city
        self.price_lb = int(math.ceil(float(price_lb))) if bool(price_lb) else 0
        self.price_ub = int(math.floor(float(price_ub))) if bool(price_ub) else 0
        self.currency = currency or 'NZD'
        self.request_date = arrow.get()

    def base_params(self):
        return {
            'currency': self.currency,
            'key': KEY,
            'locale': LOCALE,
        }

    @classmethod
    def create_url(cls, base, params=None):
        if not bool(params):
            return base
        return base + '?' + '&'.join([f'{k}={v}' for k, v in params.items()])

    def create_explore_url(self, items_offset=0, section_offset=0):
        params = self.base_params()
        params.update({
            '_format': 'for_explore_search_web',
            'fetch_filters': 'true',
            'has_zero_guest_treatment': 'true',
            'is_guided_search': 'true',
            'is_new_cards_experiment': 'true',
            'is_standard_search': 'true',
            'items_per_grid': '18',
            'luxury_pre_launch': 'false',
            'metadata_only': 'false',
            'query': self.city,
            'query_understanding_enabled': 'true',
            'refinement_paths%5B%5D': '%2Fhomes',
            'search_type': 'FILTER_CHANGE',
            'selected_tab_id': 'home_tab',
            'show_groupings': 'true',
            'supports_for_you_v3': 'true',
            'timezone_offset': '-240',
            'version': '1.5.6'
        })
        if self.price_lb > 0:
            params['price_min'] = self.price_lb
        if self.price_lb > 0 and self.price_ub > self.price_lb:
            params['price_max'] = self.price_ub
        if bool(items_offset):
            params['items_offset'] = items_offset
        if bool(section_offset):
            params['section_offset'] = section_offset

        return type(self).create_url(EXPLORE_BASE_URL, params=params)

    def start_requests(self):
        """Sends a scrapy request to the designated url price range

        Args:
        Returns:
        """

        url = self.create_explore_url()
        # self.logger.debug(f'Exploring: \n{url}')
        yield scrapy.Request(
            url=url,
            callback=self.parse_id,
            dont_filter=True
        )

    def parse_id(self, response):
        """Parses all the URLs/ids/available fields from the initial json object and stores into dictionary

        Args:
            response: Json object from explore_tabs
        Returns:
        """
        
        # Fetch and Write the response data
        data = json.loads(response.body)

        # Return a List of all homes
        homes = data.get('explore_tabs')[0].get('sections')[0].get('listings')

        if homes is None:
            try: 
                homes = data.get('explore_tabs')[0].get('sections')[3].get('listings')
            except IndexError:
                try: 
                    homes = data.get('explore_tabs')[0].get('sections')[2].get('listings')
                except:
                    raise CloseSpider("No homes available in the city and price parameters")

        data_dict = collections.defaultdict(dict) # Create Dictionary to put all currently available fields in

        tf = TimezoneFinder()

        for home in homes:
            # self.logger.info(f'Parsing home:\n{home}')
            listing_id = str(home.get('listing').get('id'))
            url = LISTING_BASE_URL + str(home.get('listing').get('id'))

            lat = home.get('listing').get('lat')
            lng = home.get('listing').get('lng')
            time_zone = tf.timezone_at(lng=lng, lat=lat)
            # self.logger.debug(f'Listing {listing_id} time zone: {time_zone}')

            room_id = listing_id
            data_dict[listing_id]['listing_id'] = listing_id
            data_dict[room_id]['url'] = url
            data_dict[room_id]['picture_url'] = home.get('listing').get('picture_url')
            data_dict[room_id]['price'] = home.get('pricing_quote').get('rate').get('amount')
            data_dict[room_id]['rate_with_service_fee'] = home.get('pricing_quote').get('rate_with_service_fee').get('amount')
            data_dict[room_id]['bathrooms'] = home.get('listing').get('bathrooms')
            data_dict[room_id]['bedrooms'] = home.get('listing').get('bedrooms')
            data_dict[room_id]['beds'] = home.get('listing').get('beds')
            data_dict[room_id]['host_languages'] = home.get('listing').get('host_languages')
            data_dict[room_id]['is_business_travel_ready'] = home.get('listing').get('is_business_travel_ready')
            data_dict[room_id]['is_fully_refundable'] = home.get('listing').get('is_fully_refundable')
            data_dict[room_id]['is_new_listing'] = home.get('listing').get('is_new_listing')
            data_dict[room_id]['is_superhost'] = home.get('listing').get('is_superhost')
            data_dict[room_id]['lat'] = lat
            data_dict[room_id]['lng'] = lng
            data_dict[room_id]['time_zone'] = time_zone
            data_dict[room_id]['localized_city'] = home.get('listing').get('localized_city')
            data_dict[room_id]['localized_neighborhood'] = home.get('listing').get('localized_neighborhood')
            data_dict[room_id]['listing_name'] = home.get('listing').get('name')
            data_dict[room_id]['person_capacity'] = home.get('listing').get('person_capacity')
            data_dict[room_id]['picture_count'] = home.get('listing').get('picture_count')
            data_dict[room_id]['reviews_count'] = home.get('listing').get('reviews_count')
            data_dict[room_id]['min_nights'] = home.get('listing').get('min_nights')
            data_dict[room_id]['max_nights'] = home.get('listing').get('max_nights')
            data_dict[room_id]['property_type_id'] = home.get('listing').get('property_type_id')
            data_dict[room_id]['room_type_category'] = home.get('listing').get('room_type_category')
            data_dict[room_id]['room_and_property_type'] = home.get('listing').get('room_and_property_type')
            data_dict[room_id]['public_address'] = home.get('listing').get('public_address')
            data_dict[room_id]['amenity_ids'] = home.get('listing').get('amenity_ids')
            data_dict[room_id]['star_rating'] = home.get('listing').get('star_rating')
            data_dict[room_id]['host_id'] = home.get('listing').get('user').get('id')
            data_dict[room_id]['avg_rating'] = home.get('listing').get('avg_rating')
            data_dict[room_id]['can_instant_book'] = home.get('pricing_quote').get('can_instant_book')
            data_dict[room_id]['monthly_price_factor'] = home.get('pricing_quote').get('monthly_price_factor')
            data_dict[room_id]['currency'] = home.get('pricing_quote').get('rate').get('currency')
            data_dict[room_id]['rate_type'] = home.get('pricing_quote').get('rate_type')
            data_dict[room_id]['weekly_price_factor'] = home.get('pricing_quote').get('weekly_price_factor')

        # Iterate through dictionary of URLs in the single page to send a SplashRequest for each
        for listing_id in data_dict:
            listing_data = data_dict.get(listing_id)
            yield SplashRequest(
                url=LISTING_BASE_URL+listing_id,
                callback=self.parse_listing,
                meta=listing_data,
                endpoint="render.html",
                args={'wait': '0.5'}
            )

            # Get calendar
            # Note: using meta with this request breaks the listing request above
            time_zone = listing_data['time_zone']
            local_request_date = self.request_date.to(time_zone or 'UTC')
            calendar_url = type(self).create_url(CALENDAR_BASE_URL, params={
                'currency': self.currency,
                'key': 'd306zoyjsyarp7ifhu67rjxn52tv0t20',
                'locale': 'en',
                'listing_id': listing_id,
                'month': local_request_date.month,
                'year': local_request_date.year,
                'count': AVAILABILITY_MONTHS,
            })
            calendar_meta = {
                'listing_id': listing_id,
                'currency': self.currency,
                'time_zone': time_zone
            }
            yield SplashRequest(
                url=calendar_url,
                callback=self.parse_calendar,
                meta=calendar_meta,
                args={'wait': '0.5'}
            )

        # After scraping entire listings page, check if more pages
        pagination_metadata = data.get('explore_tabs')[0].get('pagination_metadata')
        if pagination_metadata.get('has_next_page'):
            # If there is a next page, update url and scrape from next page
            url = self.create_explore_url(
                items_offset=pagination_metadata.get('items_offset'),
                section_offset=pagination_metadata.get('section_offset'),
            )
            # self.logger.debug(f'Exploring: \n{url}')
            yield scrapy.Request(url=url, callback=self.parse_id)

    def parse_listing(self, response):
        """
        Parses details for a single listing page and stores into AirbnbListing object

        Args:
            response: The response from the page (same as inspecting page source)
        Returns:
            An AirbnbListing object containing the set of fields pertaining to the listing
        """
        assert response.url.startswith(LISTING_BASE_URL), f'Unexpected listing response URL: {response.url}'

        # New Instance
        listing = AirbnbListing()

        # Fill in fields for Instance from initial scrapy call
        listing['listing_id'] = response.meta['listing_id']
        listing['is_superhost'] = response.meta['is_superhost']
        listing['host_id'] = str(response.meta['host_id'])
        listing['price'] = response.meta['price']
        listing['rate_with_service_fee'] = response.meta['rate_with_service_fee']
        listing['url'] = response.meta['url']
        listing['picture_url'] = response.meta['picture_url']
        listing['bathrooms'] = response.meta['bathrooms']
        listing['bedrooms'] = response.meta['bedrooms']
        listing['beds'] = response.meta['beds']
        listing['is_business_travel_ready'] = response.meta['is_business_travel_ready']
        listing['is_fully_refundable'] = response.meta['is_fully_refundable']
        listing['is_new_listing'] = response.meta['is_new_listing']
        listing['lat'] = response.meta['lat']
        listing['lng'] = response.meta['lng']
        listing['localized_city'] = response.meta['localized_city']
        listing['localized_neighborhood'] = response.meta['localized_neighborhood']
        listing['listing_name'] = response.meta['listing_name']
        listing['person_capacity'] = response.meta['person_capacity']
        listing['picture_count'] = response.meta['picture_count']
        listing['reviews_count'] = response.meta['reviews_count']
        listing['min_nights'] = response.meta['min_nights']
        listing['max_nights'] = response.meta['max_nights']
        listing['property_type_id'] = response.meta['property_type_id']
        listing['room_type_category'] = response.meta['room_type_category']
        listing['room_and_property_type'] = response.meta['room_and_property_type']
        listing['public_address'] = response.meta['public_address']
        listing['amenity_ids'] = response.meta['amenity_ids']
        listing['star_rating'] = response.meta['star_rating']
        listing['avg_rating'] = response.meta['avg_rating']
        listing['can_instant_book'] = response.meta['can_instant_book']
        listing['monthly_price_factor'] = response.meta['monthly_price_factor']
        listing['weekly_price_factor'] = response.meta['weekly_price_factor']
        listing['currency'] = response.meta['currency']
        listing['rate_type'] = response.meta['rate_type']

        # # Other fields scraped from html response.text using regex (some might fail hence try/catch)
        # try:
        #     listing['host_reviews'] = int((re.search(r'"badges":\[{"count":(.*?),"id":"reviews"',
        #                               response.text)).group(1))
        # except:
        #     listing['host_reviews'] = 0

        # # Main six rating metrics + overall_guest_satisfication
        # try:
        #     listing['accuracy'] = int((re.search('"accuracy_rating":(.*?),"', response.text)).group(1))
        #     listing['checkin'] = int((re.search('"checkin_rating":(.*?),"', response.text)).group(1))
        #     listing['cleanliness'] = int((re.search('"cleanliness_rating":(.*?),"', response.text)).group(1))
        #     listing['communication'] = int((re.search('"communication_rating":(.*?),"', response.text)).group(1))
        #     listing['value'] = int((re.search('"value_rating":(.*?),"', response.text)).group(1))
        #     listing['location'] = int((re.search('"location_rating":(.*?),"', response.text)).group(1))
        #     listing['guest_satisfication'] = int((re.search('"guest_satisfaction_overall":(.*?),"',
        #                                      response.text)).group(1))
        # except:
        #     listing['accuracy'] = 0
        #     listing['checkin'] = 0
        #     listing['cleanliness'] = 0
        #     listing['communication'] = 0
        #     listing['value'] = 0
        #     listing['location'] = 0
        #     listing['guest_satisfication'] = 0

        # # Extra Host Fields
        # try:
        #     listing['response_rate'] = int((re.search('"response_rate_without_na":"(.*?)%",', response.text)).group(1))
        #     listing['response_time'] = (re.search('"response_time_without_na":"(.*?)",', response.text)).group(1)
        # except:
        #     listing['response_rate'] = 0
        #     listing['response_time'] = ''

        # Finally return the object
        yield listing

    def parse_calendar(self, response):
        assert response.url.startswith(CALENDAR_BASE_URL), f'Unexpected calendar response URL: {response.url}'

        # Clean JSON response
        json_str = str(response.body)
        try:
            json_start = json_str.index('{')
        except Exception:
            json_start = 0
        try:
            json_end = json_str.rindex('}')
        except Exception:
            json_end = len(json_str) - 1
        json_str = json_str[json_start:json_end + 1]
        # self.logger.debug(f'Calendar URL:\n{response.url}')
        # self.logger.debug(f'Calendar response body:\n{response.body}')

        data = json.loads(json_str)
        months = data.get('calendar_months')
        # date_infos = {}
        month_infos = {}
        # listing_id = listing['id']
        listing_id = response.meta['listing_id']
        assert bool(listing_id), 'Missing listing ID'
        time_zone = response.meta['time_zone']
        time_zone_safe = time_zone or 'UTC'

        calendar = AirbnbListingCalendar()
        calendar['listing_id'] = listing_id
        calendar['currency'] = response.meta['currency']
        calendar['time_zone'] = time_zone

        now_local = arrow.utcnow().to(time_zone_safe)
        today_local = now_local.floor('day')

        def _empty_available_datespan():
            return {
                'start_date': '',
                'end_date': '',
                'available': None,
            }

        def _is_empty_available_datespan(s):
            return not bool(s['start_date'])

        def _continue_available_datespan(s, date, available):
            if not bool(s['start_date']):
                s['start_date'] = date
            s['end_date'] = date
            s['available'] = available

        def _end_available_datespan(s, a):
            # Only add available ranges
            if not _is_empty_available_datespan(s) and s['available']:
                if s['start_date'] == s['end_date']:
                    s_clean = {
                        'date': s['start_date'],
                    }
                else:
                    s_clean = dict(s)
                    del s_clean['available']
                a.append(s_clean)

        def _empty_price_datespan():
            return {
                'start_date': '',
                'end_date': '',
                'price': 0.0
            }

        def _is_empty_price_datespan(s):
            return not bool(s['price'])

        def _continue_price_datespan(s, date, price):
            if not bool(s['start_date']):
                s['start_date'] = date
            s['end_date'] = date
            s['price'] = price

        def _end_price_datespan(s, a):
            if not _is_empty_price_datespan(s):
                if s['start_date'] == s['end_date']:
                    s_clean = {
                        'date': s['start_date'],
                        'price': s['price'],
                    }
                else:
                    s_clean = dict(s)
                a.append(s_clean)

        available_timespans = []
        current_available_timespan = _empty_available_datespan()
        price_timespans = []
        current_price_timespan = _empty_price_datespan()

        for month in months:
            available_future_days = 0
            future_days = 0
            # date_info = {}
            prices = []
            days = month.get('days')
            month_num = month.get('month')
            year_num = month.get('year')
            month_info_id = f'{year_num}-{month_num}'
            future_revenue = 0.0

            for day in days:
                date_str = day.get('date')
                date = arrow.get(date_str).replace(tzinfo=time_zone_safe)
                assert date.tzinfo == today_local.tzinfo
                is_future = date >= today_local

                # Availability
                available = day.get('available')
                if not _is_empty_available_datespan(current_available_timespan) and available != current_available_timespan['available']:
                    _end_available_datespan(current_available_timespan, available_timespans)
                    current_available_timespan = _empty_available_datespan()
                _continue_available_datespan(current_available_timespan, date_str, available)

                # date_info_id = f'{date_str}'
                # date_info = {
                #     'date': date_str,
                #     'available': available
                # }

                # Price
                price = 0.0
                price_strings = re.findall(r'\d+', day.get('price').get('local_price_formatted') or '')
                if len(price_strings) == 1:
                    price = float(price_strings[0])

                if bool(price):
                    # date_info['price'] = price
                    if is_future and future_revenue >= 0 and not available:
                        future_revenue += price
                    
                    if not _is_empty_price_datespan(current_price_timespan) and price != current_price_timespan['price']:
                        _end_price_datespan(current_price_timespan, price_timespans)
                        current_price_timespan = _empty_price_datespan()
                    _continue_price_datespan(current_price_timespan, date_str, price)
                else:
                    # Handle missing price
                    if is_future and not available:
                        future_revenue = -1.0

                    _end_price_datespan(current_price_timespan, price_timespans)
                    current_price_timespan = _empty_price_datespan()

                if is_future:
                    future_days += 1
                    if bool(available):
                        available_future_days += 1

                # date_infos[date_info_id] = date_info

            if future_days == 0:
                continue

            availability = round(float(available_future_days) / float(future_days) * 100.0) / 100.0
            month_info = {
                'month': month_num,
                'year': year_num,
                'availability': availability
            }
            if future_revenue >= 0:
                month_info['future_revenue'] = future_revenue
            month_infos[month_info_id] = month_info

        _end_available_datespan(current_available_timespan, available_timespans)
        _end_price_datespan(current_price_timespan, price_timespans)

        calendar['months'] = month_infos
        # calendar['dates'] = date_infos
        calendar['availability'] = available_timespans
        calendar['prices'] = price_timespans

        yield calendar
