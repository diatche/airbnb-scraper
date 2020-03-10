# -*- coding: utf-8 -*-
import json
import collections
import re
import logging
import sys
import scrapy
import math
import arrow
import dateutil
from scrapy_splash import SplashRequest
from scrapy.exceptions import CloseSpider
from airbnb_scraper.items import AirbnbListing, AirbnbListingCalendarMonth, AirbnbListingCalendarDay, ID_KEY
from airbnb_scraper.db import AirbnbMongoDB
from airbnb_scraper import util
from timezonefinder import TimezoneFinder

REQUEST_WAIT = '0.5'
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
# Run crawler with -> scrapy crawl airbnb -o 21to25.json -a price_min='' -a price_max=''        *
# *********************************************************************************************

class AirbnbSpider(scrapy.Spider):
    name = 'airbnb'
    allowed_domains = ['www.airbnb.com']

    """
    You don't have to override __init__ each time and can simply use self.parameter (See https://bit.ly/2Wxbkd9),
    but I find this way much more readable.
    """
    def __init__(self, city='', currency='', months=AVAILABILITY_MONTHS, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filters = dict(kwargs)
        if bool(city):
            self.filters['query'] = city

        if 'price_min' not in self.filters:
             self.filters['price_min'] = 0
        self.filters['price_min'] = int(math.ceil(float(self.filters['price_min'])))

        if 'price_max' not in self.filters:
             self.filters['price_max'] = 0
        self.filters['price_max'] = int(math.floor(float(self.filters['price_max'])))

        if self.filters['price_max'] <= 0 or self.filters['price_max'] < self.filters['price_min']:
            del self.filters['price_max']

        if self.filters['price_min'] <= 0:
            del self.filters['price_min']

        self.logger.debug(f'Created crawler with filters: {self.filters}')

        self.currency = currency
        self.months = months
        self.request_date = arrow.get()

    def base_params(self):
        params = {
            'key': KEY,
            'locale': LOCALE,
        }
        if bool(self.currency):
            params['currency'] = self.currency
        return params

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
            'query_understanding_enabled': 'true',
            'refinement_paths%5B%5D': '%2Fhomes',
            'search_type': 'FILTER_CHANGE',
            'selected_tab_id': 'home_tab',
            'show_groupings': 'true',
            'supports_for_you_v3': 'true',
            'timezone_offset': '-240',
            'version': '1.5.6'
        })
        params.update(self.filters)

        if bool(items_offset):
            params['items_offset'] = items_offset
        if bool(section_offset):
            params['section_offset'] = section_offset

        return type(self).create_url(EXPLORE_BASE_URL, params=params)

    def create_calendar_url(self, listing_id='', time_zone='UTC'):
        local_request_date = self.request_date.to(time_zone)
        params = self.base_params()
        params.update({
            'listing_id': listing_id,
            'month': local_request_date.month,
            'year': local_request_date.year,
            'count': self.months,
        })
        return type(self).create_url(
            CALENDAR_BASE_URL,
            params=params
        )
    
    def start_requests(self):
        """Sends a scrapy request to the designated url price range

        Args:
        Returns:
        """

        # Open database connection
        AirbnbMongoDB.shared().open()

        url = self.create_explore_url()
        self.logger.debug(f'Starting explore: \n{url}')
        yield scrapy.Request(
            url=url,
            callback=self.parse_explore,
            dont_filter=True
        )

    def closed(self, reason):
        # Close database connection
        AirbnbMongoDB.shared().close()

    def parse_explore(self, response):
        """Parses all the URLs/ids/available fields from the initial json object and stores into dictionary

        Args:
            response: Json object from explore_tabs
        Returns:
        """
        self.logger.debug(f'Parsing explore: \n{response.url}')
        
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

        listings = []
        tf = TimezoneFinder()

        for home in homes:
            listing_info = home.get('listing')
            listing_id = str(listing_info.get('id'))
            self.logger.debug(f'Parsing listing "{listing_id}"')

            lat = listing_info.get('lat')
            lng = listing_info.get('lng')
            time_zone = tf.timezone_at(lng=lng, lat=lat) or 'UTC'
            # self.logger.debug(f'Listing {listing_id} time zone: {time_zone}')

            url = LISTING_BASE_URL + str(listing_id)

            listing_dict = {}
            listing_dict['listing_id'] = listing_id
            listing_dict['url'] = url
            listing_dict['time_zone'] = time_zone

            # Add data from fetch
            for key in AirbnbListing.fields:
                if key in listing_info:
                    listing_dict[key] = listing_info[key]

            # Add data from host
            host_info = listing_info.get('user')
            listing_dict['host_id'] = host_info.get('id')

            # Add data from pricing quote
            pricing_quote = home.get('pricing_quote')
            for key in AirbnbListing.fields:
                if key in pricing_quote:
                    listing_dict[key] = pricing_quote[key]
            
            rate_info = pricing_quote.get('rate')
            listing_dict['rate'] = rate_info.get('amount')
            listing_dict['currency'] = rate_info.get('currency')

            rate_with_service_fee_info = pricing_quote.get('rate_with_service_fee')
            listing_dict['rate_with_service_fee'] = rate_with_service_fee_info.get('amount')

            # # Get hash of values
            # listing_dict['source_hash'] = util.hash_str(listing_dict)

            # Apply data to listing
            listing = AirbnbListing.load(listing_id)
            if listing is None:
                listing = AirbnbListing.create()
            listings.append(listing)

            listing['update_date'] = arrow.now()
            for key, value in listing_dict.items():
                listing[key] = value
            listing.update_id()

            # yield SplashRequest(
            #     url=LISTING_BASE_URL+listing_id,
            #     callback=self.parse_listing_details,
            #     meta=listing,
            #     endpoint="render.html",
            #     args={'wait': REQUEST_WAIT}
            # )

            # Save listing
            yield listing

            # Check if should fetch calendar
            time_zone = listing['time_zone']
            current_month_id = AirbnbListingCalendarMonth.create_id(
                listing_id=listing_id,
                date=arrow.get(),
                tzinfo=time_zone
            )
            current_month = AirbnbListingCalendarMonth.load(current_month_id)
            if current_month is not None and not current_month.is_stale:
                # No need to refetch calendar
                self.logger.debug(f'Skipping listing "{listing_id}" calendar fetch')
                continue

            # Fetch listing calendar
            calendar_url = self.create_calendar_url(
                listing_id=listing_id,
                time_zone=time_zone
            )
            calendar_meta = {
                'listing_id': listing_id,
                'currency': listing_dict['currency'],
                'time_zone': time_zone
            }
            self.logger.debug(f'Fetching listing "{listing_id}" calendar: {calendar_url}')
            yield scrapy.Request(
                url=calendar_url,
                callback=self.parse_calendar,
                meta=calendar_meta,
                dont_filter=True
            )
        
        # After scraping entire listings page, check if more pages are available
        pagination_metadata = data.get('explore_tabs')[0].get('pagination_metadata')
        if pagination_metadata.get('has_next_page'):
            # If there is a next page, update url and scrape from next page
            url = self.create_explore_url(
                items_offset=pagination_metadata.get('items_offset'),
                section_offset=pagination_metadata.get('section_offset'),
            )
            self.logger.debug(f'Continuing explore: \n{url}')
            yield scrapy.Request(
                url=url,
                callback=self.parse_explore
            )

    # def parse_listing_details(self, response):
    #     """
    #     Parses details for a single listing page and stores into AirbnbListing object

    #     Args:
    #         response: The response from the page (same as inspecting page source)
    #     Returns:
    #         An AirbnbListing object containing the set of fields pertaining to the listing
    #     """
    #     assert response.url.startswith(LISTING_BASE_URL), f'Unexpected listing response URL: {response.url}'

    #     listing = response.meta

    #     # Other fields scraped from html response.text using regex (some might fail hence try/catch)
    #     try:
    #         listing['host_reviews'] = int((re.search(r'"badges":\[{"count":(.*?),"id":"reviews"',
    #                                   response.text)).group(1))
    #     except:
    #         listing['host_reviews'] = 0

    #     # Main six rating metrics + overall_guest_satisfication
    #     try:
    #         listing['accuracy'] = int((re.search('"accuracy_rating":(.*?),"', response.text)).group(1))
    #         listing['checkin'] = int((re.search('"checkin_rating":(.*?),"', response.text)).group(1))
    #         listing['cleanliness'] = int((re.search('"cleanliness_rating":(.*?),"', response.text)).group(1))
    #         listing['communication'] = int((re.search('"communication_rating":(.*?),"', response.text)).group(1))
    #         listing['value'] = int((re.search('"value_rating":(.*?),"', response.text)).group(1))
    #         listing['location'] = int((re.search('"location_rating":(.*?),"', response.text)).group(1))
    #         listing['guest_satisfication'] = int((re.search('"guest_satisfaction_overall":(.*?),"',
    #                                          response.text)).group(1))
    #     except:
    #         listing['accuracy'] = 0
    #         listing['checkin'] = 0
    #         listing['cleanliness'] = 0
    #         listing['communication'] = 0
    #         listing['value'] = 0
    #         listing['location'] = 0
    #         listing['guest_satisfication'] = 0

    #     # Extra Host Fields
    #     try:
    #         listing['response_rate'] = int((re.search('"response_rate_without_na":"(.*?)%",', response.text)).group(1))
    #         listing['response_time'] = (re.search('"response_time_without_na":"(.*?)",', response.text)).group(1)
    #     except:
    #         listing['response_rate'] = 0
    #         listing['response_time'] = ''

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
        month_infos = data.get('calendar_months')
        listing_id = response.meta['listing_id']
        assert bool(listing_id), 'Missing listing ID'
        time_zone = response.meta['time_zone'] or 'UTC'
        currency = response.meta['currency']

        all_months = []
        all_days = []
        now = arrow.get()

        for month_info in month_infos:
            month_num = month_info.get('month')
            year_num = month_info.get('year')
            start_date = arrow.get(year_num, month_num, 1)
            month_id = AirbnbListingCalendarMonth.create_id(
                listing_id=listing_id,
                date=start_date,
                tzinfo=time_zone
            )
            # self.logger.debug(f'Parsing listing "{listing_id}" month {year_num}-{month_num:02} ({month_id})')
            month = AirbnbListingCalendarMonth.load(month_id)
            if month is None:
                month = AirbnbListingCalendarMonth.create()
            month['update_date'] = now

            month['start_date'] = start_date
            month['end_date'] = start_date.ceil('month').floor('day')

            month['listing_id'] = listing_id
            month['currency'] = currency
            month['time_zone'] = time_zone
            month['month'] = month_num
            month['year'] = year_num
            
            days = []
            day_infos = month_info.get('days')
            for day_info in day_infos:
                day = AirbnbListingCalendarDay.load(month_id)
                if day is None:
                    day = AirbnbListingCalendarDay.create()
                day['update_date'] = now

                day['listing_id'] = listing_id
                day['currency'] = currency
                day['time_zone'] = time_zone

                date = arrow.get(day_info.get('date')).replace(tzinfo=time_zone)
                day['date'] = date
                day['available'] = day_info.get('available')

                price_strings = re.findall(r'\d+', day_info.get('price').get('local_price_formatted') or '')
                if len(price_strings) == 1:
                    day['price'] = float(price_strings[0])

                day.update_inferred()
                day.update_id()
                days.append(day)
                all_days.append(day)

                # self.logger.debug(f'Day {day[ID_KEY]} (is_complete: {day.is_data_complete}, is_booked: {day.is_booked}): {dict(day)}')

            month.update_with_days(days, now=now)
            month.update_id()
            all_months.append(month)

        AirbnbListingCalendarDay.update_group(all_days)

        for month in all_months:
            yield month
        for day in all_days:
            yield day
