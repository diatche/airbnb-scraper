# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os
from pathlib import Path
from scrapy.exporters import JsonItemExporter
from scrapy.exceptions import DropItem
from airbnb_scraper.items import AirbnbListing, AirbnbListingCalendar

TEMP_DIR = Path(os.path.abspath(os.path.dirname(__file__))) / 'temp'


class AirbnbScraperPipeline(object):

    listings_file = None
    calendars_file = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def open_spider(self, spider):
        raw_path = spider.output
        if '/' in raw_path:
            path = Path(os.path.normpath(raw_path))
        else:
            path = TEMP_DIR / raw_path

        base_dir = path.parent
        base_name = path.stem
        ext = path.suffix

        listings_file_str = f'{base_name}_listings.{ext}'
        listings_file_path = base_dir / listings_file_str
        spider.logger.info(f'Listings output path: {listings_file_path}')
        self.listings_file = listings_file_path.open('w')
        self.listings_exporter = JsonItemExporter(self.listings_file)
        self.listings_exporter.start_exporting()

        calendars_file_str = f'{base_name}_calendars.{ext}'
        calendars_file_path = base_dir / calendars_file_str
        spider.logger.info(f'Calendars output path: {calendars_file_path}')
        self.calendars_file = calendars_file_path.open('w')
        self.calendars_exporter = JsonItemExporter(self.calendars_file)
        self.calendars_exporter.start_exporting()

    def close_spider(self, spider):
        self.listings_exporter.finish_exporting()
        self.listings_file.close()

        self.calendars_exporter.finish_exporting()
        self.calendars_file.close()

    def process_item(self, item, spider):
        if isinstance(item, AirbnbListing):
            self.listings_exporter.export_item(item)
        elif isinstance(item, AirbnbListingCalendar):
            self.calendars_exporter.export_item(item)
        else:
            raise TypeError(f'Unknown item type: {type(item).__name__}')
        return item

    # def process_item(self, item, spider):
    #     return item
