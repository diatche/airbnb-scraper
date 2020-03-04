# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os
from pathlib import Path
from scrapy.exporters import JsonItemExporter
from scrapy.exceptions import DropItem
from airbnb_scraper.db import AirbnbMongoDB
from airbnb_scraper.items import AirbnbItem, ID_KEY

TEMP_DIR = Path(os.path.abspath(os.path.dirname(__file__))) / 'temp'


class AirbnbMongoPipeline(object):

    def __init__(self, client=None):
        self.client = client or AirbnbMongoDB.shared()

    @classmethod
    def from_crawler(cls, crawler):
        # Prefer shared database
        shared_client = AirbnbMongoDB.shared()
        mongo_uri = crawler.settings.get('MONGO_URI') or shared_client.mongo_uri
        mongo_db = crawler.settings.get('MONGO_DATABASE') or shared_client.mongo_db

        if mongo_uri == shared_client.mongo_uri and mongo_db == shared_client.mongo_db:
            client = shared_client
        else:
            client = AirbnbMongoDB(
                mongo_uri=mongo_uri,
                mongo_db=mongo_db
            )

        return AirbnbMongoPipeline(client=client)

    def open_spider(self, spider):
        spider.logger.debug(f'Pipeline opening MongoDB connection')
        self.client.open()

    def close_spider(self, spider):
        spider.logger.debug(f'Pipeline closing MongoDB connection')
        self.client.close()

    def process_item(self, item, spider):
        spider.logger.debug(f'Pipeline saving item to MongoDB')
        if not isinstance(item, AirbnbItem):
            raise TypeError(f'Unknown item type: {type(item).__name__}')
        item.save()
        return item


# class AirbnbJsonPipeline(object):

#     listings_path = None
#     calendars_path = None
#     listings_file = None
#     calendars_file = None

#     def __init__(self, raw_path):
#         if '/' in raw_path:
#             path = Path(os.path.normpath(raw_path))
#         else:
#             path = TEMP_DIR / raw_path

#         base_dir = path.parent
#         base_name = path.stem
#         ext = path.suffix

#         listings_file_str = f'{base_name}_listings.{ext}'
#         self.listings_path = base_dir / listings_file_str

#         calendars_file_str = f'{base_name}_calendars.{ext}'
#         self.calendars_path = base_dir / calendars_file_str

#     @classmethod
#     def from_crawler(cls, crawler):
#         return cls(
#             mongo_uri=crawler.settings.get('MONGO_URI'),
#             mongo_db=crawler.settings.get('MONGO_DATABASE', 'items')
#         )

#     def open_spider(self, spider):
#         self.listings_file = self.listings_path.open('w')
#         self.listings_exporter = JsonItemExporter(self.listings_file)
#         self.listings_exporter.start_exporting()
        
#         self.calendars_file = self.calendars_path.open('w')
#         self.calendars_exporter = JsonItemExporter(self.calendars_file)
#         self.calendars_exporter.start_exporting()

#     def close_spider(self, spider):
#         self.listings_exporter.finish_exporting()
#         self.listings_file.close()

#         self.calendars_exporter.finish_exporting()
#         self.calendars_file.close()

#     def process_item(self, item, spider):
#         if isinstance(item, AirbnbListing):
#             self.listings_exporter.export_item(item)
#         elif isinstance(item, AirbnbListingCalendar):
#             self.calendars_exporter.export_item(item)
#         else:
#             raise TypeError(f'Unknown item type: {type(item).__name__}')
#         return item
