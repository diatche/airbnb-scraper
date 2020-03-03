# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os
import pymongo
from pathlib import Path
from scrapy.exporters import JsonItemExporter
from scrapy.exceptions import DropItem
from airbnb_scraper.items import AirbnbListing, AirbnbListingCalendar, ID_KEY

TEMP_DIR = Path(os.path.abspath(os.path.dirname(__file__))) / 'temp'


class AirbnbMongoPipeline(object):

    listings_collection_name = 'listings'
    calendars_collection_name = 'calendars'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI', ''),
            mongo_db=crawler.settings.get('MONGO_DATABASE', '')
        )

    def open_spider(self, spider):
        spider.logger.debug(f'Opening MongoDB connection (uri: {self.mongo_uri}, db: {self.mongo_db})')
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        spider.logger.debug(f'Closing MongoDB connection (uri: {self.mongo_uri}, db: {self.mongo_db})')
        self.client.close()

    def process_item(self, item, spider):
        doc = dict(item)
        # spider.logger.debug(f'Saving item to MongoDB: {doc}')
        if isinstance(item, AirbnbListing):
            collection = self.db[self.listings_collection_name]
        elif isinstance(item, AirbnbListingCalendar):
            collection = self.db[self.calendars_collection_name]
        else:
            raise TypeError(f'Unknown item type: {type(item).__name__}')
        
        collection.update_one(
            {ID_KEY: item[ID_KEY]},
            {'$set': doc},
            upsert=True
        )
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
