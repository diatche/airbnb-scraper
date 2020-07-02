# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymongo
from airbnb_scraper.settings import MONGO_URI, MONGO_DATABASE

_shared = None

class AirbnbMongoDB:

    @classmethod
    def shared(cls):
        global _shared
        if _shared is None:
            _shared = cls(
                mongo_uri=MONGO_URI,
                mongo_db=MONGO_DATABASE
            )
        return _shared

    def __init__(self, mongo_uri='', mongo_db=''):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self._reset()
    
    def open(self):
        self._open_depth += 1
        if self._open_depth > 1:
            return
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close(self):
        self._open_depth -= 1
        if self._open_depth > 0:
            return
        elif self._open_depth < 0:
            raise Exception('Mismatched MongoDB open and close calls')
        self.client.close()
        self._reset()

    def _reset(self):
        self.client = None
        self.db = None
        self._open_depth = 0
