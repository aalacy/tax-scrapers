# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json
from pymongo import MongoClient
import logging

mongocli = MongoClient('mongodb+srv://hbg:**cluster0.uetxd.mongodb.net/:27017')

class OsceolaproejctPipeline:


    def process_item(self, item, spider):
        self._normalizeKeys(item['data'])
        self.save_to_mongo(item['data'])
        return item


    def save_to_mongo(self, data):
        col = mongocli['Osceola_County']['property']
        try:
            col.insert(data)
        except Exception as E:
            logging.error('Error: ' + str(E), exc_info=True)

    def strip_list1(self, arr):
        new_list = []
        for item in arr:
            if item:
                new_list.append(self._valid(item))
            else:
                new_list.append(' ')

        return new_list

    def _valid(self, val):
        if val:
            return val.strip()
        else:
            return ''

    def _normalizeKeys(self, obj):
        if type(obj) is list:
            for o in obj:
                self._normalizeKeys(o)
            return
        elif type(obj) is dict:
            keys = list(obj.keys())
            for key in keys:
                _key = '_'.join(self.strip_list1(key.lower().replace('.', '').split(' ')))
                newKey = _key.strip()
                obj[newKey] = obj.pop(key)
            for val in obj.values():
                self._normalizeKeys(val)
            return
        else:
            return

