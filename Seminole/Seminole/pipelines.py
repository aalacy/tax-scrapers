# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json

from util import Util

myutil = Util('Seminole')

class SeminolePipeline:
	def process_item(self, item, spider):
		myutil._normalizeKeys(item['data'])
		myutil._save_to_mongo(data=item['data'])
		return item
