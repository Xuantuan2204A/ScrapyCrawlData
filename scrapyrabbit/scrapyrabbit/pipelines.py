# -*- coding: utf-8 -*-

from datetime import datetime
import hashlib
import json
import unicodedata
from elasticsearch import Elasticsearch
import pika
import redis
from scrapy.conf import settings
from scrapy.exceptions import DropItem


class ElasSpidersPipeline(object):
    words_to_filter = ['politics', 'religion']
    def __init__(self, *args, **kwargs):
        self.rabbitmq()
        self.es_index = "rabbit_elasticsearch"
        self.es = Elasticsearch("http://127.0.0.1:9200")

        self.redis_db = redis.Redis(
            host=settings['REDIS_HOST'], port=settings['REDIS_PORT'], db=settings['REDIS_DB_ID'])

    def process_item(self, item, spider): 
        self.sent_data(item)
        return item
    def rabbitmq(self):
        self.connect_parameters = pika.ConnectionParameters('localhost')
        self.connection = pika.BlockingConnection(self.connect_parameters)
        self.chanel = self.connection.channel()
        self.chanel.queue_declare(queue="rabbit_elasticsearch")
        print("=====================")
        print("connect rabbitmq sussufull")
        print("=====================")

    def sent_data(self, item):
       
        string = str(item["url_title"]).encode('utf-8')
        key_insert = hashlib.md5(str(string).decode('utf-8').encode('utf-8')).hexdigest()

        item_dict = dict(item)
        item_dict['url_title'] = key_insert
        json_string = json.dumps(item_dict)
        # print("========================================================================")
        # print(json_string)
        # print("========================================================================")

        # sent data vào hàng chờ 
        self.chanel.basic_publish(exchange='',routing_key='rabbit_elasticsearch',body=json_string)
        print("========================================================================")
        print(" [x] message sent!")
        print("========================================================================")

        # add data vào elasticsearch
        res = self.es.index(index=self.es_index, id=key_insert, ignore=400, body=json_string)
        print("------------------")
        print("insert database sussufy")
        print("---------------------------------------------------------------")