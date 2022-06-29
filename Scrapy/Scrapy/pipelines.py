# -*- coding: utf-8 -*-
import sys
from elasticsearch import Elasticsearch
import pika
import redis
from scrapy.conf import settings
import hashlib
import json


class ScrapyPipeline:
    def __init__(self, *args, **kwargs):
        self.rabbitmq()
        self.es_index = "rabbit_elast0111"
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
        self.chanel.exchange_declare(exchange='logs', exchange_type='fanout')
        self.chanel.queue_bind(queue='monzitamonzita', exchange='logs',routing_key='monitaz_ifollow')
        self.chanel.queue_bind(queue='monzita_item', exchange='logs',routing_key='monitaz_ifollow')
        self.chanel.queue_bind(queue='rabbit_elasticsearch',
                               exchange='logs', routing_key='monitaz_ifollow')

    def sent_data(self, item):
        string = str(item["Url"]).encode('utf-8')
        key_insert = hashlib.md5(str(string).decode(
            'utf-8').encode('utf-8')).hexdigest()
        item_dict = dict(item)
        item_dict['Url'] = key_insert
        json_string = json.dumps(item_dict)

        # sent data vào hàng chờ

        self.chanel.basic_publish(
            exchange='logs', routing_key='monitaz_ifollow', body=json_string)
        print("========================================================================")
        print("[x] message sent!", json_string)
        print("========================================================================")

        # # add data từ hàng chờ vào elasticsearch
        # res = self.es.index(index=self.es_index, id=key_insert, ignore=400, body=json_string)
        # print("------------------")
        # print("insert database sussufy",res)
        # print("------------------")
