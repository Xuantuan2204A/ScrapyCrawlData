from datetime import datetime
import hashlib
import pymysql
from scrapy.conf import settings
import redis
from elasticsearch import Elasticsearch

class CrawldatadbPipeline(object):
    def __init__(self, *args, **kwargs):
        self.create_connection()
        self.dataformat = settings['DATETIME_FORMAT']
        self.fonttext = settings['FEED_EXPORT_ENCODING']
        self.redis_db = redis.Redis(
            host=settings['REDIS_HOST'], port=settings['REDIS_PORT'], db=settings['REDIS_DB_ID'])

    def process_item(self, item, spider):
        # self.insertdata(item)
        return item

    def create_connection(self):
        self.conn = pymysql.connect(
            db=settings['MYSQL_DB_NAME'],
            host=settings['MYSQL_HOST'],
            port=settings['MYSQL_PORT'],
            user=settings['MYSQL_USER'],
            passwd=settings['MYSQL_PASSWORD'],
            charset="utf8", use_unicode=True
        )
        self.curr = self.conn.cursor()
        print("Database connection successful")

    def insertdata(self, item):
        string = str(item['url_title'].encode('utf-8'))
        key_insert = hashlib.md5(str(string).decode(
            'utf-8').encode('utf-8')).hexdigest()

        string_cateurl = str(item['category_url'].encode('utf-8'))
        key_insert_cateurl = hashlib.md5(str(string_cateurl).decode(
            'utf-8').encode('utf-8')).hexdigest()

        thoigian_format = item['createdate']
        thoigian_format = datetime.strptime(
            thoigian_format, ' %H:%M - %d/%m/%Y')

        content_full = ""
        for content in item['content']:
            content_full = content_full + " " + content
        values = (  # This is the value we want to pass into the database
            key_insert_cateurl,
            item['category_name'],
            key_insert,
            item['title'],
            item['introl'],
            content_full,
            thoigian_format,
        )
        try:
            # SQL statement
            sql = 'INSERT INTO thegioihoinhap VALUES (%s,%s,%s,%s,%s,%s,%s)'
            self.curr.execute(sql, values)  # Execute with execute
            self.insert_key_to_redis(key_insert)
            print("Data inserted successfully")
        except Exception as e:
            print('Insert error:', e)
            self.conn.rollback()
        else:
            self.conn.commit()  # Every time you insert it, you commit it, and the data is saved

    def insert_key_to_redis(self, key):
        nano = self.redis_db.set(key, "exist")
    
    def redis_exists(self, key):
        val = self.redis_db.get(key)
        if val == "exist":
            return True
        return False

    # def create_index(self):
    #     # Connect Elasticsearch
    #     index_new = 'index_demo'
    #     es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

    #     # Delete index if exists
    #     if es.indices.exists(index_new):
    #         # es.indices.delete(index=index_new)
    #         print("Index exists !")
    #     else:
    #         # index settings
    #         settings = {
    #             "settings": {
    #                 "number_of_shards": 1,
    #                 "number_of_replicas": 0
    #             },
    #             "mappings": {
    #                 "filtered": {
    #                     "properties": {
    #                         "category_url": {
    #                             "type": "text"
    #                         },
    #                         "category_name": {
    #                             "type": "text"
    #                         },
    #                         "url_title": {
    #                             "type": "text"
    #                         },
    #                         "title": {
    #                             "type": "text"
    #                         }
    #                     }
    #                 }
    #             }
    #         }
    #     # create index
    #     try:
    #         es.indices.create(index='tvplus', ignore=400, body=settings)
    #         print( "Create Index Success !")
    #     except Exception as e:
    #         print( "Create Index Error: "+str(e))