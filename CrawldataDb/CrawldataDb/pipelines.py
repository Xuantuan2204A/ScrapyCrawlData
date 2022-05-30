from datetime import datetime
import pymysql
from scrapy.conf import settings
from scrapy.xlib.pydispatch import dispatcher
from scrapy import Spider, signals
from scrapy.exporters import JsonItemExporter


class CrawldatadbPipeline(object):

    def __init__(self, *args, **kwargs):
        self.create_connection()
        
        self.dataformat = settings['DATETIME_FORMAT']
        self.fonttext = settings['FEED_EXPORT_ENCODING']
        self.redis_db = redis.Redis(host=settings['REDIS_HOST'], port=settings['REDIS_PORT'], db=settings['REDIS_DB_ID'])

    def process_item(self, item, spider):
        # self.insert_dataCategory(item)
        # self.conn.lpush('xiaoshuo', item)
        # print(' data storage redis success ！')
        self.insertdata(item)
        return item
    
    def close_spider(self, spider):
            self.conn.close()

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
        thoigian_format = item['Createdate']
        thoigian_format = datetime.strptime(
            thoigian_format, ' %H:%M - %d/%m/%Y')

        content_full = ""
        for content in item['Content']:
            content_full = content_full + " " + content
        values = (  # This is the value we want to pass into the database
            item['Category'],
            item['Url'],
            item['Title'],
            item['Introl'],
            content_full,
            # item['Createdate'],
            thoigian_format,
        )
        try:
            # SQL statement
            sql = 'INSERT INTO dataweb VALUES (%s,%s,%s,%s,%s,%s)'
            self.curr.execute(sql, values)  # Execute with execute
            print("Data inserted successfully")
        except Exception as e:
            print('Insert error:', e)
            self.conn.rollback()
        else:
            self.conn.commit()  # Every time you insert it, you commit it, and the data is saved
# Data stored in redis in

    def redis_exists(self,key):
        val = self.redis_db.get(key)
        if val == "exist":
            return True
        return False