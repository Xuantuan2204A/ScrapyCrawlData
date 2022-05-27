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

        dispatcher.connect(self.close_spider, signals.spider_closed)
        dispatcher.connect(self.open_spider, signals.spider_opened)

    def open_spider(self, spider):
            print("spider open")

    def process_item(self, item, spider):
        # self.insert_dataCategory(item)
        self.insertdata(item)
        return item

    def close_spider(self, spider):
        self.mysql_close()

    # def insert_dataCategory(self, item):
    #     values =(   #This is the value we want to pass into the database
    #         item['Category_url'],
    #         item['Category_name'],
    #     )
    #     try:
    #         sql = 'INSERT INTO category_website VALUES (%s,%s)'  #SQL statement
    #         self.curr.execute(sql,values)   #Execute with execute
    #         print("Data inserted successfully")  
    #     except Exception as e:
    #         print('Insert error:', e)
    #         self.conn.rollback()
    #     else:
    #         self.conn.commit()
    #     return item
    def create_connection(self):
        self.conn = pymysql.connect(
            db=settings['MYSQL_DB_NAME'],
            host=settings['MYSQL_HOST'],
            port=settings['MYSQL_PORT'],
            user=settings['MYSQL_USER'],
            passwd=settings['MYSQL_PASSWORD'],
            charset="utf8", use_unicode=True
        )
        self.curr= self.conn.cursor()
        print("Database connection successful")
    # def format_datetime(self):
    #     thoigian_format = self.item['Createdate']
    #     thoigian_format = datetime.strptime(thoigian_format, "%y-%d-%m %H:%M:%S")
    # def datetimefomat(self, item):
    #     str = item['Createdate']
    #     datetime_value = dt.datetime.strptime(str,'%H:%M - %d/%m/%Y')
    #     print(datetime_value)

    def insertdata(self, item):
        thoigian_format = item['Createdate']
        thoigian_format = datetime.strptime(thoigian_format,' %H:%M - %d/%m/%Y')    

        content_full = ""
        for content in item['Content']:
            content_full = content_full + " " + content
        values =(   #This is the value we want to pass into the database
            item['Category'],
            item['Url'],
            item['Title'],
            item['Introl'],
            content_full,
            # item['Createdate'],
            thoigian_format,
        )
        try:
            sql = 'INSERT INTO dataweb VALUES (%s,%s,%s,%s,%s,%s)'  #SQL statement
            self.curr.execute(sql,values)   #Execute with execute
            print("Data inserted successfully")  
        except Exception as e:
            print('Insert error:', e)
            self.conn.rollback()
        else:
            self.conn.commit()  #Every time you insert it, you commit it, and the data is saved
    # def open_spider(self, spider):
    #     self.file = open('items.jl', 'w')

    # def close_spider(self, spider):
    #     self.file.close()


    

    # def format_datetime(self,datetime_str):
    #     #Helper().format_bnews()
    #     funcName = "format_"+self.name
    #     datetime_formater = getattr(Helper(),funcName)
    #     return datetime_formater(datetime_str)
    
    