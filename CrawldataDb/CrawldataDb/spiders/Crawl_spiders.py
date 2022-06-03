import scrapy
from CrawldataDb.items import CrawldatadbItem
from scrapy.conf import settings
import pymysql
import redis
import hashlib


class CategorySpider(scrapy.spiders.Spider):
    name = "thegioihoinhap"
    
    # urls = [
    #         'https://thegioihoinhap.vn/'
    #     ]
    SPIDER_MIDDLEWARES = {
        'scrapy_deltafetch.DeltaFetch ':  100,
    }

    def __init__(self,name=None, url=None, *args, **kwargs):
        self.url =url
        self.create_connection()
        self.redis_db = redis.Redis(
            host=settings['REDIS_HOST'], port=settings['REDIS_PORT'], db=settings['REDIS_DB_ID'])
        super(CategorySpider, self).__init__(*args, **kwargs)
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

    def start_requests(self):
        item = CrawldatadbItem()
        # self.logger.info('Start url: %s' % self.url)
        # yield scrapy.Request(url=self.url, callback=self.parse)

        mycursor = self.conn.cursor()
        mycursor.execute("SELECT * FROM Category_website")
        myresult = mycursor.fetchall()
        for item['Category_url'] in myresult:
            print(item['Category_url'][1])
            # yield scrapy.Request(url=item['Category_url'],meta={'Category_url': item['Category_url']}, callback=self.parsePage)
            
    def parsePage(self, response):
        item = CrawldatadbItem()
        for item['pageurl'] in response.xpath("//section[@id='content']/div//div/h2/a"):
            item['page_url'] = item['pageurl'].xpath('./@href').extract_first()
            string = str(item['page_url'].encode('utf-8'))
            key_insert = hashlib.md5(str(string).decode(
                'utf-8').encode('utf-8')).hexdigest()
            duplicate = self.redis_exists(key_insert)
            self.insert_key_to_redis(key_insert)
            if duplicate == True:
                print("[INFO][DEBUG] ITEM ALREADY EXISTS DON'T REQUEST AGAIN")
                # self.delete_key_to_redis(key_insert)
            else:
                yield scrapy.Request(url=item['page_url'], meta={'page_url': item['page_url']}, callback=self.parse)

    def redis_exists(self, key):
        val = self.redis_db.get(key)
        if val == "exist":
            return True
        return False

    def insert_key_to_redis(self, key):
        nano = self.redis_db.set(key, "exist")                                                                                                                  

    def parse(seft, response):
        item = CrawldatadbItem()
        item['Category'] = response.xpath(
            ".//meta[@property='article:section']/@content").extract_first()
        item['Url'] = response.url
        item['Title'] = response.xpath(".//h1//text()").extract_first()
        item['Introl'] = response.xpath(
            ".//meta[@name='twitter:description']/@content").extract_first()
        item['Content'] = response.xpath(
            ".//div[@id='post-template']/p//text()").extract()
        item['Createdate'] = response.xpath(
            ".//p[@class='date']//text()").extract_first()
        yield item


    def delete_key_to_redis(self, key):
        try:
            self.redis_db.delete(key)
            print( "[INFO][DEBUG] Remove Key Success")
        except Exception as e:
            print( "[INFO][ERROR] Remove Key Error: "+str(e))
