import scrapy
from CrawldataDb.items import CrawldatadbItem
# from CrawldataDb.items import NewItem
from scrapy.conf import settings
import pymysql
import redis


class CategorySpider(scrapy.Spider):
    name = "thegioihoinhap"

    SPIDER_MIDDLEWARES = {
        'scrapy_deltafetch.DeltaFetch ':  100,
    }

    def __init__(self, *args, **kwargs):
        self.urls = [
            'https://thegioihoinhap.vn/'
        ]
        self.count = 1
        self.create_connection()
        self.redis_db = redis.Redis(
            host=settings['REDIS_HOST'], port=settings['REDIS_PORT'], db=settings['REDIS_DB_ID'])

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
        mycursor = self.conn.cursor()
        mycursor.execute("SELECT * FROM Category_website")
        myresult = mycursor.fetchall()
        i = 1
        for url in myresult:
            if i == 1:
                i += 1
                yield scrapy.Request(url=url[0], callback=self.parsePage)

    def parsePage(self, response):
        for pageurl in response.xpath("//section[@id='content']/div//div/h2/a"):
            page_url = pageurl.xpath('./@href').extract_first()
            string = str(page_url.encode('utf-8'))
            key_insert = hashlib.md5(str(string).decode(
                'utf-8').encode('utf-8')).hexdigest()
            duplicate = self.redis_exists(key_insert)
            if self.count == 2:
                print "=========="
                print duplicate
                print "=========="
                if duplicate == True:
                    print"[INFO][DEBUG] ITEM ALREADY EXISTS DON'T REQUEST AGAIN"
                else:
                    print"[INFO][DEBUG] NOT DUPLICATE"
                    yield scrapy.Request(url=page_url, callback=self.parse)
            self.count += 1

    def redis_exists(self, key):
        val = self.redis_db.mget(key)
        if val == "exists":
            return True
        return False

    def parse(seft, response):
        item = NewItem()
        item['Category'] = response.xpath(
            ".//meta[@property='article:section']/@content").extract_first()
        item['Url'] = response.url
        item['Title'] = response.xpath(".//h1//text()").extract_first()
        item['Introl'] = response.xpath(
            ".//meta[@name='twitter:description']/@content").extract_first()
        item['Content'] = response.xpath(
            ".//div[@id='post-template']/p//text()").extract()
        item['Createdate'] = response.xpath(
            "..//p[@class='date']//text()").extract_first()
        yield item
