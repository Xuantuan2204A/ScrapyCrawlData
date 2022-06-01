import scrapy
from CrawldataDb.items import CrawldatadbItem
# from CrawldataDb.items import NewItem
from scrapy.conf import settings
import pymysql
import redis
import hashlib


class CategorySpider(scrapy.Spider):
    name = "thegioihoinhap"

    service_id = 0
    allowed_domains = ["example.com"]
    # urls = [
    #         'https://thegioihoinhap.vn/'
    #     ]
    SPIDER_MIDDLEWARES = {
        'scrapy_deltafetch.DeltaFetch ':  100,
    }

    def __init__(self,url=None, cate = None, debug=None, *args, **kwargs):
    
        self.crawl_one_url = url
        self.crawl_one_cate = cate
        self.debug= debug
        # self.count = 1
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
        print("===")
        print("BEGIN CRAWLER WEBSITE ") + self.allowed_domains[0]
        print("===")
        # page = 1
        # cf_domain = self.allowed_domains[0]
        # parser_xpath = self.get_xpaths()
        # if parser_xpath:
        #     if self.crawl_one_url == None and self.crawl_one_cate == None :
        #         list_data = DispatcherLibrary(self.service_id, self.group).getCateUrls(cf_domain)

        #     elif self.crawl_one_url != None and self.crawl_one_cate == None :
        #         Pass
        #     elif self.crawl_one_url == None and self.crawl_one_cate != None :
        #         Pass
        #     else:
        #         print("[Error] GET XPATH IN DB ERROR")


        mycursor = self.conn.cursor()
        mycursor.execute("SELECT * FROM Category_website")
        myresult = mycursor.fetchall()
        # print myresult
        for url in myresult:
            yield scrapy.Request(url=url[0], callback=self.parsePage)

    def parsePage(self, response):
        for pageurl in response.xpath("//section[@id='content']/div//div/h2/a"):
            page_url = pageurl.xpath('./@href').extract_first()
            string = str(page_url.encode('utf-8'))
            key_insert = hashlib.md5(str(string).decode(
                'utf-8').encode('utf-8')).hexdigest()
            duplicate = self.redis_exists(key_insert)
            self.insert_key_to_redis(key_insert)
            if duplicate == True:
                # if key_insert == "ff1542449dae5dc0d82b21f7496ae066":
                #     # self.delete_key_to_redis(key_insert)
                print("[INFO][DEBUG] ITEM ALREADY EXISTS DON'T REQUEST AGAIN")
            else:
                yield scrapy.Request(url=page_url, callback=self.parse)

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
            "..//p[@class='date']//text()").extract_first()
        yield item

    # def delete_key_to_redis(self, key):
    #     try:
    #         self.redis_db.delete(key)
    #         print "[INFO][DEBUG] Remove Key Success"
    #     except Exception as e:
    #         print "[INFO][ERROR] Remove Key Error: "+str(e)
