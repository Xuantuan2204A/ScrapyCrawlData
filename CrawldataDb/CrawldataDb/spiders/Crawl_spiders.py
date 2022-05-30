import scrapy
from CrawldataDb.items import CrawldatadbItem
# from CrawldataDb.items import NewItem
from scrapy.conf import settings
import pymysql
import redis


class CategorySpider(scrapy.Spider):
    name = "thegioihoinhap"

    def __init__(self, *args, **kwargs):
        self.urls = [
            'https://thegioihoinhap.vn/'
        ]
        self.create_connection()
        self.redis_db = redis.Redis(host=settings['REDIS_HOST'], port=settings['REDIS_PORT'], db=settings['REDIS_DB_ID'])

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

    def start_requests(self):
        mycursor = self.conn.cursor()
        mycursor.execute("SELECT Category_url FROM Category_website")
        myresult = mycursor.fetchall()
        for url in myresult:
            yield scrapy.Request(url=url[0], callback=self.parsePage)


    def parsePage(self, response):
        for pageurl in response.xpath("//section[@id='content']/div//div/h2/a"):
            page_url1 = pageurl.xpath('./@href').extract_first()
            # if page_url1 not in self.media_urls:
            #     string= str(page_url1).encode('utf-8')
            #     key_insert = hashlib.md5(str(string).decode('utf-8').encode('utf-8')).hexdigest()
            #     duplicate = self.redis_exists(key_insert)
            #     if duplicate == True:
            #         print("ITEM ALREADY EXISTS DON'T REQUEST AGAIN")
            #     else:
            #         yield Request(url=page_url1, callback=self.parsePage)
            # # yield scrapy.Request(url=page_url1, callback=self.parse)


            
            # if post_url not in self.visited_urls:
            #     if post_url not in self.media_urls:
            #         # if meta_req["domain"] == "nongnghiep.vn":
            #         #     meta_req['domain'] = meta_req['domain'].replace("‘", "")

            #         string = str(post_url.encode('utf-8')) + "_" + str(meta_req['domain'])
            #         key_insert = hashlib.md5(str(string).decode('utf-8').encode('utf-8')).hexdigest()
            #         # print "==================================="
            #         # print post_url
            #         # print key_insert
            #         # print "==================================="

            #         duplicate = self.redis_exists(key_insert)

            #         # IF NGÀY ĐĂNG LÀ 2021-05-03 or 2021-05-04 
            #         # duplicate = False

            #         if duplicate == True:
            #             print "[INFO] ITEM ALREADY EXISTS DON'T REQUEST AGAIN"
            #             print "[INFO] THE KEY: " + key_insert
            #             print "[INFO] THE WEB LINK : " + str(post_url.encode("utf-8"))
            #         else:
            #             # print "============= REQUEST TRUE ============="
            #             # print post_url
            #             # print "=========================================="
            #             yield Request(post_url, callback=self.parse_full_post, meta=meta_req)
            #     else:
            #         # print "============== FILTER MEDIA =============="
            #         # print post_url
            #         # print "=========================================="
            #         logging.info( "[FILTER MEDIA] request filtered " + post_url)
            # else:
            #     # print "============== FILTER REQUEST =============="
            #     # print post_url
            #     # print "============================================"
            #     logging.info( "[FILTER] request filtered " + post_url)
            #     self.to_update_urls.append(post_url)


    def redis_exists(self,key):
        val = self.redis_db.get(key)
        if val == "exist":
            return True
        return False

    
    def parse(seft, response):
        item = CrawldatadbItem()
        item['Category'] = response.xpath(
            ".//meta[@property='article:section']/@content").extract_first()
        item['Url'] = response.xpath(
            ".//link[@rel='canonical']/@href").extract_first()
        item['Title'] = response.xpath(".//h1//text()").extract_first()
        item['Introl'] = response.xpath(
            ".//meta[@name='twitter:description']/@content").extract_first()
        item['Content'] = response.xpath(
            ".//div[@id='post-template']/p//text()").extract()
        item['Createdate'] = response.xpath(
            "..//p[@class='date']//text()").extract_first()
        yield item

        # print(item)