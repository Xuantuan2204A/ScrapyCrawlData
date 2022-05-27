import scrapy
from CrawldataDb.items import CrawldatadbItem
# from CrawldataDb.items import NewItem
from scrapy.conf import settings
import pymysql


class CategorySpider(scrapy.Spider):
    name = "thegioihoinhap"

    visited_urls = []
    visited_datetimes = []

    def __init__(self, *args, **kwargs):
        self.urls = [
            'https://thegioihoinhap.vn/'
        ]
        self.create_connection()

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
            print(url)
        # for url in url:
            # yield scrapy.Request(url=url, callback=self.parsePage)

    # def parsePage(self, response):
    #     item = CrawldatadbItem()
    #     for page in response.xpath(".//ul[@class='menu']/li/a [not(ancestor::nav[@class='footer-menu']) and not(ancestor::li[@id='menu-item-8476'])] | //ul[@class='menu']/li/ul/li/a [not(ancestor::nav[@class='footer-menu'])]"):
    #         item['Category_url'] = page.xpath('./@href').extract_first()
    #         item['Category_name'] = page.xpath('.//text()').extract_first()
    #         yield item
            # yield scrapy.Request(url=item['Category_url'], callback=self.parseUrl)

    # def parsePagemini(self, response):
    #     url = response.xpath(
    #         ".//ul[@class='menu']/li/a/@href [not(ancestor::nav[@class='footer-menu']) and not(ancestor::li[@id='menu-item-8476'])] | //ul[@class='menu']/li/ul/li/a/@href [not(ancestor::nav[@class='footer-menu'])]").extract_first()
    #     for page in range(0, 20):
    #         self.start_urls.append(url + str(page))
    #     yield scrapy.Request(url=url, callback=self.parseUrl)

    # def parsePage(self, response):
    #     for pageurl in response.xpath("//section[@id='content']/div//div/h2/a"):
    #         page_url1 = pageurl.xpath('./@href').extract_first()
    #         yield scrapy.Request(url=page_url1, callback=self.parse)

    # def parse(seft, response):

    #     item = CrawldatadbItem()
    #     item['Category'] = response.xpath(
    #         ".//meta[@property='article:section']/@content").extract_first()
    #     item['Url'] = response.xpath(
    #         ".//link[@rel='canonical']/@href").extract_first()
    #     item['Title'] = response.xpath(".//h1//text()").extract_first()
    #     item['Introl'] = response.xpath(
    #         ".//meta[@name='twitter:description']/@content").extract_first()
    #     item['Content'] = response.xpath(
    #         ".//div[@id='post-template']/p//text()").extract()
    #     item['Createdate'] = response.xpath(
    #         "..//p[@class='date']//text()").extract_first()
    #     # yield item

    #     print(item)
