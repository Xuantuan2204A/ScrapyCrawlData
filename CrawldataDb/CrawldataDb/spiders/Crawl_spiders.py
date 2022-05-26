import scrapy
from CrawldataDb.items import CrawldatadbItem
# from CrawldataDb.items import NewItem
from scrapy.conf import settings


class CategorySpider(scrapy.Spider):
    name = "thegioihoinhap"

    def __init__(self, *args, **kwargs):
        super(CategorySpider, self).__init__(*args, **kwargs)
        self.urls = [
            'https://thegioihoinhap.vn/'
        ]
        self.dataformat = settings['DATETIME_FORMAT']
        self.fonttext = settings['FEED_EXPORT_ENCODING']

    def start_requests(self):
        for url in self.urls:
            yield scrapy.Request(url=url, callback=self.parsePage)

    def parsePage(self, response):
        for page in response.xpath(".//ul[@class='menu']/li/a [not(ancestor::nav[@class='footer-menu']) and not(ancestor::li[@id='menu-item-8476'])] | //ul[@class='menu']/li/ul/li/a [not(ancestor::nav[@class='footer-menu'])]"):
            page_url = page.xpath('./@href').extract_first()
            yield scrapy.Request(url=page_url, callback=self.parseUrl)

    # def parsePagemini(self, response):
    #     url = response.xpath(
    #         ".//ul[@class='menu']/li/a/@href [not(ancestor::nav[@class='footer-menu']) and not(ancestor::li[@id='menu-item-8476'])] | //ul[@class='menu']/li/ul/li/a/@href [not(ancestor::nav[@class='footer-menu'])]").extract_first()
    #     for page in range(0, 20):
    #         self.start_urls.append(url + str(page))
    #     yield scrapy.Request(url=url, callback=self.parseUrl)

    def parseUrl(self, response):
        for pageurl in response.xpath("//section[@id='content']/div//div/h2/a"):
            page_url1 = pageurl.xpath('./@href').extract_first() 
            yield scrapy.Request(url=page_url1, callback=self.parse)

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
            ".//div[@id='post-template']/p//text()").getall()
        item['Createdate'] = response.xpath(
            ".//meta[@property='article:published_time']/@content").extract_first()
        yield item 

        # print(item)