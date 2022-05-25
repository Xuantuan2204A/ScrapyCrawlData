import scrapy
from CrawldataDb.items import CrawldatadbItem
# from CrawldataDb.items import NewItem

class CategorySpider(scrapy.Spider):
    name = "thegioihoinhap"
    def start_requests(self):
        urls = [
            'https://thegioihoinhap.vn/'
        ]
        for url in urls:
            yield scrapy.Request(url = url, callback=self.parsePage)

    def parsePage(self, response):
        for page in response.xpath("//ul[@class='menu']/li/a [not(ancestor::nav[@class='footer-menu']) and not(ancestor::li[@id='menu-item-8476'])] | //ul[@class='menu']/li/ul/li/a [not(ancestor::nav[@class='footer-menu'])]"):
            page_url = page.xpath('./@href').extract_first()
            yield scrapy.Request(url = page_url, callback=self.parseUrl)

    # def parsePagemini(self, response):
    #     ''' do something with this parser '''
    #     next_page = response.xpath("//a[@class='next page-numbers']/@href").extract_first()
    #     if next_page is not None:
    #         next_page = response.urljoin(next_page)
    #         yield scrapy.follow(next_page, callback=self.parseUrl)        
    def parseUrl(self, response):
        for pageurl in response.xpath("//section[@id='content']/div//div/h2/a"):
            page_url1 = pageurl.xpath('./@href').extract_first()
            yield scrapy.Request(url = page_url1, callback=self.parse)
    def parse(seft, response):
        item = CrawldatadbItem()
        item['Category'] = response.xpath(".//meta[@property='article:section']/@content").extract_first()
        item['Url'] = response.xpath(".//link[@rel='canonical']/@href").extract_first()
        item['Title'] = response.xpath(".//h1//text()").extract_first()
        item['Introl'] = response.xpath(".//meta[@name='twitter:description']/@content").extract_first()
        item['Content'] = response.xpath(".//div[@id='post-template']/p//text()").getall()
        item['Createdate'] = response.xpath(".//meta[@property='article:published_time']/@content").extract_first()
        yield item
        
    
