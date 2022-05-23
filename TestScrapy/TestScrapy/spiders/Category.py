import scrapy
import mysql


class CategorySpider(scrapy.Spider):
    name = "basic"
    def start_requests(self):
        urls = [
            'https://tapchibonbanh.com/'
        ]
        for url in urls:
            yield scrapy.Request(url = url, callback=self.parsePage)

    def parsePage(self, response):
        for page in response.xpath("//h3[contains(@class,'td-module-title')]//a [not(ancestor::div[@class='td-trending-now-wrapper'])]"):
            page_url = page.xpath('./@href').extract_first()
            yield scrapy.Request(url = page_url, callback=self.parse)
    def parse(self, response):
        postTitle = response.xpath("//div[@class='tdb-block-inner td-fix-index']//h1/text()").extract_first()
        postIntro = response.xpath("//meta[@name='description']/@content").extract_first()
        postContent = response.xpath("//div[@class='meta-related']/p//text() [not(ancestor::strong)]").extract_first()
        postCreatedat = response.xpath("//div[@class='tdb-block-inner td-fix-index']//time/@datetime").extract_first()

        yield {
            'title': postTitle,
            'introl': postIntro,
            'content': postContent,
            'createdat': postCreatedat,
        }