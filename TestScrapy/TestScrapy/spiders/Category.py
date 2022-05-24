# -*- coding: utf-8 -*-
from cgitb import text
import scrapy
from TestScrapy.items import NewItem
from scrapy.linkextractors import LinkExtractor

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
        item=NewItem()
        item['Url'] = response.xpath(".//link[@rel='canonical']//@href").extract_first()
        item['Title'] = response.xpath(".//div[@class='tdb-block-inner td-fix-index']//h1/text()").extract_first()
        item['Intro'] = response.xpath(".//meta[@name='description']/@content").extract_first()
        item['Content'] = response.xpath("//div[contains(@class,'meta-related')]/p//text() [not(ancestor::strong)]").extract()
        item['Createdate'] = response.xpath(".//meta[@property='article:published_time']/@content").extract_first()
        yield item
