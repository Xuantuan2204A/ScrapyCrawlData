# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field


class CrawldatadbItem(Item):
    Category = Field()
    Url = Field()
    Title = Field()
    Introl = Field()
    Content = Field()
    Createdate = Field()
    Category_url = Field()
    Category_name = Field()
    pageurl = Field()    
    page_url = Field()