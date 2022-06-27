# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.item import Item, Field

class CrawldatadbItem(Item):
    category = Field()
    url_title = Field()
    title = Field()
    introl = Field()
    content = Field()
    createdate = Field()
    category_url = Field()
    category_name = Field()
    pageurl = Field()    
    page_url = Field()