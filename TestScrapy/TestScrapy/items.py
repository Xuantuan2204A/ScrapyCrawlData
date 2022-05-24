# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field
 
class NewItem(Item):
    Url = Field()
    Title = Field()
    Intro = Field()
    Content = Field()
    Createdate = Field()
