# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

# 第一阶段：分类信息Item
class KongfzCategoryItem(scrapy.Item):
    category_name = scrapy.Field()
    category_url = scrapy.Field()
    parent_category = scrapy.Field()
    crawl_time = scrapy.Field()

# 第二阶段：书籍信息Item
class KongfzBookItem(scrapy.Item):
    book_title = scrapy.Field()
    book_price = scrapy.Field()
    book_url = scrapy.Field()
    category = scrapy.Field()
    crawl_time = scrapy.Field()
