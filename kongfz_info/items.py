# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from itemloaders.processors import TakeFirst, MapCompose

class MainCategoryItem(scrapy.Item):
    """主分类信息Item"""
    type = scrapy.Field(output_processor=TakeFirst())
    text = scrapy.Field(output_processor=TakeFirst())
    url = scrapy.Field(output_processor=TakeFirst())

def clean_text(text):
    """清理文本数据"""
    if text:
        return text.strip().replace('\n', ' ').replace('\t', ' ')
    return text

def clean_price(price):
    """清理价格数据"""
    if price:
        # 移除货币符号等
        return str(price).replace('￥', '').replace(',', '').strip()
    return price


class BookItem(scrapy.Item):
    # 书籍基本信息
    title = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=TakeFirst()
    )
    author = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=TakeFirst()
    )
    press = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=TakeFirst()
    )
    quality = scrapy.Field(output_processor=TakeFirst())
    price = scrapy.Field(
        input_processor=MapCompose(clean_price),
        output_processor=TakeFirst()
    )

    # 时间信息
    show_time = scrapy.Field(output_processor=TakeFirst())
    crawl_time = scrapy.Field(output_processor=TakeFirst())

    # 商家信息
    shop_name = scrapy.Field(output_processor=TakeFirst())

    # 链接信息
    img_url = scrapy.Field(output_processor=TakeFirst())
    img_big_url = scrapy.Field(output_processor=TakeFirst())
    book_link = scrapy.Field(output_processor=TakeFirst())

    # 来源信息
    source_url = scrapy.Field(output_processor=TakeFirst())
    category = scrapy.Field(output_processor=TakeFirst())


