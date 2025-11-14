#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Project : spider
@File    : kongfz_book_recursive.py
@Author  : Administrator
@Date    : 2025/11/14 15:58
@Desc    : 
"""
import scrapy
from scrapy import cmdline
from scrapy.http import HtmlResponse, Request
import redis
import json
import logging
import time
from urllib.parse import urljoin, urlparse
import re


class KongfzBookRecursiveSpider(scrapy.Spider):
    name = "kongfz_book_recursive"

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 1,
        'ROBOTSTXT_OBEY': False,
    }

    def __init__(self, *args, **kwargs):
        super(KongfzBookRecursiveSpider, self).__init__(*args, **kwargs)
        self.redis_client = None
        self.max_pages_per_category = 5  # 每个分类最多爬取页数

    def start_requests(self):
        """从Redis读取分类URL并开始爬取"""
        # 连接Redis
        try:
            self.redis_client = redis.Redis(
                host='localhost',
                port=6379,
                db=0,
                decode_responses=True
            )
            self.logger.info("Redis连接成功")
        except Exception as e:
            self.logger.error(f"Redis连接失败: {e}")
            return

        # 从Redis队列读取分类URL
        while True:
            # 从右侧弹出URL（FIFO）
            url_data = self.redis_client.rpop('kongfz.books.urls')
            if not url_data:
                self.logger.info("Redis队列中没有更多URL")
                break

            try:
                url_info = json.loads(url_data)
                category_url = url_info.get('url')
                category_name = url_info.get('text', '未知分类')

                if category_url:
                    yield Request(
                        url=category_url,
                        callback=self.parse_category,
                        meta={
                            'category_name': category_name,
                            'current_page': 1,
                            'max_pages': self.max_pages_per_category
                        },
                        headers={
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
                            'Referer': 'https://www.kongfz.com/',
                        }
                    )

            except Exception as e:
                self.logger.error(f"解析Redis URL数据失败: {e}")
                continue

    def parse_category(self, response):
        """解析分类页面，提取商品列表"""
        category_name = response.meta['category_name']
        current_page = response.meta['current_page']
        max_pages = response.meta['max_pages']

        self.logger.info(f"正在爬取分类 [{category_name}] 第 {current_page} 页")

        # 提取商品列表
        # 根据孔夫子网站的实际结构调整选择器
        product_selectors = [
            "//div[contains(@class, 'product-item')]",
            "//div[contains(@class, 'book-item')]",
            "//li[contains(@class, 'item')]",
            "//div[contains(@class, 'item')]"
        ]

        products = None
        for selector in product_selectors:
            products = response.xpath(selector)
            if products:
                self.logger.info(f"使用选择器: {selector}, 找到 {len(products)} 个商品")
                break

        if not products:
            self.logger.warning(f"在分类 [{category_name}] 中未找到商品列表")
            return

        # 解析每个商品
        for product in products:
            try:
                product_info = self.parse_product_item(product, response, category_name)
                if product_info and product_info.get('detail_url'):
                    # 请求商品详情页
                    yield Request(
                        url=product_info['detail_url'],
                        callback=self.parse_product_detail,
                        meta={
                            'category_name': category_name,
                            'base_info': product_info
                        }
                    )
            except Exception as e:
                self.logger.error(f"解析商品项失败: {e}")
                continue

        # 处理翻页
        if current_page < max_pages:
            next_page_url = self.get_next_page_url(response)
            if next_page_url:
                yield Request(
                    url=next_page_url,
                    callback=self.parse_category,
                    meta={
                        'category_name': category_name,
                        'current_page': current_page + 1,
                        'max_pages': max_pages
                    }
                )

    def parse_product_item(self, product, response, category_name):
        """解析商品列表项"""
        product_info = {
            'category_name': category_name,
            'crawl_time': time.strftime('%Y-%m-%d %H:%M:%S'),
            'source_page': response.url
        }

        # 商品标题
        title_selectors = [
            ".//h3/a/text()",
            ".//h4/a/text()",
            ".//a[contains(@class, 'title')]/text()",
            ".//div[contains(@class, 'title')]//a/text()"
        ]

        for selector in title_selectors:
            title = product.xpath(selector).get()
            if title:
                product_info['title'] = title.strip()
                break

        # 商品详情页URL
        url_selectors = [
            ".//h3/a/@href",
            ".//h4/a/@href",
            ".//a[contains(@class, 'title')]/@href",
            ".//div[contains(@class, 'title')]//a/@href"
        ]

        for selector in url_selectors:
            detail_url = product.xpath(selector).get()
            if detail_url:
                product_info['detail_url'] = response.urljoin(detail_url)
                break

        # 价格信息
        price_selectors = [
            ".//span[contains(@class, 'price')]/text()",
            ".//div[contains(@class, 'price')]/text()",
            ".//em[contains(@class, 'price')]/text()"
        ]

        for selector in price_selectors:
            price = product.xpath(selector).get()
            if price:
                product_info['price'] = price.strip()
                break

        # 卖家信息
        seller_selectors = [
            ".//span[contains(@class, 'seller')]/text()",
            ".//div[contains(@class, 'seller')]/text()",
            ".//a[contains(@class, 'shop')]/text()"
        ]

        for selector in seller_selectors:
            seller = product.xpath(selector).get()
            if seller:
                product_info['seller'] = seller.strip()
                break

        self.logger.info(f"提取商品: {product_info.get('title', '未知标题')}")
        return product_info

    def parse_product_detail(self, response):
        """解析商品详情页"""
        base_info = response.meta['base_info']
        category_name = response.meta['category_name']

        self.logger.info(f"正在解析商品详情: {base_info.get('title', '未知商品')}")

        product_detail = base_info.copy()
        product_detail['detail_page'] = response.url

        # 提取详细描述
        description_selectors = [
            "//div[contains(@class, 'product-desc')]//text()",
            "//div[contains(@class, 'description')]//text()",
            "//div[contains(@class, 'detail')]//text()"
        ]

        for selector in description_selectors:
            desc_elements = response.xpath(selector).getall()
            if desc_elements:
                description = ' '.join([desc.strip() for desc in desc_elements if desc.strip()])
                if description:
                    product_detail['description'] = description
                    break

        # 提取作者信息
        author_selectors = [
            "//span[contains(text(), '作者')]/following-sibling::text()",
            "//div[contains(text(), '作者')]/following-sibling::div/text()"
        ]

        for selector in author_selectors:
            author = response.xpath(selector).get()
            if author:
                product_detail['author'] = author.strip()
                break

        # 提取出版社信息
        publisher_selectors = [
            "//span[contains(text(), '出版社')]/following-sibling::text()",
            "//div[contains(text(), '出版社')]/following-sibling::div/text()"
        ]

        for selector in publisher_selectors:
            publisher = response.xpath(selector).get()
            if publisher:
                product_detail['publisher'] = publisher.strip()
                break

        # 提取出版时间
        publish_date_selectors = [
            "//span[contains(text(), '出版时间')]/following-sibling::text()",
            "//div[contains(text(), '出版时间')]/following-sibling::div/text()"
        ]

        for selector in publish_date_selectors:
            publish_date = response.xpath(selector).get()
            if publish_date:
                product_detail['publish_date'] = publish_date.strip()
                break

        # 提取ISBN
        isbn_selectors = [
            "//span[contains(text(), 'ISBN')]/following-sibling::text()",
            "//div[contains(text(), 'ISBN')]/following-sibling::div/text()"
        ]

        for selector in isbn_selectors:
            isbn = response.xpath(selector).get()
            if isbn:
                product_detail['isbn'] = isbn.strip()
                break

        yield product_detail

    def get_next_page_url(self, response):
        """获取下一页URL"""
        next_page_selectors = [
            "//a[contains(text(), '下一页')]/@href",
            "//a[contains(@class, 'next')]/@href",
            "//li[contains(@class, 'next')]/a/@href"
        ]

        for selector in next_page_selectors:
            next_url = response.xpath(selector).get()
            if next_url:
                return response.urljoin(next_url)

        return None

    def closed(self, reason):
        """爬虫关闭时的处理"""
        if self.redis_client:
            self.redis_client.close()
        self.logger.info(f"递归爬虫关闭，原因: {reason}")


if __name__ == '__main__':
    cmdline.execute('scrapy crawl kongfz_book_recursive'.split())