import sys
import os

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, project_root)

import scrapy
from scrapy import cmdline, Spider
from scrapy.http import HtmlResponse,Request
from scrapy.exceptions import CloseSpider
from urllib.parse import urlencode
import time
import datetime
import json
import re

class KongfzBookInfoSpider(scrapy.Spider):
    name = "kongfz_book_info"

    def __init__(self, *args, **kwargs):
        super(KongfzBookInfoSpider, self).__init__(*args, **kwargs)
        self.start_urls = ["https://www.kongfz.com/"]
        # self.logger = logging.getLogger(__name__)

    def start_requests(self):
        """重写start_requests以添加自定义headers"""
        for url in self.start_urls:
            yield Request(
                url=url,
                callback=self.parse,
                headers={
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                },
                dont_filter=True
            )

    def parse(self, response):
        """解析主页分类信息"""
        if response.status != 200:
            self.logger.error(f"请求失败，状态码: {response.status}")
            return

        """更精确的解析方法"""
        # 方案1：直接定位左上角的主要分类区域
        main_category_sections = response.xpath("""
            //div[contains(@class, 'cagetory-box')]/
            div[contains(@class, 'list-group')]/
            div[contains(@class, 'list-group-item')]
        """)

        for section in main_category_sections:
            # 提取主分类文本和链接
            main_links = section.xpath("""
                .//div[contains(@class, 'item-text')]//a[text()]
            """)

            for link in main_links:
                info_dict = dict()
                info_dict['text'] = link.xpath('normalize-space(.)').get()
                temp_url = link.xpath('@href').get()
                info_dict['url'] = response.urljoin(temp_url)
                print(f"主分类: {info_dict['text']} -> {info_dict['url']}")
                yield info_dict

            # 提取子分类（detail区域）
            # sub_links = section.xpath("""
            #     .//div[contains(@class, 'detail')]//a[text()]
            # """)
            # for link in sub_links:
            #     text = link.xpath('normalize-space(.)').get()
            #     href = link.xpath('@href').get()
            #
            #     if text and href:
            #         full_url = response.urljoin(href)
            #         print(f"  └─ 子分类: {text} -> {full_url}")


    def closed(self, reason):
        """爬虫关闭时的处理"""
        self.logger.info(f"爬虫关闭，原因: {reason}")

if __name__ == '__main__':
    cmdline.execute('scrapy crawl kongfz_book_info'.split())
