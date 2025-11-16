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
from kongfz_info.items import MainCategoryItem, BookItem
import time
from datetime import datetime
import json
import re

class KongfzBookInfoSpider(scrapy.Spider):
    name = "kongfz_book_info"

    def __init__(self, *args, **kwargs):
        super(KongfzBookInfoSpider, self).__init__(*args, **kwargs)
        self.start_urls = ["https://www.kongfz.com/"]
        self.api_base = "https://search.kongfz.com/pc-gw/search-web/client/pc/product/category/list"

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

    def get_browser_headers(self):
        """获取模拟浏览器的请求头"""
        return {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def get_api_headers(self, referer):
        """获取API请求的headers"""
        return {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': referer,
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Origin': 'https://search.kongfz.com',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site'
        }

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
                time.sleep(2)  # 等待2秒
                info_item = MainCategoryItem()
                info_item['type'] = 'main_category'
                info_item['text'] = link.xpath('normalize-space(.)').get()
                temp_url = link.xpath('@href').get()
                info_item['url'] = response.urljoin(temp_url)
                self.logger.info(f"主分类: {info_item['text']} -> {info_item['url']}")
                yield info_item

                yield Request(
                    url=info_item['url'],
                    callback=self.parse_category,
                    headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
                    }
                )

    def extract_cat_id(self, url):
        """从URL中提取分类ID"""
        # 尝试多种方式提取catId
        patterns = [
            r'/category/(\d+)',
            r'catId=(\d+)',
            r'/(\d+)/?$'
        ]

        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)

        return None

    def parse_category(self, response: HtmlResponse):
        """解析分类信息"""
        if response.status != 200:
            self.logger.error(f"请求失败，状态码: {response.status}")
            return

        cat_id = self.extract_cat_id(response.url)
        self.logger.info(f"提取到分类ID: {cat_id}")

        # 检查Cookie是否设置成功
        # self.logger.info(f"搜索页面响应Cookie: {response.headers.get('Set-Cookie', '无')}")

        max_page = self.settings.get('MAX_PAGE', 10)
        userArea = 1006000000
        cookies = {
            'kfz_uuid': '916fbf6e-8bf4-4087-82aa-3742c365adc5',
            'shoppingCartSessionId': 'f57e1572708ed5b61d2f150f7ddea58e',
            'reciever_area': '1006000000',
            'kfz-tid': 'fc6bdb2b2426fa7b773e6ee156116ff6',
            'PHPSESSID': 'c6492f58baa5096c6f631ac076f79706b4f01d5f',
            'kfz_trace': '916fbf6e-8bf4-4087-82aa-3742c365adc5|12048706|05dacfdc644319fa|-'
        }

        for page in range(1, max_page + 1):
            time.sleep(2)  # 等待2秒
            api_url=f"{self.api_base}?catId={cat_id}&page={page}&userArea={userArea}"
            # print("api_url是:", api_url)
            yield Request(
                url = api_url,
                callback = self.parse_book_list,
                cookies = cookies,
                headers = {
                    'Accept': 'application/json, text/javascript, */*; q=0.01',
                    'Accept-Language': 'zh-CN,zh;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'X-Requested-With': 'XMLHttpRequest',
                    'Referer': response.url,
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Origin': 'https://www.kongfz.com',
                    'Sec-Fetch-Dest': 'empty',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Site': 'same-site'
                },
                meta={
                    'catId': cat_id,
                    # 'actionPath': cat_id,
                    'page': page,
                    'userArea': userArea
                },
                dont_filter=False
            )

    def parse_book_list(self, response):
        """解析书籍列表页面的JSON数据"""
        print(f"获取的json_data: {response.text}")
        print(f"当前爬取的url: {response.url}")
        try:
            # 解析json响应
            json_data = json.loads(response.text)

            # 检查API响应状态
            if json_data.get('status') != 1:
                self.logger.warning(f"API响应异常: {json_data.get('message', 'Unknown error')}")
                return
            data = json_data.get('data', {})
            item_response = data.get('itemResponse', {})
            books = item_response.get('list', [])
            # 处理每本书籍
            for book_data in books:
                # 创建BookItem
                book_item = BookItem()
                # 填充数据
                book_item['title'] = book_data.get('title', '')
                book_item['author'] = book_data.get('author', '')
                book_item['press'] = book_data.get('press', '')
                book_item['quality'] = book_data.get('quality', '')
                book_item['price'] = book_data.get('price', '')
                book_item['show_time'] = book_data.get('showTimeText', '')
                book_item['shop_name'] = book_data.get('shopName', '')
                book_item['img_url'] = book_data.get('imgUrl', '')
                book_item['img_big_url'] = book_data.get('imgBigUrl', '')
                book_item['book_link'] = book_data.get('link', {}).get('pc', '')
                book_item['crawl_time'] = datetime.now().isoformat()
                book_item['source_url'] = response.url
                # 可选：打印信息
                self.log_book_info(book_item)
                yield book_item

        except json.JSONDecodeError as e:
            self.logger.error(f"JSON解析错误: {e}, URL: {response.url}")
        except Exception as e:
            self.logger.error(f"解析书籍列表错误: {e}")

    def log_book_info(self, book_item):
        """记录书籍信息到日志"""
        self.logger.info(f"获取的book title: {book_item['title']}")
        self.logger.info(f"获取的作者: {book_item['author']}")
        self.logger.info(f"获取的出版社: {book_item['press']}")
        self.logger.info(f"获取的书质量: {book_item['quality']}")
        self.logger.info(f"获取的价格: {book_item['price']}")
        self.logger.info(f"获取的上架时间: {book_item['showTimeText']}")
        self.logger.info(f"获取的售卖书店: {book_item['shopName']}")
        self.logger.info(f"获取的图片: {book_item['imgUrl']}")
        self.logger.info(f"获取的大图片: {book_item['imgBigUrl']}")
        self.logger.info(f"获取的图书链接: {book_item['link']['pc']}")

    def closed(self, reason):
        """爬虫关闭时的处理"""
        self.logger.info(f"爬虫关闭，原因: {reason}")

if __name__ == '__main__':
    cmdline.execute('scrapy crawl kongfz_book_info'.split())
