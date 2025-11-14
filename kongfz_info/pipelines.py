# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymongo
import json
from scrapy.exceptions import DropItem
import logging
import hashlib
from datetime import datetime
import redis

class KongfzInfoMongoDBPipeline:
    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.client = None
        self.db = None
        self.collection = None

    @classmethod
    def from_crawler(cls, crawler):
        mongo_uri = crawler.settings.get('MONGO_URI')
        mongo_db = crawler.settings.get('MONGO_DATABASE', 'items')
        return cls(mongo_uri, mongo_db)

    def open_spider(self, spider):
        # 链接MongoDB
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        self.collection = self.db[spider.settings.get('MONGO_COLLECTION', 'kongfz_books')]

        # 创建索引
        self.collection.create_index([('url', pymongo.ASCENDING)], unique=True)
        logging.info('MongoDB index created and connected successfully.')

    def close_spider(self, spider):
        if self.client:
            self.client.close()
            logging.info('MongoDB closed and disconnected.')

    def process_item(self, item, spider):
        try:
            # 转换为字典
            item_dict = ItemAdapter(item).asdict()
            # 插入数据
            result = self.collection.update_one(
                {'url': item_dict['url']},
                {'$set': item_dict},
                upsert=True
            )
            logging.info(f"Inserted news: {item_dict.get('text', 'Unknown')}")
            return item
        except pymongo.errors.DuplicateKeyError:
            raise DropItem(f"Duplicate item found: {item['url']}")
        except Exception as e:
            logging.error(e)
            raise DropItem("Error saving item: {e}")
        return item

class KongfzInfoRedisPipeline:
    """Redis URL存储管道 用于存储URL并去重"""
    def __init__(self, redis_host, redis_port, redis_password, redis_db):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_password = redis_password
        self.redis_db = redis_db
        self.redis_client = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            redis_host=crawler.settings.get('REDIS_HOST', 'localhost'),
            redis_port=crawler.settings.get('REDIS_PORT', 6379),
            redis_password=crawler.settings.get('REDIS_PASSWORD'),
            redis_db=crawler.settings.get('REDIS_DB', 0)
        )

    def open_spider(self, spider):
        """爬虫启动时连接redis"""
        try:
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                password=self.redis_password,
                db=self.redis_db,
                decode_responses=True
            )
            #测试链接
            self.redis_client.ping()
            spider.logger.info('Redis connection established.')
        except Exception as e:
            spider.logger.error("连接Redis失败：", e)
            raise

    def close_spider(self, spider):
        """爬虫关闭时关闭Redis连接"""
        if self.redis_client:
            self.redis_client.close()
            spider.logger.info('Redis connection closed.')

    def generate_url_md5(self, url):
        return hashlib.md5(url.encode('utf-8')).hexdigest()

    def is_duplicate(self, url):
        """检查URL是否已经存在"""
        url_md5 = self.generate_url_md5(url)
        return self.redis_client.sismember('kongfz:books:dupefilter', url_md5)

    def process_item(self, item, spider):
        """处理Item，将URL存入Redis"""
        try:
            item_dict = ItemAdapter(item).asdict()
            url = item_dict.get('url')
            if not url:
                spider.logger.warning("没有url字段，跳过")
                return item

            if self.is_duplicate(url):
                spider.logger.info("url已经存在，跳过:", url)
                return item

            # 生成URL的MD5并添加到去重集合
            url_md5 = self.generate_url_md5(url)
            self.redis_client.sadd('kongfz:books:dupefilter', url_md5)

            # 将URL信息存入待爬取队列
            url_data = {
                'url': url,
                'text': item_dict.get('text'),
            }

            # 使用JSON格式存储
            self.redis_client.lpush('kongfz.books.urls', json.dumps(url_data, ensure_ascii=False))

            spider.logger.info("成功存储：", url)

            # 统计信息
            total_urls = self.redis_client.llen('kongfz:books:urls')
            unique_urls = self.redis_client.scard('kongfz:books:dupefilter')
            spider.logger.info(f"当前队列长度: {total_urls}, 唯一URL数: {unique_urls}")
            return item
        except Exception as e:
            spider.logger.error("Redis存储错误:", e)