# Scrapy settings for kongfz_info project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "kongfz_info"

SPIDER_MODULES = ["kongfz_info.spiders"]
NEWSPIDER_MODULE = "kongfz_info.spiders"

ADDONS = {}


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "kongfz_info (+http://www.yourdomain.com)"

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Concurrency and throttling settings
#CONCURRENT_REQUESTS = 16
CONCURRENT_REQUESTS_PER_DOMAIN = 1
DOWNLOAD_DELAY = 1

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en",
    "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}


# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    "kongfz_info.middlewares.KongfzInfoSpiderMiddleware": 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    "kongfz_info.middlewares.KongfzInfoDownloaderMiddleware": 543,
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
   "kongfz_info.pipelines.KongfzInfoRedisPipeline": 300,
   # "kongfz_info.pipelines.KongfzInfoMongoDBPipeline": 301,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
FEED_EXPORT_ENCODING = "utf-8"

# 日志设置
LOG_ENABLED = True
# LOG_ENCODING = 'utf-8'
LOG_LEVEL = 'WARNING'           # 日志级别：DEBUG, INFO, WARNING, ERROR, CRITICAL
# LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
# LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'
# LOG_STDOUT = False
# LOG_FILE = None

# scrapy-redis 配置项
# SCHEDULER = "scrapy_redis.scheduler.Scheduler" # 调度器
# DUPEFILTER_CLASS = "scrapy_redis.dupefilter.RFPDupeFilter"  # 指纹去重
# SCHEDULER_PERSIST = True
# REDIS_URL = "redis://127.0.0.1:6379/0" # redis服务器地址
# SCHEDULER_QUEUE_CLASS = "scrapy_redis.queue.PriorityQueue" # 优先级队列，使用有序集合来存储
# SCHEDULER_QUEUE_CLASS = "scrapy_redis.queue.FifoQueue"  # 先进先出
# SCHEDULER_QUEUE_CLASS = "scrapy_redis.queue.LifoQueue"  # 后进先出


# MongoDB配置
MONGO_URI = 'mongodb://localhost:27017'  # MongoDB连接字符串
MONGO_DATABASE = 'kongfz_info'           # 数据库名称
MONGO_COLLECTION = 'kongfz_book'         # 集合名称

# redis配置
REDIS_HOST = 'localhost'  # Redis服务器地址
REDIS_PORT = 6379         # Redis端口
REDIS_PASSWORD = None      # Redis密码（如果没有则为None）
REDIS_DB = 0              # Redis数据库编号

# 去重队列名称
REDIS_URLS_KEY = 'kongfz:books:urls'          # 待爬取URL队列
REDIS_DUPE_KEY = 'kongfz:books:dupefilter'    # 去重集合

# 启用Redis相关组件
DUPEFILTER_CLASS = "scrapy_redis.dupefilter.RFPDupeFilter"
SCHEDULER = "scrapy_redis.scheduler.Scheduler"
SCHEDULER_PERSIST = True  # 是否持久化调度队列