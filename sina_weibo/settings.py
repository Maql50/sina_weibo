# encoding=utf-8
BOT_NAME = 'sina_weibo'

SPIDER_MODULES = ['sina_weibo.spiders']
NEWSPIDER_MODULE = 'sina_weibo.spiders'
LOG_LEVEL = 'INFO' # default is DEBUG
URL_DIR = '/data/urls'

DOWNLOADER_MIDDLEWARES = {
    "sina_weibo.middleware.UserAgentMiddleware": 401,
    "sina_weibo.middleware.CookiesMiddleware": 402,
#     'sina_weibo.middleware.ProxyMiddleware': 740,
#    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 750,
}

#HTTP_PROXY = 'http://localhost:5678'
#HTTPS_PROXY = 'http://localhost:5678'
#HTTP_PROXY_USER = 'crawler'
#HTTP_PROXY_PASS = 'dcPass66'

EXTENSIONS = {
    'sina_weibo.extensions.LogDCStats': 500
}

LOGSTATS_INTERVAL = 60.0

# Graphite connections
# -------------------------------------------------
GRAPHITE_UDP_IP = '123.206.206.77'
GRAPHITE_UDP_PORT = 8125

ITEM_PIPELINES = {"sina_weibo.pipelines.PostgresPipleline": 10}

# SCHEDULER = 'sina_weibo.redis.scheduler.Scheduler'
# SCHEDULER_PERSIST = True
# SCHEDULER_QUEUE_CLASS = 'sina_weibo.redis.queue.SpiderQueue'
# DUPEFILTER_CLASS = 'sina_weibo.redis.dupefilter.RFPDupeFilter'
# REDIS_URL = None
# REDIS_HOST = 'localhost'
# REDIS_PORT = 6379

#disable db
# DATA_DB = {
#   'host' : '123.206.194.38',
#   'user': 'crawler',
#   'password' : '8uGGdCybtkQz',
#   'dbname' : 'datacell',
#   'port' : '5432'
# }

DATA_DB = {
    'host' : 'localhost',
    'user': 'postgres',
    'password' : '123321',
    'dbname' : 'postgres',
     'port' : '5432'
}

META_DB = {
  'host' : '123.206.194.38',
  'user': 'crawler',
  'password' : '8uGGdCybtkQz',
  'dbname' : 'dcsys',
  'port' : '5432'
}

# META_DB = {
#   'host' : 'localhost',
#   'user': 'postgres',
#   'password' : '123321',
#   'dbname' : 'postgres',
#   'port' : '5432'
# }

DOWNLOAD_DELAY = 5  # 间隔时间
STATS_TABLE = 'crawler_stats'
LOG_TABLE = 'crawler_log'
CLOSESPIDER_PAGECOUNT = 0
CLOSESPIDER_ERRORCOUNT = 0
# CONCURRENT_ITEMS = 1000
# CONCURRENT_REQUESTS = 100
# REDIRECT_ENABLED = False
# CONCURRENT_REQUESTS_PER_DOMAIN = 100
# CONCURRENT_REQUESTS_PER_IP = 0
# CONCURRENT_REQUESTS_PER_SPIDER=100
# DNSCACHE_ENABLED = True
# LOG_LEVEL = 'INFO'    # 日志级别
# CONCURRENT_REQUESTS = 70

PHANTOMJS_SERVICE_ARGS = ['—debug=true', '—load-images=false', '—webdriver-loglevel=debug', '—ignore-ssl-errors=yes']
