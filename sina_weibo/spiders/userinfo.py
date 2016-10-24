# encoding=utf-8
import re
import datetime
import socket
import logging
import json
import pytz
import uuid
import base64
import requests
import psycopg2
from scrapy.selector import Selector
from scrapy.http import Request
from sina_weibo.items import InformationItem, TweetsItem, FollowsItem, FansItem
from sina_weibo.weiboID import weiboID
from sina_weibo.handlers import PGHandler
from sina_weibo.redis.spiders import RedisSpider
from scrapy.spiders import Spider
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from scrapy.conf import settings
from scrapy.exceptions import CloseSpider
from sina_weibo.postgres import db
from parse_user_detil import get_user_detail

def url_generator_by_file(urlfile):
    with open (urlfile) as f:
        urls = f.readlines()
    for url in urls:
        yield url.rstrip()

class Spider(Spider):
    name = "sina_user_weibo"
    domain = "http://weibo.cn"
    target = 'weibo'
    hostname = socket.gethostname()
    jobtime = datetime.datetime.now()
    time_record_static = datetime.datetime.now()
    time_record_dynamic = datetime.datetime.now()
    cell_id = None
    job_id = None
    user_id = None
    error_found = False
    ua = 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0'
    cookies = {}
    handle_httpstatus_list = [302]
#     redis_key = "sina_weibo:start_ids"
#    start_ids = weiboID
#     start_ids = [
#         5235640836, 5676304901, 5871897095, 2139359753, 5579672076, 2517436943, 5778999829, 5780802073, 2159807003,
#         1756807885, 3378940452, 5762793904, 1885080105, 5778836010, 5722737202, 3105589817, 5882481217, 5831264835,
#         2717354573, 3637185102, 1934363217, 5336500817, 1431308884, 5818747476, 5073111647, 5398825573, 2501511785,
#     ]

#     start_urls = [
#         'http://weibo.com/u/1563296410'
#     ]

    account = ''
    password = ''


    def __init__(self, *args, **kwargs):
        super(Spider, self).__init__(self.name, *args, **kwargs)
        dispatcher.connect(self.spider_opened, signals.spider_opened)
        dispatcher.connect(self.spider_closed, signals.spider_closed)

#        paramsjson = json.loads(params)
#        start_urls = paramsjson.get('start_urls')
#        url_file = paramsjson.get('url_file')
#        if start_urls:
#            self.start_urls = start_urls.split(',')
#        elif url_file: # get urls from url file
#            self.start_urls = url_generator_by_file(settings['URL_DIR'] + '/' + url_file)

        meta_conn_string = ' '.join(["host=", settings['META_DB']['host'], "dbname=", settings['META_DB']['dbname'], \
            "user=", settings['META_DB']['user'], "password=", settings['META_DB']['password'], "port=", settings['META_DB']['port']])
        self.meta_conn = psycopg2.connect(meta_conn_string)
        self.meta_conn.autocommit = True
        self.meta_session = db(self.meta_conn)
        data_conn_string = ' '.join(["host=", settings['DATA_DB']['host'], "dbname=", settings['DATA_DB']['dbname'], \
            "user=", settings['DATA_DB']['user'], "password=", settings['DATA_DB']['password'], "port=", settings['DATA_DB']['port']])
        self.data_conn = psycopg2.connect(data_conn_string)
        self.data_session = db(self.data_conn)
        resource = self.meta_session.getOne("select account, password, ua_mobile, download_delay from crawler_resource where host = '%s' and target = '%s'" % (self.hostname, self.target))

        self.start_ids = self.data_session.getAll("select uid from dc36.weibo_user_simple")

        self.account = resource[0]
        self.password =resource[1]
        self.cell_name = self.name
#        self.ua = resource[2]
#        download_delay = resource[3]
        self.cookies = self.getCookies()
        self.cell_name = self.name
#        self.dc_custom_settings = {}
#        if download_delay:
#            self.dc_custom_settings['DOWNLOAD_DELAY'] = download_delay
#        self.dstats = {}
#        self.error_found = False

    def spider_opened(self, spider):
        # get stats
        handler = PGHandler(self.name)
        handler.setLevel(settings.get('LOG_LEVEL'))
        logger = logging.getLogger()
        logger.addHandler(handler)
        self.dstats = spider.crawler.stats.get_stats()
        self.collect_stats(status='running')

    def spider_closed(self, spider):
        self.data_conn.close()
        self.collect_stats(status='finished')

#    @classmethod
#	 def from_crawler(cls, crawler, *args, **kwargs):
#        spider = cls(*args, **kwargs)

        # change settings in the runtime
        # have to hack crawler.py to disable settings freeze (comment out self.settings.freeze())
        # TODO: try to start spider in a different approach so as to get parameter earlier than crawler is initialized and change settings accordingly
#        if spider.dc_custom_settings:
#            crawler.settings.setdict(spider.dc_custom_settings, 'cmdline')
       
#        spider._set_crawler(crawler)

#        return spider


    def start_requests(self):
        # if self.start_urls:
        #     self.start_ids = [url.split('/')[-1] for url in self.start_urls]
        for ID in self.start_ids:
            ID = str(ID[0])
            follows = []
            followsItems = FollowsItem()
            followsItems["user_id"] = ID
            followsItems["follows"] = follows
            fans = []
            fansItems = FansItem()
            fansItems["user_id"] = ID
            fansItems["fans"] = fans

            url_follows = "http://weibo.cn/%s/follow" % ID
            url_fans = "http://weibo.cn/%s/fans" % ID
            url_tweets = "http://weibo.cn/%s/profile?filter=1&page=1" % ID

            self.time_record_dynamic = datetime.datetime.now()

            if (self.time_record_dynamic - self.time_record_static).seconds > 60:
                print self.time_record_dynamic
                print self.time_record_static
                self.cookies = self.getCookies()
                self.time_record_static = datetime.datetime.now()
                self.time_record_dynamic = datetime.datetime.now()

            yield get_user_detail(uid=ID, cookies=self.cookies)

    def parse2(self, response):
        """ 抓取微博数据 """
        if response.status >= 300:
            logging.error(response.status)
            logging.info(response.url)
            logging.info(response.body)
            if response.status == 302:
                self.error_found = True
                raise CloseSpider("访问异常: " + str(response.status))
            
        selector = Selector(response)
        tweets = selector.xpath('body/div[@class="c" and @id]')
            
        for tweet in tweets:
            tweetsItems = TweetsItem()
            tweet_id = tweet.xpath('@id').extract_first()  # 微博ID
            content = tweet.xpath('div/span[@class="ctt"]/text()').extract_first()  # 微博内容
            cooridinates = tweet.xpath('div/a/@href').extract_first()  # 定位坐标
            like = re.findall(u'\u8d5e\[(\d+)\]', tweet.extract())  # 点赞数
            transfer = re.findall(u'\u8f6c\u53d1\[(\d+)\]', tweet.extract())  # 转载数
            comment = re.findall(u'\u8bc4\u8bba\[(\d+)\]', tweet.extract())  # 评论数
            others = tweet.xpath('div/span[@class="ct"]/text()').extract_first()  # 求时间和使用工具（手机或平台）

            tweetsItems["user_id"] = response.meta["ID"]
            tweetsItems["tweet_id"] = str(response.meta["ID"]) + "-" + str(tweet_id)
            if content:
                tweetsItems["content"] = content.strip(u"[\u4f4d\u7f6e]")  # 去掉最后的"[位置]"
            if cooridinates:
                cooridinates = re.findall('center=([\d|.|,]+)', cooridinates)
                if cooridinates:
                    tweetsItems["co_oridinates"] = cooridinates[0]
            if like:
                tweetsItems["like"] = int(like[0])
            if transfer:
                tweetsItems["transfer"] = int(transfer[0])
            if comment:
                tweetsItems["comment"] = int(comment[0])
            if others:
                others = others.split(u"\u6765\u81ea")
                tweetsItems["pubtime"] = others[0]
                if len(others) == 2:
                    tweetsItems["tools"] = others[1]
            yield tweetsItems
        url_next = selector.xpath(
            u'body/div[@class="pa" and @id="pagelist"]/form/div/a[text()="\u4e0b\u9875"]/@href').extract()
        if url_next:
            yield Request(url=self.domain + url_next[0], meta={"ID": response.meta["ID"]}, callback=self.parse2)

    def parse3(self, response):
        """ 抓取关注或粉丝 """
        if response.status >= 300:
            logging.error(response.status)
            logging.info(response.url)
            logging.info(response.body)
            if response.status == 302:
                self.error_found = True
                raise CloseSpider("访问异常: " + str(response.status))
            
        items = response.meta.get("item")
        selector = Selector(response)
        text2 = selector.xpath(
            u'body//table/tr/td/a[text()="\u5173\u6ce8\u4ed6" or text()="\u5173\u6ce8\u5979"]/@href').extract()
            
        headers = {}
        headers['referer'] = response.url

        for elem in text2:
            elem = re.findall('uid=(\d+)', elem)
            if elem:
                response.meta["result"].append(elem[0])
                ID = int(elem[0])

                follows = []
                followsItems = FollowsItem()
                followsItems["user_id"] = ID
                followsItems["follows"] = follows
                fans = []
                fansItems = FansItem()
                fansItems["user_id"] = ID
                fansItems["fans"] = fans
                url_follows = "http://weibo.cn/%s/follow" % ID
                url_fans = "http://weibo.cn/%s/fans" % ID
                url_tweets = "http://weibo.cn/%s/profile?filter=1&page=1" % ID
                url_information0 = "http://weibo.cn/attgroup/opening?uid=%s" % ID
                yield Request(url=url_follows, headers=headers, meta={"item": followsItems, "result": follows},
                              callback=self.parse3)  # 去爬关注人
                yield Request(url=url_fans, headers=headers, meta={"item": fansItems, "result": fans}, callback=self.parse3)  # 去爬粉丝
                yield Request(url=url_information0, headers=headers, meta={"ID": ID}, callback=self.parse0)  # 去爬个人信息
                yield Request(url=url_tweets, headers=headers, meta={"ID": ID}, callback=self.parse2)  # 去爬微博
        url_next = selector.xpath(
            u'body//div[@class="pa" and @id="pagelist"]/form/div/a[text()="\u4e0b\u9875"]/@href').extract()
        if url_next:
            yield Request(url=self.domain + url_next[0], headers=headers, meta={"item": items, "result": response.meta["result"]},
                          callback=self.parse3)
        else:  # 如果没有下一页即获取完毕
            yield items

    def collect_stats(self, status):
        
        response_count_3xx = None
        response_count_4xx = None
        response_count_5xx = None

        job_stats = {}
        job_stats['host'] = self.hostname
#         job_stats['user_id'] = self.user_id
#         job_stats['cell_id'] = self.cell_id
        job_stats['cell_name'] = self.name
        job_stats['item_count'] = self.dstats.get('item_scraped_count', 0)
        if self.dstats.get('start_time', datetime.datetime.now()):   # when job starts in waiting queue
            job_stats['run_time'] = pytz.utc.localize(self.dstats.get('start_time')) # when job starts running
        if self.dstats.get('finish_time'):
            job_stats['end_time'] = pytz.utc.localize(self.dstats.get('finish_time'))
        if status == 'running':
            job_stats['status'] = 'running'
        elif status == 'finished':
            job_stats['status'] = self.dstats.get('finish_reason', None)
            if self.error_found or job_stats['item_count'] == 0:
                job_stats['status'] = 'failed'
            
        job_stats['image_count'] = self.dstats.get('image_count')
        job_stats['image_downloaded'] = self.dstats.get('image_downloaded')
        job_stats['request_count'] = self.dstats.get('downloader/request_count')
        job_stats['response_bytes'] = self.dstats.get('downloader/response_bytes')
        job_stats['response_count'] = self.dstats.get('downloader/response_count')
        job_stats['response_count_200'] = self.dstats.get('downloader/response_status_count/200')
        for key, value in self.dstats.iteritems():
            if 'downloader/response_status_count/3' in key:
                job_stats['response_count_3xx'] = int(response_count_3xx or 0) + value
            elif 'downloader/response_status_count/4' in key:
                job_stats['response_count_4xx'] = int(response_count_4xx or 0) + value
            elif 'downloader/response_status_count/5' in key:
                job_stats['response_count_5xx'] = int(response_count_5xx or 0) + value
        job_stats['load_time'] = datetime.datetime.now()
        
        try:
            with self.meta_conn:
                if not self.job_id:
                    self.job_id = uuid.uuid1().hex
                    job_stats['job_id'] = self.job_id
                    job_stats['start_time'] = self.jobtime
                    self.meta_session.Insert(settings['STATS_TABLE'], job_stats)
                else:
                    wheredict = {}
                    wheredict['job_id'] = self.job_id
                    self.meta_session.Update(settings['STATS_TABLE'], job_stats, wheredict)
                    
        except psycopg2.Error, e:
            logging.warn('Failed to refresh job stats: %s' % e)


    def getCookies(self):
        """ 获取Cookies """
        loginURL = r'https://login.sina.com.cn/sso/login.php?client=ssologin.js(v1.4.15)'
        account = self.account
        password = self.password
        username = base64.b64encode(account.encode('utf-8')).decode('utf-8')
        postData = {
            "entry": "sso",
            "gateway": "1",
            "from": "null",
            "savestate": "30",
            "useticket": "0",
            "pagerefer": "",
            "vsnf": "1",
            "su": username,
            "service": "sso",
            "sp": password,
            "sr": "1440*900",
            "encoding": "UTF-8",
            "cdult": "3",
            "domain": "sina.com.cn",
            "prelt": "0",
            "returntype": "TEXT",
        }
        session = requests.Session()
        
        r = session.post(loginURL, data=postData)
        jsonStr = r.content.decode('gbk')
        info = json.loads(jsonStr)
        if info["retcode"] == "0":
            cookie = session.cookies.get_dict()
            print "Get Cookie Success!( Account:%s )" % account
            logging.info("Get Cookie Success!( Account:%s )" % account)
        else:
            print 'Login failed account: ' + account
            print info['reason']
            logging.info('Login failed account: ' + account)
            logging.info(info['reason'])
    
        return cookie

