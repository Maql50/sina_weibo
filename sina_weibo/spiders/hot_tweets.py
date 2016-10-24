# encoding=utf-8
import re
import datetime
import socket
import logging
import pytz
import uuid
import psycopg2
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.spiders import Spider
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from scrapy.conf import settings
from scrapy.exceptions import CloseSpider
from sina_weibo.utils import getCookies,time_tran
from sina_weibo.items import TweetsItem
from sina_weibo.handlers import PGHandler
from sina_weibo.postgres import db
from parse_user_detil import get_user_detail, get_uid_by_uname
class HotTweetsSpider(Spider):
    '''爬取热门微博'''
    name = "weibo_hot_tweets"
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

    start_urls = [
        'http://weibo.cn/pub/topmblog'
    ]

    account = ''
    password = ''

    def __init__(self, *args, **kwargs):
        super(Spider, self).__init__(self.name, *args, **kwargs)
        dispatcher.connect(self.spider_opened, signals.spider_opened)
        dispatcher.connect(self.spider_closed, signals.spider_closed)

        meta_conn_string = ' '.join(["host=", settings['META_DB']['host'], "dbname=", settings['META_DB']['dbname'], \
                                     "user=", settings['META_DB']['user'], "password=", settings['META_DB']['password'],
                                     "port=", settings['META_DB']['port']])
        self.meta_conn = psycopg2.connect(meta_conn_string)
        self.meta_conn.autocommit = True
        self.meta_session = db(self.meta_conn)
        data_conn_string = ' '.join(["host=", settings['DATA_DB']['host'], "dbname=", settings['DATA_DB']['dbname'], \
                                     "user=", settings['DATA_DB']['user'], "password=", settings['DATA_DB']['password'],
                                     "port=", settings['DATA_DB']['port']])
        self.data_conn = psycopg2.connect(data_conn_string)
        self.data_session = db(self.data_conn)
        resource = self.meta_session.getOne(
            "select account, password, ua_mobile, download_delay from crawler_resource where host = '%s' and target = '%s'" % (
            self.hostname, self.target))


        self.account = resource[0]
        self.password = resource[1]
        self.cell_name = self.name
        self.cookies = getCookies(self.account, self.password)
        self.cell_name = self.name

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


    def parse(self, response):
        '''爬取微博内容'''
        for t in response.css('.c'):
            if not t.css('.ctt'):
                continue

            ti = TweetsItem()
            # 获取用户id
            if t.css('.nk'):
                ti['user_id'] = t.css('.nk::attr(href)').extract()[0].split('/')[-1]
                if not ti['user_id'].isdigit():
                    ti['user_id'] = get_uid_by_uname(ti['user_id'], self.cookies)
                    if not ti['user_id']:
                        continue
                ti['nickname'] = t.css('.nk::text').extract()[0]
                print ti['user_id']
            try:
                ti['tweet_id'] = ti['user_id'] + '-' + t.css('.c::attr(id)')[0].extract()
            except:
                ti['tweet_id'] = ''

            # 去爬个人信息
            yield get_user_detail(uid=ti['user_id'], cookies=self.cookies)

            if t.css('.ct'):
                ti['pubtime'] = time_tran(t.css('.ct::text').extract()[0].split(u'\u6765\u81ea')[0])
                if len(t.css('.ct::text').extract()[0].split(u'来自')) > 1:
                    ti['tools'] = t.css('.ct::text').extract()[0].split(u'\u6765\u81ea')[1].strip(' \r\t\n')

            ti['like'] = 0
            if t.css('.ctt > a[href*="attitude"]'):
                ti['like'] = int(t.css('.ctt > a[href*="attitude"]::text').extract()[0].split('[')[1][:-1])
            ti['comment'] = 0
            if t.css('a.cc'):
                ti['comment'] = int(t.css('a.cc::text').extract()[0].split('[')[1][:-1])

            ti['transfer'] = 0
            ti['content'] = t.xpath('div/span[1]')[0].xpath("string(.)").extract()[0][1:]
            if t.css('.ctt > a[href*="repost"]'):
                ti['transfer'] = int(t.css('.ctt > a[href*="repost"]::text').extract()[0].split('[')[1][:-1])
            ti['vflag'] = ''
            ti['tag'] = 'uid'

            if t.css('img[alt="V"]'):
                ti['vflag'] = 'T'
            else:
                ti['vflag'] = 'F'
            # 获取微文信息
            yield ti

        #下页
        np = response.css('#pagelist > form:nth-child(1) > div:nth-child(1) > a:nth-child(1)::text')
        if np and u'下页' == np[0].extract():
            url =  response.urljoin(response.css('#pagelist > form:nth-child(1) > div:nth-child(1) > a:nth-child(1)::attr(href)')[0].extract())
            print url
            yield Request(url=url,callback=self.parse)


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
        if self.dstats.get('start_time', datetime.datetime.now()):  # when job starts in waiting queue
            job_stats['run_time'] = pytz.utc.localize(self.dstats.get('start_time'))  # when job starts running
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


