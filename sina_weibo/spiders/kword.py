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
from sina_weibo.items import InformationItem, TweetsItem, FollowsItem, FansItem, CommentItem
from sina_weibo.weiboID import weiboID
from sina_weibo.handlers import PGHandler
from sina_weibo.utils import getCookies,time_tran
from sina_weibo.redis.spiders import RedisSpider
from sina_weibo.postgres import db
from scrapy.spiders import Spider
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from scrapy.conf import settings
from scrapy.exceptions import CloseSpider
from bs4 import BeautifulSoup

def url_generator_by_file(urlfile):
	with open (urlfile) as f:
		urls = f.readlines()
	for url in urls:
		yield url.rstrip()

#接收名为para的命令行参数，如果为‘m’，表示是主爬虫，会向Redis中写入Request。如果留空，表示是从爬虫，只从Redis中读取并处理Request
class Spider(RedisSpider):
	name = "sina_weibo_kword"
	domain = "http://weibo.cn"
	target = 'weibo'
	hostname = socket.gethostname()
	jobtime = datetime.datetime.now()
	cell_id = None
	job_id = None
	user_id = None
	error_found = False
	cookies = {}
	handle_httpstatus_list = [302]

	s_url = 'http://weibo.cn/search/mblog?keyword='
	c_url = 'http://weibo.cn/comment/'
	
	account = 'u8240328daoy@163.com'
	password = '5wrib52'

	ua = 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0'

	start_urls = [
	]

	kword = u'\u5c0f\u76ee\u6807'

	def __init__(self, para, *args, **kwargs):
		super(Spider, self).__init__(self.name, *args, **kwargs)
		dispatcher.connect(self.spider_opened, signals.spider_opened)
		dispatcher.connect(self.spider_closed, signals.spider_closed)

		self.flag = para

		meta_conn_string = ' '.join(["host=", settings['META_DB']['host'], "dbname=", settings['META_DB']['dbname'], \
			"user=", settings['META_DB']['user'], "password=", settings['META_DB']['password'], "port=", settings['META_DB']['port']])
		self.meta_conn = psycopg2.connect(meta_conn_string)
		self.meta_conn.autocommit = True
		self.meta_session = db(self.meta_conn)
		data_conn_string = ' '.join(["host=", settings['DATA_DB']['host'], "dbname=", settings['DATA_DB']['dbname'], \
			"user=", settings['DATA_DB']['user'], "password=", settings['DATA_DB']['password'], "port=", settings['DATA_DB']['port']])
		self.data_conn = psycopg2.connect(data_conn_string)
		self.data_session = db(self.data_conn)
		self.cookies = getCookies(self.account, self.password)
		self.dstats = {}
		self.error_found = False

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

	@classmethod
	def from_crawler(cls, crawler, *args, **kwargs):
		spider = cls(*args, **kwargs)

		# change settings in the runtime
		# have to hack crawler.py to disable settings freeze (comment out self.settings.freeze())
		# TODO: try to start spider in a different approach so as to get parameter earlier than crawler is initialized and change settings accordingly
		
		spider._set_crawler(crawler)

		return spider

	def make_requests_from_url(self,url):
		return Request(url, callback=self.parse)	

	def start_requests(self):
		if self.flag == 'm':
			yield Request(self.s_url+self.kword, meta={'kword':self.kword}, callback=self.parse)		

	#抓取微博内容
	def parse(self, r):
		s = BeautifulSoup(r.body,'html.parser')
		for v in s.select('div.c'):
			if not v.select('.ctt'):
				continue
			tweetsItem = TweetsItem()
			if v.select('a.cc'):
				if len(v.select('a.cc')[0]['href'].split('uid=')) > 1:
					tweetsItem['user_id'] = int(v.select('a.cc')[0]['href'].split('uid=')[1][0:10])
				tweetsItem['comment'] = int(v.select('a.cc')[0].text.split('[')[1][:-1])
				if 0 != tweetsItem['comment']:
					meta = r.meta
					meta['tid'] = v['id']
					yield Request(v.select('.cc')[0]['href'], meta=meta, callback=self.cp)
			else:
				tweetsItem['comment'] = 0
			tweetsItem['tweet_id'] = v['id']
			if v.select('span.ct'):
				tweetsItem['pubtime'] = time_tran(v.select('span.ct')[0].text.split(u'\u6765\u81ea')[0])
			if v.select('.ctt'):
				tweetsItem['content'] = v.select('.ctt')[0].text[1:]
			if len(v.select('span.ct')[0].text.split(u'\u6765\u81ea')) > 1:
				tweetsItem['tools'] = v.select('span.ct')[0].text.split(u'\u6765\u81ea')[1].strip(' \r\t\n')
			if v.select('a[href*="attitude"]'):
				tweetsItem['like'] = int(v.select('a[href*="attitude"]')[0].text.split('[')[1][:-1])
			else:
				tweetsItem['like'] = 0
			if v.select('a[href*="repost"]'):
				if len(v.select('a[href*="repost"]')[0]['href'].split('uid=')) > 1:
					tweetsItem['user_id'] = v.select('a[href*="repost"]')[0]['href'].split('uid=')[1][0:10]
				tweetsItem['transfer'] = int(v.select('a[href*="repost"]')[0].text.split('[')[1][:-1])
			tweetsItem['vflag'] = False
			if v.select('img[alt="V"]'):
				tweetsItem['vflag'] = True
			if v.select('.nk'):
				tweetsItem['nickname'] = v.select('.nk')[0].text
			tweetsItem['tag'] = 'keyword'+':'+r.meta['kword']
			yield tweetsItem
		np = s.select('#pagelist > form:nth-of-type(1) > div:nth-of-type(1) > a:nth-of-type(1)')
		if np:
			if np[0].text == u'\u4e0b\u9875':
				yield Request('http://weibo.cn'+np[0]['href'], meta=r.meta, callback=self.parse)
			else:
				return
		else:
			return

	#抓取评论内容
	def cp(self,r):
		s = BeautifulSoup(r.body, 'html.parser')
		for c in s.select('.c'):
			if not c.select('.ctt'):
				continue
			ci = CommentItem()
			if c.select('a[href*="spam"]'):
				ci['user_id'] = int(c.select('a[href*="spam"]')[0]['href'].split('?')[1].split('&')[1].split('=')[1])
			ci['tweet_id'] = r.meta['tid']
			ci['comment_id'] = c['id'].split('_')[1]
			ci['content'] = c.select('.ctt')[0].text
			if c.select('.ct'):
				ci['pubtime'] = time_tran(c.select('.ct')[0].text.split(u'\u6765\u81ea')[0])
			if c.select('a[href*="attitude"]'):
				ci['like'] = int(c.select('a[href*="attitude"]')[0].text.split('[')[1][:-1])
			if c.select('a[href^="/u"]'):
				ci['nickname'] = c.select('a[href^="/u"]')[0].text
			ci['vflag'] = False
			if c.select('img[alt="V"]'):
				ci['vflag'] = True
			ci['tag'] = 'keyword'+':'+r.mata['kword']
			yield ci
		np = s.select('#pagelist > form:nth-of-type(1) > div:nth-of-type(1) > a:nth-of-type(1)')
		if np:
			if np[0].text.strip(' \r\t\n') == u'\u4e0b\u9875':
				yield Request('http://weibo.cn'+np[0]['href'], meta=r.meta, callback=self.cp)
			else:
				return
		else:
			return

	def collect_stats(self, status):
		response_count_3xx = None
		response_count_4xx = None
		response_count_5xx = None

		job_stats = {}
		job_stats['host'] = self.hostname
#		  job_stats['user_id'] = self.user_id
#		  job_stats['cell_id'] = self.cell_id
		job_stats['cell_name'] = self.name
		job_stats['item_count'] = self.dstats.get('item_scraped_count', 0)
		if self.dstats.get('start_time', datetime.datetime.now()):	 # when job starts in waiting queue
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
