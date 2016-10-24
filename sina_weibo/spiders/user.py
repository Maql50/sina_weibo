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
	name = "weibo_tweets_new"
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
	cookies = {}
	handle_httpstatus_list = [302]

	u_url = 'http://weibo.cn/'
	c_url = 'http://weibo.cn/comment/'
	

	ua = 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0'

	def __init__(self, para, *args, **kwargs):
		
		super(Spider, self).__init__(self.name, *args, **kwargs)
		dispatcher.connect(self.spider_opened, signals.spider_opened)
		dispatcher.connect(self.spider_closed, signals.spider_closed)

		self.flag = para
		self.cell_name = self.name
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

		self.account = resource[0]
		self.password =resource[1]
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
	
	#如果接收到的命令行参数为m，则认定为主爬虫，从数据库中读入用户ID，生成请求并写入到redis中
	def start_requests(self):
		
		if self.flag == 'm':
			with self.data_conn.cursor() as cur:
				cur.execute('select uid from dc36.weibo_user_simple')
				for uid in cur:
					self.time_record_dynamic = datetime.datetime.now()
					if (self.time_record_dynamic - self.time_record_static).seconds > 43200:
						self.cookies = getCookies(self.account, self.password)
						self.time_record_static = datetime.datetime.now()
						self.time_record_dynamic = datetime.datetime.now()
						print 'relogging!!!'
					yield Request(self.u_url+uid[0], meta={'uid':uid[0],'nickname':''}, callback=self.parse)		

	#解析微博页面
	def parse(self, r):
		nickname = '' #html>body>div>table>tbody>tr>td>div>span
		try:
			nickname = r.selector.css('td>div>span.ctt::text').extract()[0]
			if nickname.strip()=='':
				nickname = r.meta['nickname']
			else:
				r.meta['nickname'] = nickname
		except:
			nickname = r.meta['nickname']
		#if r.css('.ut::text'):
		#	nickname = r.css('.ut::text').extract()[0].split(' ')[0]

		vflag = False
		if 'vflag' in r.meta:
			if r.meta['vflag'] == 'T':
				vflag = True
		elif r.css('img[alt="V"]'):
			vflag = True
			r.meta['vflag'] = 'T'
		else:
			r.meta['vflag'] = 'F'

		for t in r.css('.c'):
			if t.css('.ctt'):
				pass
			else:
				continue
			ti = TweetsItem()
			ti['user_id'] = r.meta['uid']

			try:
				ti['tweet_id'] = ti['user_id'] + '-' + t.css('.c::attr(id)')[0].extract()

			except:
				ti['tweet_id'] = ''

			if t.css('.ctt'):
				ti['content'] = t.css('.ctt::text').extract()[0].strip(' \r\t\n')
			if t.css('.ct'):
				ti['pubtime'] = time_tran(t.css('.ct::text').extract()[0].split(u'\u6765\u81ea')[0])
				ti['tools'] = t.css('.ct::text').extract()[0].split(u'\u6765\u81ea')[1].strip(' \r\t\n')
			ti['like'] = 0
			if t.css('.ctt > a[href*="attitude"]'):
				ti['like'] = int(t.css('.ctt > a[href*="attitude"]::text').extract()[0].split('[')[1][:-1])
			ti['comment'] = 0
			if t.css('a.cc'):
				ti['comment'] = int(t.css('a.cc::text').extract()[0].split('[')[1][:-1])
				'''
				if ti['comment'] > 0:
					yield Request(t.css('a.cc::attr(href)').extract()[0], meta={'tid':ti['tweet_id']}, callback=self.cp)
				'''
			ti['transfer'] = 0
			if t.css('.ctt > a[href*="repost"]'):
				ti['transfer'] = int(t.css('.ctt > a[href*="repost"]::text').extract()[0].split('[')[1][:-1])
			ti['vflag'] = vflag
			ti['nickname'] = nickname

			ti['tag'] = 'uid'
			yield ti

		np = r.css('#pagelist > form:nth-child(1) > div:nth-child(1) > a:nth-child(1)::text')
		#下页
		if np and u'\u4e0b\u9875' == np[0].extract():

			yield Request(r.urljoin(r.css('#pagelist > form:nth-child(1) > div:nth-child(1) > a:nth-child(1)::attr(href)')[0].extract()), callback=self.parse, meta=r.meta)

		return

	#解析评论页面
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
			ci['tag'] = 'uid'
			yield ci
		np = s.select('#pagelist > form:nth-of-type(1) > div:nth-of-type(1) > a:nth-of-type(1)')
		if np and np[0].text.strip(' \r\t\n') == u'\u4e0b\u9875':
			yield Request(r.urljoin(np[0]['href']), meta={'tid':r.meta['tid']}, callback=self.cp)
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
