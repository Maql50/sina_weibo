# encoding=utf-8
import logging
import json
import time
import psycopg2
import re
import pytz
import uuid
import socket
from sina_weibo.tagID import tagID
from sina_weibo.handlers import PGHandler
from sina_weibo.utils import getCookies,time_tran
from sina_weibo.items import InformationItem, TweetsItem, FollowsItem, FansItem,CommentItem
from sina_weibo.spiders import spiders
from scrapy import signals
from scrapy.conf import settings
from scrapy.exceptions import CloseSpider
from scrapy.selector import Selector
from scrapy.spiders import Spider
from scrapy.http import Request
from scrapy.xlib.pydispatch import dispatcher
from bs4 import BeautifulSoup
from sina_weibo.postgres import db
from datetime import datetime

class Spider(Spider):
	name = 'sina_weibo_tag'
	domain = "http://weibo.com"
	target = 'weibo'
	hostname = socket.gethostname()
	jobtime = datetime.now()
	cell_id = None
	job_id = None
	user_id = None
	error_found = False
	cookies = {}
	handle_httpstatus_list = [302,404,403,303]
	account = 'u80676137zhic@163.com'
	password = '8m4opw'

	pp,p,cp,pb = 0,1,0,0
	ps = {}

	start_urls = [
		r'http://d.weibo.com/p/aj/v6/mblog/mbloglist?'
	]

	ua = r'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0'

	cc_url = r'http://weibo.com/aj/v6/comment/big?'

	def __init__(self, *args, **kwargs):
		super(Spider, self).__init__(self.name, *args, **kwargs)
		dispatcher.connect(self.spider_opened, signals.spider_opened)
		dispatcher.connect(self.spider_closed, signals.spider_closed)

		meta_conn_string = ' '.join(["host=", settings['META_DB']['host'], "dbname=", settings['META_DB']['dbname'], "user=", settings['META_DB']['user'], "password=", settings['META_DB']['password'], "port=", settings['META_DB']['port']])
		self.meta_conn = psycopg2.connect(meta_conn_string)
		self.meta_conn.autocommit = True
		self.meta_session = db(self.meta_conn)
		data_conn_string = ' '.join(["host=", settings['DATA_DB']['host'], "dbname=", settings['DATA_DB']['dbname'], "user=", settings['DATA_DB']['user'], "password=", settings['DATA_DB']['password'], "port=", settings['DATA_DB']['port']])
		self.data_conn = psycopg2.connect(data_conn_string)
		self.data_session = db(self.data_conn)
		self.cookies = getCookies(self.account, self.password)
		self.tg = self.tag_gen()
		tag = self.tg.next()
		self.ps['ajwvr'] = '6'
		self.ps['domain'] = tag
		self.ps['tab'] = 'home'
		self.ps['pl_name'] = 'Pl_Core_NewMixFeed__5'
		self.ps['id'] = tag
		self.ps['script_uri'] = '/'+tag
		self.ps['feed_type'] = '1'
		self.ps['domain_op'] = tag
		self.ps['current_page'] = str(self.cp)
		self.ps['pre_page'] = str(self.pp)
		self.ps['page'] = str(self.p)
		self.ps['pagebar'] = str(self.pb)
		self.dstats = {}
		self.error_found = False

	def tag_gen(self):
		for tag in tagID:
			self.tag_name = tag
			yield tagID[tag] 

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

	def make_requests_from_url(self,url):
		self.ps['__rnd'] = str(int(1000*time.time()))
		return Request(self.url_gen(url), callback=self.parse, dont_filter=True)

	def url_gen(self,url):
		for k in self.ps:
			url += k+'='+self.ps[k]+'&'
		return url[:-1]

	#解析微博博文
	def parse(self,response):
		s = BeautifulSoup(json.loads(response.body)['data'],'html.parser')
		
		if s.select('.WB_feed_type'):
			pass
		else:
			print 'tag change'
			tag = self.tg.next()
			if tag:
				self.pp,self.p,self.cp,self.pb = 0,1,0,0
				self.ps['current_page'] = str(self.cp)
				self.ps['pre_page'] = str(self.pp)
				self.ps['page'] = str(self.p)
				self.ps['pre_page'] = str(self.pp)
				self.ps['domain'] = tag
				self.ps['domain_op'] = tag
				self.ps['id'] = tag	
				self.ps['script_uri'] = '/'+tag
				self.ps['__rnd'] = str(int(1000*time.time()))
				yield Request(self.url_gen(self.start_urls[0]), callback=self.parse, dont_filter=True)
			return 
		
		for t in s.select('.WB_feed_type'):
			tweetsItems = TweetsItem()
			if t.select('.WB_text'):
				tweetsItems['content']=t.select('.WB_text')[0].text.strip(' \n\r\t')
			tweetsItems['user_id']=t['tbinfo'].split('=')[1]
			tweetsItems['tweet_id']=t['mid']
			if t.select('ul.WB_row_line > li:nth-of-type(4) > a:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > em:nth-of-type(2)'):
				if u'\u8d5e' == t.select('ul.WB_row_line > li:nth-of-type(4) > a:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > em:nth-of-type(2)')[0].text:	#赞
					tweetsItems['like'] = 0
				else:
					tweetsItems['like']=int(t.select('ul.WB_row_line > li:nth-of-type(4) > a:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > em:nth-of-type(2)')[0].text)
			if t.select('ul.WB_row_line > li:nth-of-type(2) > a:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > em:nth-of-type(2)'):
				if u'\u8f6c\u53d1' == t.select('ul.WB_row_line > li:nth-of-type(2) > a:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > em:nth-of-type(2)')[0].text:	#转发
					tweetsItems['transfer']=0
				else:
					tweetsItems['transfer']=int(t.select('ul.WB_row_line > li:nth-of-type(2) > a:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > em:nth-of-type(2)')[0].text)
			if t.select('ul.WB_row_line > li:nth-of-type(3) > a:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > em:nth-of-type(2)'):
				if u'\u8bc4\u8bba' == t.select('ul.WB_row_line > li:nth-of-type(3) > a:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > em:nth-of-type(2)')[0].text:	#评论
					tweetsItems['comment'] = 0
				else:
					tweetsItems['comment']=int(t.select('ul.WB_row_line > li:nth-of-type(3) > a:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > em:nth-of-type(2)')[0].text)
					yield Request(self.cc_url+'ajwvr=6&'+'id='+tweetsItems['tweet_id']+'&__rnd='+str(int(1000*time.time())), meta={'page':'1','tid':tweetsItems['tweet_id']}, callback=self.ccparse, dont_filter=True)
			if t.select('.WB_from > a:nth-of-type(1)'):
				tweetsItems['pubtime']=t.select('.WB_from > a:nth-of-type(1)')[0]['title']
			if t.select('.WB_from > a:nth-of-type(2)'):
				tweetsItems['tools']=t.select('.WB_from > a:nth-of-type(2)')[0].text
			if t.select('a.W_fb'):
				tweetsItems['nickname']=t.select('a.W_fb')[0]['nick-name']
			tweetsItems['vflag']=False
			if t.select('i.icon_approve_co'):
				tweetsItems['vflag']=True
			tweetsItems['tag'] = self.tag_name
			yield tweetsItems

		self.cp += 1
		if self.cp%3 == 1:
			self.pp += 1
			self.pb = 0
		if self.cp%3 == 0:
			self.p += 1
		if self.cp%3 == 2:
			self.pb = 1
		self.ps['current_page'] = str(self.cp)
		self.ps['pre_page'] = str(self.pp)
		self.ps['page'] = str(self.p)
		self.ps['pagebar'] = str(self.pb)
		self.ps['__rnd'] = str(int(1000*time.time()))
		yield Request(self.url_gen(self.start_urls[0]), callback=self.parse, dont_filter=True)
		return

	#解析微博评论
	def ccparse(self,r):
		s = BeautifulSoup(json.loads(r.body)['data']['html'],'html.parser')
		if s.select('.icon_warnB'):
			return
		if s.select('.list_li'):
			pass
		else:
			return
		for li in s.select('.list_li'):
			ccItems = CommentItem()
			ccItems['tweet_id'] = r.meta['tid']
			ccItems['comment_id'] = li['comment_id']
			if li.select('div.WB_text > a:nth-of-type(1)'):
				ccItems['user_id'] = li.select('div.WB_text > a:nth-of-type(1)')[0]['usercard'].split('=')[1]
				ccItems['nickname'] = li.select('div.WB_text > a:nth-of-type(1)')[0].text
			if li.select('div.WB_text'):
				content = li.select('div.WB_text')[0].text
				ccItems['content'] = content[1+content.find(u'\uff1a'):].strip(' \n\r\t')	#"："前的内容是评论者昵称，从内容中去掉
			if li.select('div.WB_from'):
				ccItems['pubtime'] = time_tran(li.select('div.WB_from')[0].text)
			ccItems['like'] = 0
			if li.select('span[nodetype="like_status"] > em'):
				if li.select('span[nodetype="like_status"] > em')[0].text:
					ccItems['like'] = int(li.select('span[nodetype="like_status"] > em')[0].text)
			ccItems['vflag'] = False
			if li.select('i.icon_approve'):
				ccItems['vflag'] = True
			ccItems['tag'] = self.tag_name
			yield ccItems

		page = 1 + int(r.meta['page'])
		self.cell_name = self.name
		url = self.cc_url+'ajwvr=6&'+'id='+r.meta['tid']+'&__rnd='+str(int(1000*time.time()))+'&page='+str(page)
		print url
		yield Request(url = url, meta={'page':str(page),'tid':r.meta['tid']}, callback=self.ccparse, dont_filter=True)
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
		if self.dstats.get('start_time', datetime.now()):	 # when job starts in waiting queue
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
		job_stats['load_time'] = datetime.now()

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
