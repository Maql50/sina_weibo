# encoding=utf-8
import logging
import json
import time
import re
import psycopg2
import pytz
import uuid
import socket
from sina_weibo.tagID import tagID
from sina_weibo.handlers import PGHandler
from sina_weibo.utils import getCookies
from sina_weibo.items import InformationItem, TweetsItem, FollowsItem, FansItem, TopicItem
from sina_weibo.spiders import spiders
from sina_weibo.postgres import db
from scrapy import signals
from scrapy.conf import settings
from scrapy.exceptions import CloseSpider
from scrapy.selector import Selector
from scrapy.spiders import Spider
from scrapy.http import Request
from scrapy.xlib.pydispatch import dispatcher
from bs4 import BeautifulSoup
from datetime import datetime

class Spider(Spider):
	name = 'sina_weibo_topic'
	domain = "http://weibo.com"
	target = 'weibo'
	hostname = socket.gethostname()
	jobtime = datetime.now()
	cell_id = None
	job_id = None
	user_id = None
	cookies = {}
	handle_httpstatus_list = [302,303,404,403]
	account = 'u80676137zhic@163.com'
	password = '8m4opw'

	pp,p,cp,pb = 0,1,0,0
	ps = {}
	tp = {}
	tpage = 2
	stamp = 0

	start_urls = [
		r'http://d.weibo.com/100803?'
	]
	
	ua = r'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0'
	blog_url = r'http://d.weibo.com/p/aj/v6/mblog/mbloglist?'
	topic_url = r'http://d.weibo.com/100803?'

	tag = ''
	topics = []

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
		
		self.ps['ajwvr'] = '6'
		self.ps['domain'] = '100808'
		self.ps['from'] = 'faxian_huati'
		self.ps['tab'] = 'home'
		self.ps['pl_name'] = 'Pl_Third_App__11'
		self.ps['feed_type'] = '1'
		self.ps['domain_op'] = '100808'
		self.ps['current_page'] = str(self.cp)
		self.ps['pre_page'] = str(self.pp)
		self.ps['page'] = str(self.p)
		self.ps['pagebar'] = str(self.pb)

		self.tp['pids'] = 'Pl_Discover_Pt6Rank__5'
		self.tp['cfs'] = '920'
		self.tp['Pl_Discover_Pt6Rank__5_filter'] = 'hothtlist_type'
		self.tp['ajaxpagelet'] = '1'
		self.tp['__ref'] = '/100803'

		self.error_found = False
		self.dstats = {}
	
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
		self.stamp = int(100000*time.time())
		self.tp['_t'] = 'FM_'+str(self.stamp)
		self.tp['Pl_Discover_Pt6Rank__5_page'] = str(self.tpage)
		return Request(self.url_gen(url,self.tp), callback=self.parse, dont_filter=True)

	def url_gen(self,url,dic):
		for k in dic:
			url += k+'='+dic[k]+'&'
		return url[:-1]

	#首先用正则表达式从js中提取出需要的html内容，然后进行解析得到话题详细信息存入数据表dc36.weibo_topic中
	def parse(self,r):
		m = re.findall(u'{"ns":"pl.content.miniTab.index","domid":"Pl_Discover_Pt6Rank__5".*}',r.body)
		if m:
			s = BeautifulSoup(json.loads(m[0])['html'],'html.parser')
			if s.select('li.pt_li'):
				pass
			else:
				return
			for li in s.select('li.pt_li'):
				topicItem = TopicItem()
				if li.select('a.S_txt1'):
					topicItem['topic_name'] = li.select('a.S_txt1')[0].text.strip(' \r\t\n')
					topicItem['topic_code'] = li.select('a.S_txt1')[0]['href'].split('/')[-1].split('?')[0]
					self.pp,self.p,self.cp,self.pb = 0,1,0,0
					self.ps['pagebar'] = str(self.pb)
					self.ps['current_page'] = str(self.cp)
					self.ps['page'] = str(self.p)
					self.ps['pre_page'] = str(self.pp)
					self.ps['id'] = topicItem['topic_code']
					self.ps['script_uri'] = '/p/' + topicItem['topic_code']
					self.ps['__rnd'] = str(1000*time.time())
					yield Request(self.url_gen(self.blog_url, self.ps), meta=self.ps, callback=self.blparse)
				if li.select('.W_btn_tag'):
					topicItem['tag'] = li.select('.W_btn_tag')[0].text.strip(' \t\r\n')
				if li.select('div.subtitle'):
					topicItem['subtitle'] = li.select('div.subtitle')[0].text.strip(' \t\r\n')
				if li.select('span.number'):
					topicItem['views'] = li.select('span.number')[0].text.strip(' \t\r\n')
				if li.select('a.tlink'):
					topicItem['host_id']  = li.select('a.tlink')[0]['href'].split('/')[-1].split('?')[0]
					topicItem['host_nick'] = li.select('a.tlink')[0].text.strip(' \t\r\n')
				topicItem['last_update'] = str(datetime.now())
				print topicItem
			self.stamp += 4
			self.tp['_t'] = 'FM_'+str(self.stamp)
			self.tpage += 1
			self.tp['Pl_Discover_Pt6Rank__5_page'] = str(self.tpage)
			yield Request(self.url_gen(self.topic_url,self.tp), callback=self.parse)
			return
		else:
			print 're error'
			return

	#分别抓取每个话题下的微博
	def blparse(self,r):
		s = BeautifulSoup(json.loads(r.body)['data'],'html.parser')
		if s.select('.W_loading'):
			pass
		else:
			print 'empty'
			return 
		for t in s.select('.WB_feed_type'):
			tweetsItems = TweetsItem()
			if t.select('.WB_text'):
				tweetsItems['content']=t.select('.WB_text')[0].text
			tweetsItems['user_id']=t['tbinfo'].split('=')[1]
			tweetsItems['tweet_id']=t['mid']
			tweetsItems['like'] = 0
			if t.select('ul.WB_row_line > li:nth-of-type(4) > a:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > em:nth-of-type(2)'):
				if u'\u8d5e' != t.select('ul.WB_row_line > li:nth-of-type(4) > a:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > em:nth-of-type(2)')[0].text:
					tweetsItems['like']=int(t.select('ul.WB_row_line > li:nth-of-type(4) > a:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > em:nth-of-type(2)')[0].text)
			tweetsItems['transfer']=0
			if t.select('ul.WB_row_line > li:nth-of-type(2) > a:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > em:nth-of-type(2)'):
				if u'\u8f6c\u53d1' != t.select('ul.WB_row_line > li:nth-of-type(2) > a:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > em:nth-of-type(2)')[0].text:
					tweetsItems['transfer']=int(t.select('ul.WB_row_line > li:nth-of-type(2) > a:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > em:nth-of-type(2)')[0].text)
			tweetsItems['comment'] = 0
			if t.select('ul.WB_row_line > li:nth-of-type(3) > a:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > em:nth-of-type(2)'):
				if u'\u8bc4\u8bba' != t.select('ul.WB_row_line > li:nth-of-type(3) > a:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > em:nth-of-type(2)')[0].text:
					tweetsItems['comment']=int(t.select('ul.WB_row_line > li:nth-of-type(3) > a:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > span:nth-of-type(1) > em:nth-of-type(2)')[0].text)
			if t.select('.WB_from > a:nth-of-type(1)'):
				tweetsItems['pubtime']=t.select('.WB_from > a:nth-of-type(1)')[0]['title']
			if t.select('.WB_from > a:nth-of-type(2)'):
				tweetsItems['tools']=t.select('.WB_from > a:nth-of-type(2)')[0].text
			print tweetsItems

		cp = int(r.meta['current_page'])+1
		r.meta['current_page'] = str(cp)
		if cp%3 == 1:
			r.meta['pre_page'] = str(int(r.meta['pre_page'])+1)
			r.meta['pagebar'] = str(0)
		if cp%3 == 0:
			r.meta['page'] = str(int(r.meta['page'])+1)
		if cp%3 == 2:
			r.meta['pagebar'] = str(1)
		r.meta['__rnd'] = str(1000*time.time())
		yield Request(self.url_gen(self.blog_url, r.meta), meta=r.meta, callback=self.blparse)

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
