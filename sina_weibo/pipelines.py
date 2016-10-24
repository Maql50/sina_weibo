# encoding=utf-8
# import pymongo
from .items import InformationItem, TweetsItem, FollowsItem, FansItem, CommentItem, UserItem
import logging
# from PyDispatcher import dispatcher
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals

class PostgresPipleline(object):

	def __init__(self):
		dispatcher.connect(self.spider_opened, signals.spider_opened)

	def spider_opened(self, spider):
		self.conn = spider.data_conn
		self.db = spider.data_session

	def process_item(self, item, spider):
		""" 判断item的类型，并作相应的处理，再入数据库 """
		if isinstance(item, InformationItem):
			#table_name = 'dc36.weibo_user_new'
			table_name = 'dc36.weibo_user'
			try:
				with self.conn:
					self.db.Insert(table_name, item)
			except Exception as e:
				logging.error('Failed to insert item: %s' % e)
		elif isinstance(item, TweetsItem):
			table_name = 'dc36.weibo_tweets_new'
			try:
				with self.conn:
					self.db.Insert(table_name, item)
			except Exception as e:
				logging.error('Failed to insert item: %s' % e)
		elif isinstance(item, FollowsItem):
			table_name = 'dc36.weibo_follows'
			try:
				with self.conn:
					self.db.Insert(table_name, item)
			except Exception as e:
				logging.error('Failed to insert item: %s' % e)
		elif isinstance(item, FansItem):
			table_name = 'dc36.weibo_fans_new'
			try:
				with self.conn:
					self.db.Insert(table_name, item)
			except Exception as e:
				logging.error('Failed to insert item: %s' % e)
		elif isinstance(item, CommentItem):
			table_name = 'dc36.weibo_comments'
			try:
				with self.conn:
					self.db.Insert(table_name, item)
			except Exception as e:
				logging.error('Failed to insert item: %s' % e)
		elif isinstance(item, UserItem):
			table_name = 'dc36.weibo_user_simple'
			try:
				with self.conn:
					self.db.Insert(table_name, item)
			except Exception as e:
				logging.error('Failed to insert item: %s' % e)
		return item


# class MongoDBPipleline(object):
#	  def __init__(self):
#		  clinet = pymongo.MongoClient("localhost", 27017)
#		  db = clinet["Sina"]
#		  self.Information = db["Information"]
#		  self.Tweets = db["Tweets"]
#		  self.Follows = db["Follows"]
#		  self.Fans = db["Fans"]
# 
#	  def process_item(self, item, spider):
#		  """ 判断item的类型，并作相应的处理，再入数据库 """
#		  if isinstance(item, InformationItem):
#			  try:
#				  self.Information.insert(dict(item))
#			  except Exception:
#				  pass
#		  elif isinstance(item, TweetsItem):
#			  try:
#				  self.Tweets.insert(dict(item))
#			  except Exception:
#				  pass
#		  elif isinstance(item, FollowsItem):
#			  followsItems = dict(item)
#			  follows = followsItems.pop("follows")
#			  for i in range(len(follows)):
#				  followsItems[str(i + 1)] = follows[i]
#			  try:
#				  self.Follows.insert(followsItems)
#			  except Exception:
#				  pass
#		  elif isinstance(item, FansItem):
#			  fansItems = dict(item)
#			  fans = fansItems.pop("fans")
#			  for i in range(len(fans)):
#				  fansItems[str(i + 1)] = fans[i]
#			  try:
#				  self.Fans.insert(fansItems)
#			  except Exception:
#				  pass
#		  return item
