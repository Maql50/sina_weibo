# -*- coding: utf-8 -*-

#import pytz
import socket
import logging
#import datetime
#import psycopg2
from scrapy.conf import settings
from twisted.internet import task
from scrapy.exceptions import NotConfigured
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher

logger = logging.getLogger(__name__)

class LogDCStats(object):
    """Log and load basic scraping stats periodically"""

    def __init__(self, stats, interval=60.0):
        dispatcher.connect(self.spider_opened, signals.spider_opened)
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        self.stats = stats
        self.interval = interval

    @classmethod
    def from_crawler(cls, crawler):
        interval = crawler.settings.getfloat('LOGSTATS_INTERVAL')
        
        if not interval:
            raise NotConfigured
        o = cls(crawler.stats, interval)
#         crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
#         crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
        return o

    def spider_opened(self, spider):
        print 'spider_opened in extensions.py'
        self.pagesprev = 0
        self.itemsprev = 0
        self.task = task.LoopingCall(self.log, spider)
        self.task.start(self.interval)

    def sent2Graphite(self, message,spider):
        
        udp_ip = settings['GRAPHITE_UDP_IP']
        udp_port = settings['GRAPHITE_UDP_PORT']
        sock = socket.socket(socket.AF_INET, # Internet
                       socket.SOCK_DGRAM) # UDP
        sock.sendto(message, (udp_ip, udp_port))
        
    def log(self, spider):
        items = self.stats.get_value('item_scraped_count', 0)
        pages = self.stats.get_value('response_received_count', 0)
        self.pagesprev, self.itemsprev = pages, items
        
        print 'message sent!!'
        udp_message = spider.name + '.' + spider.cell_name + '.' + 'scrapedLines:' + str(items)+'|g'
        self.sent2Graphite(udp_message,spider)


    def spider_closed(self, spider, reason):
        self.log(spider)
        if self.task.running:
            self.task.stop()
