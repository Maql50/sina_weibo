import time

from scrapy.dupefilters import BaseDupeFilter
from scrapy.utils.request import request_fingerprint
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals

from . import connection

DUPEFILTER_KEY = 'dupefilter:%(spider)s'

class RFPDupeFilter(BaseDupeFilter):
    """Redis-based request duplication filter"""

    def __init__(self, server, key):
        """Initialize duplication filter

        Parameters
        ----------
        server : Redis instance
        key : str
            Where to store fingerprints
        """
        dispatcher.connect(self.spider_opened, signals.spider_opened)

        self.server = server
        self.key = key

    def spider_opened(self, spider):
        if not self.key:
            if spider.cell_id:
                self.key = DUPEFILTER_KEY % {'spider': spider.cell_id}
            else:
                self.key = DUPEFILTER_KEY % {'spider': spider.name}
    
    @classmethod
    def from_settings(cls, settings):
        server = connection.from_settings(settings)
        # create one-time key. needed to support to use this
        # class as standalone dupefilter with scrapy's default scheduler
        # if scrapy passes spider on open() method this wouldn't be needed
        key = ''
        return cls(server, key)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings)

    def request_seen(self, request):
        fp = request_fingerprint(request)
        added = self.server.sadd(self.key, fp)
        return not added

    def close(self, reason):
        """Delete data on close. Called by scrapy's scheduler"""
        self.clear()

    def clear(self):
        """Clears fingerprints data"""
        self.server.delete(self.key)
