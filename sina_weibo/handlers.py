import logging
import datetime
from scrapy.conf import settings
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from sina_weibo.postgres import db
# import psycopg2

class PGHandler(logging.Handler):

    def __init__(self,cell_name):

        logging.Handler.__init__(self)
        dispatcher.connect(self.spider_opened, signals.spider_opened)

        self.conn = None
        self.log_table = settings['LOG_TABLE']
        self.cell_name = cell_name
#         self.host = settings['META_DB']['host']
#         self.database = settings['META_DB']['dbname']
#         self.user = settings['META_DB']['user']
#         self.password = settings['META_DB']['password']
#         self.port = settings['META_DB']['port']
		

    def spider_opened(self, spider):
        self.job_id = spider.job_id
        self.user_id = spider.user_id
        self.hostname = spider.hostname
        self.conn = spider.meta_conn
        
#     def get_conn(self):
# 
#         if not self.conn:
#             # if spider's connection is passed here, make_conn is not necessary
#             self.make_conn()
# 
#         return self.conn
# 
#     def make_conn(self):
# 
#         self.conn = psycopg2.connect(
#             database=self.database,
#             host=self.host,
#             user=self.user,
#             password=self.password,
#             port=self.port)
# 
#         self.conn.autocommit = True
#          
#         logging.info("Connected to database for logging with autocommit on: {0}.".format(self.conn))

    def emit(self, record):

#         self.format(record)
#         if record.exc_info:
#             record.exc_text = dcus._defaultFormatter.formatException(record.exc_info)
#         else:
#             record.exc_text = ""
# 
#         if isinstance(record.msg, Exception):
#             record.msg = str(record.msg)
#             
#         conn = self.get_conn()
        conn = self.conn
        mydb = db(conn)
    
        dlog = {}
        dlog['job_id'] = self.job_id
        dlog['user_id'] = self.user_id
        dlog['host'] = self.hostname
        dlog['time'] = datetime.datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')
        dlog['log_level'] = logging.getLevelName(record.levelno)
        dlog['cell_name'] = self.cell_name
        
        if record.getMessage() != '':
            dlog['message'] = record.getMessage()
        else:
            try:
                dlog['message'] = record.msg.event.get('failure')
            except:
                pass
         
        mydb.Insert(settings['LOG_TABLE'], dlog)
        
