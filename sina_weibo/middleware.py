# encoding=utf-8
import base64
from scrapy.conf import settings
from scrapy.exceptions import IgnoreRequest
from scrapy.http import HtmlResponse, Response
class UserAgentMiddleware(object):

    def process_request(self, request, spider):
        request.headers["User-Agent"] = spider.ua
        request.meta['dont_redirect'] = True


class CookiesMiddleware(object):

    def process_request(self, request, spider):
        request.cookies = spider.cookies

class ProxyMiddleware(object):
    
    def process_request(self, request, spider):
        
        protocol = request.url.split(':')[0].lower()
        if protocol == 'https':
            proxy = settings.get('HTTPS_PROXY')
        else:
            proxy = settings.get('HTTP_PROXY')

        proxy_user_pass = settings.get('HTTP_PROXY_USER') + ':' + settings.get('HTTP_PROXY_PASS')
        request.meta['proxy'] = proxy
        if proxy_user_pass:
            encoded_user_pass = base64.encodestring(proxy_user_pass)
            request.headers['Proxy-Authorization'] = 'Basic ' + encoded_user_pass


