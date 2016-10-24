# encoding=utf-8
import re
import datetime
from scrapy.selector import Selector
from scrapy.http import Request
from sina_weibo.items import InformationItem
from sina_weibo.redis.spiders import RedisSpider
from scrapy.spiders import Spider

class Spider(RedisSpider):
    name = "weibo_user"
    host = "http://weibo.cn"
    cell_id = None
    # 谢娜 - 8千万粉丝
    start_urls = [
        1192329374
    ]

    def start_requests(self):
        for ID in self.start_urls:
            ID = str(ID)
            url_information0 = "http://weibo.cn/attgroup/opening?uid=%s" % ID
            url_fans = "http://weibo.cn/%s/fans" % ID
            yield Request(url=url_information0, meta={"ID": ID}, callback=self.parse0)  # 去爬个人信息
            yield Request(url=url_fans, callback=self.parse3)  # 去爬粉丝

    def parse0(self, response):
        """ 抓取个人信息1 """
        informationItems = InformationItem()
        selector = Selector(response)
        text0 = selector.xpath('body/div[@class="u"]/div[@class="tip2"]').extract_first()
        if text0:
            num_tweets = re.findall(u'\u5fae\u535a\[(\d+)\]', text0)  # 微博数
            num_follows = re.findall(u'\u5173\u6ce8\[(\d+)\]', text0)  # 关注数
            num_fans = re.findall(u'\u7c89\u4e1d\[(\d+)\]', text0)  # 粉丝数
            if num_tweets:
                informationItems["num_tweets"] = int(num_tweets[0])
            if num_follows:
                informationItems["num_follows"] = int(num_follows[0])
            if num_fans:
                informationItems["num_fans"] = int(num_fans[0])
            informationItems["user_id"] = response.meta["ID"]
            url_information1 = "http://weibo.cn/%s/info" % response.meta["ID"]
            yield Request(url=url_information1, meta={"item": informationItems}, callback=self.parse1)

    def parse1(self, response):
        """ 抓取个人信息2 """
        informationItems = response.meta["item"]
        selector = Selector(response)
        text1 = ";".join(selector.xpath('body/div[@class="c"]/text()').extract())  # 获取标签里的所有text()
        nickname = re.findall(u'\u6635\u79f0[:|\uff1a](.*?);', text1)  # 昵称
        gender = re.findall(u'\u6027\u522b[:|\uff1a](.*?);', text1)  # 性别
        place = re.findall(u'\u5730\u533a[:|\uff1a](.*?);', text1)  # 地区（包括省份和城市）
        signature = re.findall(u'\u7b80\u4ecb[:|\uff1a](.*?);', text1)  # 个性签名
        birthday = re.findall(u'\u751f\u65e5[:|\uff1a](.*?);', text1)  # 生日
        sexorientation = re.findall(u'\u6027\u53d6\u5411[:|\uff1a](.*?);', text1)  # 性取向
        marriage = re.findall(u'\u611f\u60c5\u72b6\u51b5[:|\uff1a](.*?);', text1)  # 婚姻状况
        url = re.findall(u'\u4e92\u8054\u7f51[:|\uff1a](.*?);', text1)  # 首页链接

        if nickname:
            informationItems["nickname"] = nickname[0]
        if gender:
            informationItems["gender"] = gender[0]
        if place:
            place = place[0].split(" ")
            informationItems["province"] = place[0]
            if len(place) > 1:
                informationItems["city"] = place[1]
        if signature:
            informationItems["signature"] = signature[0]
        if birthday:
            try:
                birthday = datetime.datetime.strptime(birthday[0], "%Y-%m-%d")
                informationItems["birthday"] = birthday - datetime.timedelta(hours=8)
            except Exception:
                pass
        if sexorientation:
            if sexorientation[0] == gender[0]:
                informationItems["sex_orientation"] = "gay"
            else:
                informationItems["sex_orientation"] = "Heterosexual"
        if marriage:
            informationItems["marriage"] = marriage[0]
        if url:
            informationItems["url"] = url[0]
        yield informationItems

    def parse3(self, response):
        """ 抓取关注或粉丝 """
        selector = Selector(response)
        text2 = selector.xpath(
            u'body//table/tr/td/a[text()="\u5173\u6ce8\u4ed6" or text()="\u5173\u6ce8\u5979"]/@href').extract()
        for elem in text2:
            elem = re.findall('uid=(\d+)', elem)
            if elem:
                ID = int(elem[0])

                url_information0 = "http://weibo.cn/attgroup/opening?uid=%s" % ID
                yield Request(url=url_information0, meta={"ID": ID}, callback=self.parse0)  # 去爬个人信息
        url_next = selector.xpath(
            u'body//div[@class="pa" and @id="pagelist"]/form/div/a[text()="\u4e0b\u9875"]/@href').extract()
        if url_next:
            yield Request(url=self.host + url_next[0],
                          callback=self.parse3)
