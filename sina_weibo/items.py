# encoding=utf-8

from scrapy import Item, Field


class InformationItem(Item):
    """ 个人信息 """
    user_id = Field()  # 用户ID
    nickname = Field()  # 昵称
    gender = Field()  # 性别
    province = Field()  # 所在省
    city = Field()  # 所在城市
    signature = Field()  # 个性签名
    birthday = Field()  # 生日
    num_tweets = Field()  # 微博数
    num_follows = Field()  # 关注数
    num_fans = Field()  # 粉丝数
    sex_orientation = Field()  # 性取向
    marriage = Field()  # 婚姻状况
    url = Field()  # 首页链接
    fir_category = Field()
    sec_category = Field()


class TweetsItem(Item):
    """ 微博信息 """
    user_id = Field()  # 用户ID
    tweet_id = Field()  # 用户ID-微博ID
    content = Field()  # 微博内容
    pubtime = Field()  # 发表时间
    co_oridinates = Field()  # 定位坐标
    tools = Field()  # 发表工具/平台
    like = Field()  # 点赞数
    comment = Field()  # 评论数
    transfer = Field()  # 转载数
    vflag = Field()	#认证用户标记
    nickname = Field()	#用户昵称
    tag = Field()	#微博标签


class FollowsItem(Item):
    """ 关注人列表 """
    user_id = Field()  # 用户ID
    follows = Field()  # 关注


class FansItem(Item):
    """ 粉丝列表 """
    user_id = Field()  # 用户ID
    fans = Field()  # 粉丝


class CommentItem(Item):
	""" 评论信息 """
	user_id = Field()	#用户ID
	tweet_id = Field()	#微博ID
	comment_id = Field()	#评论ID
	content = Field()	#内容
	pubtime = Field()	#发表时间
	co_ordinates = Field()	#定位坐标
	like = Field()	#点赞数
	vflag = Field()	#认证用户标记
	nickname = Field()	#用户昵称
	tag = Field()	#分类标记


class TopicItem(Item):
	""""  话题信息  """
	topic_name = Field()	#话题名称
	topic_code = Field()	#话题编号
	tag = Field()	#话题标签
	subtitle= Field()	#话题子标题
	views = Field()	#话题被浏览次数
	host_id = Field()	#主持人ID
	host_nick = Field()	#主持人昵称
	last_update = Field()	#最后抓取时间

class UserItem(Item):
	""" 用户信息  """
	uid = Field()	#用户ID
	nickname = Field()	#昵称
	vflag = Field()	#是否是认证用户
