# encoding=utf-8
import logging
import json
import base64
import requests
import re
import time
from sina_weibo import resources_pb2
from datetime import datetime
from socket import *

def getCookies(account,password):
    """ 获取Cookies """
    loginURL = r'https://login.sina.com.cn/sso/login.php?client=ssologin.js(v1.4.15)'
    username = base64.b64encode(account.encode('utf-8')).decode('utf-8')

    postData = {
        "entry": "sso",
        "gateway": "1",
        "from": "null",
        "savestate": "30",
        "useticket": "0",
        "pagerefer": "",
        "vsnf": "1",
        "su": username,
        "service": "sso",
        "sp": password,
        "sr": "1440*900",
        "encoding": "UTF-8",
        "cdult": "3",
        "domain": "sina.com.cn",
        "prelt": "0",
        "returntype": "TEXT",
    }
    session = requests.Session()

    r = session.post(loginURL, data=postData)
    jsonStr = r.content.decode('gbk')
    info = json.loads(jsonStr)

    if info["retcode"] == "0":
        cookie = session.cookies.get_dict()
        print "Get Cookie Success!( Account:%s )" % account
        logging.info("Get Cookie Success!( Account:%s )" % account)
    else:
        print 'Login failed account: ' + account
        print info['reason']
        logging.info('Login failed account: ' + account)
        logging.info(info['reason'])

    return cookie

def time_tran(s):
    """ 将时间格式标准化  """
    s = s.strip(' \r\t\n')
    if s.find(u'\u5206\u949f\u524d') >= 0:
        return str(datetime.fromtimestamp((int(time.time())-60*int(s[:s.find(u'\u5206\u949f\u524d')]))))
    elif s.find(u'\u79d2\u524d') >= 0:
        return str(datetime.fromtimestamp((int(time.time())-int(s[:s.find(u'\u79d2\u524d')]))))
    elif s.find(u'\u4eca\u5929') >= 0:
        return str(datetime(datetime.now().year,datetime.now().month,datetime.now().day,int(s.split(' ')[1].split(':')[0]),int(s.split(' ')[1].split(':')[1]),0))
    elif s.find(u'\u6708') >= 0 and s.find(u'\u5e74') < 0:
        y = datetime.now().year
        m = int(s.split(u'\u6708')[0])
        s = s.split(u'\u6708')[1]
        d = int(s.split(u'\u65e5')[0])
        s = s.split(u'\u65e5')[1]
        h = int(s.split(':')[0])
        minute = int(s.split(':')[1])
        return str(datetime(y,m,d,h,minute,0))
    else:
        return s

'''将传入的uname转为uid'''
def get_uid_by_uname(uname, cookies):
    r = requests.get(url="http://weibo.cn/" + uname, cookies=cookies)
    id = re.search(r'<a href="/(\d+)/info">', r.text)
    if id:
        return id.groups()[0]
    return None

''' 请求代理时返回字符串，请求账号时返回tuple  '''
def resource_req():
    ''' 构造资源请求message '''
    request = resources_pb2.ResourceRequest()
    request.clientID = '192.168.1.222'		#请求端标识
    request.requestType = 'test'			#请求类型

    demand = request.requests.add()
    demand.resourceName = 'PROXY'			#请求资源名PROXY/WEIBO_ACCOUNT
    demand.resourceCount = 1				#请求资源数量
    demand.strategy = resources_pb2.Allocation_Strategy_Fixed		#分配策略

    load = request.SerializeToString()

    ''' 构造传输协议实体
    head = proto.QzoneProtocolHead()
    entity = proto.QzoneProtocol()
    entity.soh = 0
    entity.head = head
    entity.body = load
    '''

    ''' 用socket发送传输协议实体  '''
    HOST = '192.168.1.178'
    PORT = 4567
    BUFSIZE = 4096
    ADDR = (HOST,PORT)
    cliSock = socket.socket(socket.AF_INET , socket.SOCK_STREAM)
    cliSock.connect(ADDR)

    fmt = "!2cih2ichci%ds"%len(load)
    entity = struct.pack(fmt , chr(4) , chr(1) , 0x01 , 0 , 123456 , 0 , chr(0) , 0 , chr(0) , 24 + len(load) , load)
    print 'entity start'
    print len(entity)
    print 'entity end'
    cliSock.sendall(entity)

    time.sleep(1)

    ''' 用socket接收调度器返回的资源响应message  '''
    s = ''
    data = ''
    while True:
        data = cliSock.recv(BUFSIZE)
        if not data:
            break
        s += data
    print 'response start'
    print len(s)
    print 'response end'

    ''' 解析协议内容，得到资源信息 '''
    fmt = "!2cih2ichci%ds"%len(s)-24
    rst = struct.unpack(fmt,s)
    res = resources_pb2.ResourceResponse()
    res.ParseFromString(rst[-1])
    print res.resourceName,res.resourceInfo

