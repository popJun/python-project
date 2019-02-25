# -*- coding: utf-8 -*-
'''
Created on 2016-12-12

@author: kang
'''
import sys
import getopt
import os
import datetime
import types
import commands
import re
import multiprocessing,Queue
import fcntl
import hashlib
import time
import json
import urllib,urllib2,socket,cookielib
import traceback

def getAllMac():
    try:
        ret = ''
        cmdline='/sbin/ifconfig'
        r = os.popen(cmdline).read()
        if r:
            L = re.findall('HWaddr.*?([0-9,A-F]{2}:[0-9,A-F]{2}:[0-9,A-F]{2}:[0-9,A-F]{2}:[0-9,A-F]{2}:[0-9,A-F]{2})', r)
            L = set(L)
            ret = ','.join(str(tmp) for tmp in L)
    except Exception,e:
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'get mac fail:',e
    return ret

#HTTP请求封装
class HttpHelper:
    @staticmethod
    def request(url,params = None):#用于读取网页内容
        try:
            timeout = 30
            socket.setdefaulttimeout(timeout)
            cj = cookielib.CookieJar()
            opener=urllib2.build_opener(urllib2.HTTPCookieProcessor(cj),urllib2.HTTPRedirectHandler());
            opener.addheaders = [('User-agent',
                    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)')]
            if params != None:
                params = urllib.urlencode(params)
            req = urllib2.Request(url, params)
            response = opener.open(req)
            content = response.read()
            return content
        #400-599 状态码异常
        except urllib2.HTTPError,e:
            #print "status_fail",e
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"status_fail",e.reason
            return None
        # 连接异常
        except urllib2.URLError,e:
            #print "connect_fail",e.reason
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"connect_fail",e.reason
            return None
        #超时异常
        except Exception,e:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),Exception,e
            return None

    @staticmethod
    def data_post(url,data):
        http_content = None
        for i in xrange(3):
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'post try count:',i+1
            try :
                http_content = HttpHelper.request(url,data)
                if http_content != None:
                    break
            except:
                print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'post try fail count',i+1
        if http_content == None:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'upload_content None'
        else:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'upload_content:',http_content.strip()
            
    @staticmethod
    def data_requset(url,data):
        encodedjson = []
        
        for i in xrange(3):
            try:
                print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'try request count:',i+1
                http_content = HttpHelper.request(url,data)
                if http_content == None:
                    continue
                encodedjson = eval(http_content)
            except Exception,e:
                print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),Exception,e
                continue
            break 
        return encodedjson

class Config:
    def __init__(self):  
        self.TaskId = None
        self.Deep = None
        self.OnceCrawNum = None
        self.UpdateMinuteBefore = None
        self.CrawMinute = None
        self.WhiteStaticDomainNumPre = None
        self.WhiteStaticDomainSizePre = None
        self.CheckHttpsRequestSwitch = None
        self.HostSwitch = None
        self.StaticTypes = None
        self.ProccessNum = 5
        
    def init(self):    
        global TASKID,GET_TASK_URL,MACS,URLS,static_content_types,check_request_https_switch
        data = HttpHelper.data_requset(GET_TASK_URL,{'macs':MACS,'taskId':TASKID})
        #data = {'hostFirstSwitch': 0, 'lastValid_t': 21600, 'httpsSwitch': 0, 'id': 44, 'staticTypes': 'Image css javascript flash', 'staticNumPer': 50, 'eachDepth_t': 30, 'staticSizePer': 50, 'depth': 3, 'rangeNum': 200, 'datas': [{'url': 'http://www.baidu.com'},{'url': 'http://www.youku.com'}]}
        try:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),data
        except:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'print data fail'

        try:
            if data == []:
                Static.refuse_duty("refuse task beacause get config fail or empty!")
                
            if int(data["id"]) != TASKID:
                Static.refuse_duty("refuse task beacause taskid clash")
                
            self.TaskId = TASKID
            self.Deep = int(data["depth"])
            self.OnceCrawNum = int(data["rangeNum"])
            self.UpdateMinuteBefore = int(data["lastValid_t"])
            self.CrawMinute = int(data["eachDepth_t" ])
            self.WhiteStaticDomainNumPre = int(data["staticNumPer"])
            self.WhiteStaticDomainSizePre = int(data["staticSizePer"])
            
            static_content_types = data["staticTypes"].split()
            
            
                        
            if int(data["httpsSwitch" ]) == 1:
                self.CheckHttpsRequestSwitch = True
            elif int(data["httpsSwitch" ]) == 0:
                self.CheckHttpsRequestSwitch = False
            else :
                Static.refuse_duty("refuse task beacause the httpsSwitch not 1 or 0 ,value range or type fail")

            if int(data["hostFirstSwitch" ]) == 1:
                self.HostSwitch = True
            elif int(data["hostFirstSwitch" ]) == 0:
                self.HostSwitch = False
            else :
                Static.refuse_duty("refuse task beacause the hostFirstSwitch not 1 or 0 ,value range or type fail")
            
            if not self.checkdata() :
                Static.refuse_duty("refuse task beacause the config value unaviliable")
            
            if type(data["datas"]) == type([]) and data["datas"] != []:
                for dict in data["datas"]:
                    URLS.append(dict["url"])
            else :
               Static.refuse_duty("refuse task beacause the datas value type error or empty") 
               
        except Exception,e:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),Exception,e
            Static.refuse_duty("refuse task beacause get config fail or empty!")
            Static.sys_exit()
         
        time.sleep(3)
        self.output()
        
    def checkdata(self):
        if type(self.TaskId) != None and type(self.Deep) == types.IntType and type(self.OnceCrawNum) == types.IntType and type(self.UpdateMinuteBefore) == types.IntType and type(self.CrawMinute) == types.IntType and type(self.WhiteStaticDomainNumPre) == types.IntType and type(self.WhiteStaticDomainSizePre) == types.IntType and type(self.ProccessNum) == types.IntType :
            return True
        else :
            return False

    def simple(self):
        global URLS
        self.TaskId = 44
        self.Deep = 4
        self.OnceCrawNum = 200
        self.UpdateMinuteBefore = 21600
        self.CrawMinute = 20
        self.WhiteStaticDomainNumPre = 50
        self.WhiteStaticDomainSizePre = 50
        self.CheckHttpsRequestSwitch = 50
        self.HostSwitch = True
        self.StaticTypes = True
        self.ProccessNum = 5  
        URLS=[]
        URLS.append("http://www.baidu.com")
        URLS.append("http://www.youku.com")
        
    def output(self):
        global URLS,static_content_types
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "id",self.TaskId
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "depth",self.Deep
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "rangeNum",self.OnceCrawNum
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "lastValid_t",self.UpdateMinuteBefore
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "eachDepth_t",self.CrawMinute
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "staticNumPer",self.WhiteStaticDomainNumPre
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "staticSizePer",self.WhiteStaticDomainSizePre
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "httpsSwitch" ,self.CheckHttpsRequestSwitch
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "hostFirstSwitch",self.HostSwitch
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "staticTypes",static_content_types
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.ProccessNum,self.ProccessNum
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), URLS,URLS

class Static:
    @staticmethod
    def is_instance():
        global pidfile
        pidfile = open(os.path.realpath(__file__), "r")
        try:
            fcntl.flock(pidfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except Exception,e:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"another instance is running..."
            return False
            
    @staticmethod            
    def refuse_duty(message,status = 1):
        global TASKID,TASK_REFUSE_URL,MACS
        
        #status = 1
        if status and Static.is_instance():
            status = 0
        data = {'errMes':message,'taskId':TASKID,'macs':MACS,'status':status}
        HttpHelper.data_post(TASK_REFUSE_URL,data)
        print TASK_REFUSE_URL,data
        Static.sys_exit()
        return

    @staticmethod         
    def reset_pid():
        global FILENAME
        pid = os.getpid()
        _str = 'ps aux | grep '+FILENAME+ ' | grep -v ' + str(pid) + '|awk  \'{s[$2\"\"]++}END{for (i in s) str = str\"\"i\"\"\" \";print str}\''
        pids = commands.getoutput('kill -9 `' + _str + '`')
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'reach command and stop another task!'

    @staticmethod   
    def sys_exit():
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'---------------------end this script ...!-------------'
        sys.exit(1)
        
    @staticmethod
    def stopChildPid():
        global FILENAME
        pid = str(os.getpid())
        _str = 'ps axjf | grep ' +  pid + ' | grep '+ FILENAME +' | awk \'{if( $2 != "'+ pid +'" ) print $2}\''
        pids = commands.getoutput('kill -9 `'+_str+'`')
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'stopchildpid',pids
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'reach command and stop another task!'
        
    @staticmethod       
    def deal_command(command):
        if command == 'stop':
            Static.reset_pid()
            Static.sys_exit()
        if command == 'restart':
            Static.reset_pid()
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'start new task!' 

class StaticUtil:  
    @staticmethod
    def get_md5_value(src):
        myMd5 = hashlib.md5()
        myMd5.update(src)
        myMd5_Digest = myMd5.hexdigest()
        return myMd5_Digest.strip()

    @staticmethod
    def get_precent(numerator,denominator):
        try :
            return round(float(numerator*100)/denominator,2)
        except:
            return round(0,2)
            
    @staticmethod
    def handler(signum, frame):
        raise AssertionError
        
    @staticmethod
    def craw_one_url(url,time=None):
        global PWD
        if  time != None and datetime.datetime.now().strftime("%Y/%m/%d/-%H:%M:%S") > time:
            return None
        command = PWD+'referer/phantomjs '+ PWD+'referer/hm_crawler.js \"' + url.strip() + '\"'
        try:
            s, output = commands.getstatusoutput(command)
            json_object = json.loads(output, encoding='gb2312')
            if s != 0 or json_object['status'] == '':
                return None
            return json_object 
        except Exception,e:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), command,' crawl_error:',e
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'output'
            return None
            
    @staticmethod
    def craw_file(md5,avilibletime):
        global craw_url_path
        craw_url_file = craw_url_path + md5
        str = StaticFile.cat_file(craw_url_file)
        try:
            content = eval(str)
            if content['status'] == '' or content['status'] == 'fail' or content['refreshtime'] < avilibletime:
                return None
            tmpcheck = content['status']
            return content
        except:
            return None
            
    @staticmethod
    def craw_url(md5,avilibletime,url,endtime):
        try:
            json_data = StaticUtil.craw_file(md5,avilibletime)
            tmp_https_domains = set()
            if json_data == None:
                json_data = StaticUtil.craw_one_url(url,endtime)
                if json_data != None:
                    json_data,tmp_https_domains = Static_Resource.updata_craw_data(json_data,md5)
            return json_data,tmp_https_domains
        except Exception,e:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'time out',url
            return None,None
        
class StaticFile:
    @staticmethod
    def re_write_file(file,str):
        if type(file) != types.StringType or file == '' or type(str) != types.StringType:
            return
        fp = open(file,'w+')
        try:
            fp.write(str)
        finally:
            fp.close()
            
    @staticmethod
    def remove_file(file):
        if type(file) == types.StringType and file != "":
            commands.getoutput('/bin/rm -rf ' + file)
        
    @staticmethod
    def cat_file(file_path):
        data = ''
        if type(file_path) != types.StringType or file == '' :
            return 
        if os.path.exists(file_path):
            s, data = commands.getstatusoutput('/bin/cat ' + file_path)
            if s != 0:
                data = ''
        return data

    @staticmethod
    def re_write_json(file_path,values):
        if type(file_path) != types.StringType or file_path == '' :
            return 
        try:
            StaticFile.remove_file(file_path)
            json.dump(values, open(file_path, 'w'))
        except Exception,e:
            print 'json dump str error',values,type(values),Exception,e
            

            
    @staticmethod
    def check_json(file_path):
        json = StaticFile.cat_json(file_path)
        std = ''
        try:
            tmp = json['status']
            if tmp == 'xxx':
                tmp ='ok'
        except Exception,e:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'std = e',e
            return False
        return True
			
    @staticmethod
    def cat_json(file_path):	
        try:
            if not os.path.exists(file_path):
                return None
            readed = json.load(open(file_path, 'r'))
            return readed
        except Exception,e:			
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'read file',file_path,'error:',e
            #print cat_file(file_path)
            StaticFile.remove_file(file_path)
            return None

class StaticHTTP:
    @staticmethod
    def get_url_request_type(url):
        request_type = -1
        try:
            if url.find("http://") == 0:
                request_type = 1     #http请求
            elif url.find("https://") == 0:
                request_type = 2     #https请求
        except Exception,e:
            request_type = -1

        finally:
            return request_type
            
    @staticmethod
    def get_domain_and_port (url):
        host = None
        port = None
        url = url.strip()
        try:
            proto, rest = urllib.splittype(url) 
            host, rest = urllib.splithost(rest) 
            host, port = urllib.splitport(host)  
            host = host.strip()
            if port == None:
                port = 80
            port = str(port)
            if host == '' or host == None:
                host = None
                port = None
            
        except Exception,e:
            host = None
            port = None
            #print 'get domain fail',url,e
        finally:
            return host,port  

    @staticmethod            
    def get_host(domain):
        strs = domain.split('.')
        _len = len(strs)
        if _len < 2:
            return domain
        host = strs[ _len-2 ] + '.' + strs[ _len-1 ]
        if host == 'com.cn' and _len >= 3:
            host = strs[ _len-3 ] + '.' + host
        return host

    @staticmethod
    def check_port(domain,port):
        #    nc -v -w 1 -z www.baidu.com 443
        status = False
        sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sk.settimeout(1)
        try:
            sk.connect((domain,port))
            status = True
        except Exception:
            status = False
        sk.close()
        return status

class Static_Resource:
    ###页面资源更新
    @staticmethod
    def updata_resource_data(contents):
        global resource_path,tmp_file,https_domains,check_request_https_switch,tmp_domain_set

        tmp_https_domains = set()
        resoures = set()
        for content in contents:
            url = content['url'].strip()
            request_type = StaticHTTP.get_url_request_type(url)
            if request_type < 0 :
                continue
            md5 = StaticUtil.get_md5_value(url)
            if md5 in resoures :
                continue
            domain,port = StaticHTTP.get_domain_and_port(url)
            if domain == None:
                continue 
            type = ''
            if request_type == 1:
                #http://
                type = 'http'
                tmp_url = "https" + url[4:]
                if check_request_https_switch and domain not in tmp_domain_set and domain not in https_domains:
                    tmp_domain_set.add(domain)
                    commands.getoutput('/bin/echo "'+domain+' '+ tmp_url +'" >>' + tmp_file)
            elif request_type == 2 :
                #https://
                type = 'https'
                if domain not in tmp_https_domains:
                    tmp_https_domains.add(domain)
            data = {'request_type':type,"contentType":content["contentType"],"domain":domain,"contentLength":content["contentLength"],'port':port,'url':url}
            if md5 == '':
                print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'md5 fail',url,type(url),StaticUtil.get_md5_value(url)
            StaticFile.re_write_json(resource_path+ md5,data)
            resoures.add(md5)
        resoures = ','.join(str(tmp) for tmp in resoures)
        return resoures,tmp_https_domains
    
    @staticmethod
    def updata_craw_data(json_object,filename):
        global craw_url_path
        try:
            if json_object == None or json_object['status'] == None or json_object['status'] == '' or json_object['status'] == 'fail':
                return None,None
        except:
            return None,None
        
        refreshtime = datetime.datetime.now().strftime("%Y/%m/%d/-%H:%M:%S")
        ###次级链接更新    
        next_links_dict = []
        links_set = set()
        for dict in json_object['links']:
            url = dict['url'].strip()
            request_type = StaticHTTP.get_url_request_type(url)
            if request_type < 0 :
                continue
            domain,port = StaticHTTP.get_domain_and_port(url)
            if domain == None:
                continue
                
            url_tmp = url
            pos = url.find('?')
            if pos > 0 :
                url_tmp = url[0:pos]
            md5 = StaticUtil.get_md5_value(url_tmp)
            if md5 not in links_set:
                tmp = {'url':url,'md5':md5,'host':StaticHTTP.get_host(domain)}
                next_links_dict.append(tmp)
                links_set.add(md5)
                
        resoure,tmp_https_domains = Static_Resource.updata_resource_data(json_object['contents'])     
        data = {'refreshtime':refreshtime,'status':json_object['status'],'resource':resoure,'links':next_links_dict}
        StaticFile.re_write_json(craw_url_path+filename,data)
        if not StaticFile.check_json(craw_url_path+filename):
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), data
        return data,tmp_https_domains
        
class Detection():
    pid = ''
    oldchildren = []
    childmommand = ''
    
    def __init__(self):
        global FILENAME
        pid = str(os.getppid())
        s,gpid = commands.getstatusoutput('ps axjf | grep '+ pid +' | grep ' + FILENAME +' | head -n 1 | awk \'{print $3}\'')
        self.childmommand = 'ps axjf | grep '+ gpid +' | grep "hm_crawler.js" | grep -v "sh -c " | awk \'{print $2""$NF}\''
        self.alivecommand = 'ps axjf | grep '+ gpid +' | grep ' + FILENAME + ' | wc -l'
        print self.childmommand
        print self.alivecommand
        
    def pidalive(self):
        try :
            s,str = commands.getstatusoutput(self.alivecommand)
            if s != 0:
                print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'alive fail count ',str
            if int(str) > 4:
                return True
            else:
                return False
        except Exception,e:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'detection alive',Exception,e
        return False
    
    def killprocess(self,url):
        try:
            npos = url.find('http')
            if npos <= 0:
                return 
            pid = int(url[0:npos])
            command = 'kill -9 '+ str(pid)
            status,output = commands.getstatusoutput(command)
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'kill url:',url
        except Exception, e:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),('kill %s fail,%s:%s',url,Exception,e)
        
    def getchilren(self):
        status,output = commands.getstatusoutput(self.childmommand)
        if status == 0 and output != None and output != '':
            try:
                children = output.split()
                for str in children:
                    if len(str) < 7 or str.find('http') == -1:
                        children.remove(str)
                return children
            except Exception,e:
                print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), Exception,':',str
        return None 
        
    def start(self):
        newchild=self.getchilren()
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'last time craw :',self.oldchildren
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'this time craw :',newchild
        if newchild == None or type(newchild) != type([]):
            return
            
        if self.oldchildren == None:
            self.oldchildren = newchild       
            return
        
        for url in self.oldchildren:
            if url in newchild:
                self.killprocess(url)
        
        self.oldchildren = newchild

class Module:
    @staticmethod
    def init():
        global CONFIG,result_path,craw_url_path,resource_path,tmp_file,https_domains,https_domains_file
        CONFIG.init()
        #CONFIG.simple()
        if not os.path.exists('referer/data/'):
            commands.getoutput('/bin/mkdir referer/data/')
        if not os.path.exists(result_path):
            commands.getoutput('/bin/mkdir ' + result_path)
        if not os.path.exists(craw_url_path):
            commands.getoutput('/bin/mkdir ' + craw_url_path)
        if not os.path.exists(resource_path):
            commands.getoutput('/bin/mkdir ' + resource_path)
        StaticFile.remove_file(tmp_file)
        
        https_domains = set()
        domains_str = StaticFile.cat_file(https_domains_file)
        try:
            domains = domains_str.split(',')
            for domain in domains:
                if domain not in https_domains:
                    https_domains.add(domain)
        except:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'get https_domainsfail'
            
        commands.getstatusoutput('rm -rf '+result_path+'*')
             
    @staticmethod
    def run():
        global URLS,CONFIG,DONE,https_domains,craw_end,rate_txt
        commands.getoutput('rm -rf '+ rate_txt)
        for URL in URLS:
            crawtask(URL,CONFIG,DONE)
            StaticFile.re_write_file(rate_txt,str(len(DONE)*100/len(URLS)))
    @staticmethod
    def detetion():
        detection = Detection()
        if detection == None:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'detection fail'
            Static.sys_exit()
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'detection start---------------------------------------------'
        while True:
            time.sleep(60)
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'detection start'
            detection.start()
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'detection end'
     
    @staticmethod
    def upload(rate=None):
        global UPLOAD_DATA_URL,MACS,TASKID,result_path,https_domains,craw_end,rate_txt
        datas=[]
        
        for file in os.listdir(result_path):
            try:
                str = StaticFile.cat_file(result_path + file)
                
                data = eval(str)
                mDatas = []
                for mDomain in data["Children"].keys():
                    try:
                        tmp = data["Children"][mDomain]
                        staticDomain = 1
                        port80 = 1
                        if mDomain in https_domains:
                            staticDomain = 0
                        if tmp['HttpPorts'] != '':
                            port80 = 0
                        mDatas.append({'refreshTime':data['RefreshTime'],'mDomain':mDomain,'status':data['Status'],'depth':data['Deep'],'port80':port80,'not80':tmp['HttpPorts'],'staticDomain':staticDomain,'httpsNum':tmp['HttpsNum'],'httpNum':tmp['HttpNum'],'staticNum':tmp['HttpStaticNum'],'staticNumPer':tmp['StaticNumPrecent'],'httpSize':tmp['HttpTotalSize'],'staticSize':tmp['HttpStaticSize'],'staticSizePer':tmp['StaticSizePrecent']})
                    except Exception,e:
                        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), Exception,e
                datas.append({'url':data['Url'],'domain':data['MainDomain'],'mDatas':mDatas})
            except Exception,e:
                print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'fail get file:',result_path + file
                continue
                http_content = None
        try:
            datas = json.dumps(datas)
        except Exception,e:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'data change to json fail,stop upload!'
            return 
        
        try :
            if rate== None:
                str = StaticFile.cat_file(rate_txt)
                print '=-------get file',str
                rate = int(str)
        except:
            rate = 30
        upload_data = {'macs':MACS,'taskId':TASKID,'rate':rate,'datas':datas}
        #print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'upload---',upload_data
        HttpHelper.data_post(UPLOAD_DATA_URL,upload_data)
 
class Class_CrawState:
    main_domain = None  #string
    url = None          #string   
    host = ''
    resource = set()
    link_md5=set()
    deep = 0            #int 
    status = 'fail'       #string
    
    next_links = []
    
    switch = False      #优先爬取同host域名
    endtime = datetime.datetime.now().strftime("%Y/%m/%d/-%H:%M:%S")
    refreshtime = ''
    avilibletime = ''
    proccess_num = 1
    
    https_domains= set()
    avilible = False
    md5 = None
    
    def __init__(self,url,md5,crawminite,avilibletime,once_craw_num,switch=False,proccess_num=5):
        self.url = url
        self.md5 = md5
        self.main_domain,port = StaticHTTP.get_domain_and_port(url)
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.main_domain,port
        if self.main_domain == None or len(self.main_domain)==0:
            return
            
        endtime = datetime.datetime.now() + datetime.timedelta(seconds=crawminite*60)
        self.endtime = endtime.strftime("%Y/%m/%d/-%H:%M:%S")
        avilibletime = datetime.datetime.now() - datetime.timedelta(seconds=avilibletime*60)
        self.avilibletime = avilibletime.strftime("%Y/%m/%d/-%H:%M:%S")    
        self.switch = switch
        self.proccess_num = proccess_num
        
        self.host = StaticHTTP.get_host(self.main_domain)
        self.resource = set()
        self.link_md5=set()
        self.deep = 0
        self.status = 'fail' 
        self.next_links = []
        self.https_domains= set()
        self.once_craw_num = once_craw_num
        
        self.avilible = True 
        
    def craw(self):
        if self.deep > 0:
            self.craw_again()
            return 
            
        if not self.avilible:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.url,'craw fail'
            return 
        
        if self.craw_md5(self.md5) == False:
            return 
        url_data = StaticUtil.craw_file(self.md5,self.avilibletime)
        if url_data == None :
            json_object = StaticUtil.craw_one_url(self.url,self.endtime)
            if json_object == None:
                return
            while json_object["status"] == 'fail' and json_object["redirect"] != '':
                url = json_object["redirect"].split()
                if url[0:4] != 'http':
                    break
                json_object = StaticUtil.craw_one_url(url,self.endtime)
            url_data,tmp_https_domains = Static_Resource.updata_craw_data(json_object,self.md5)
            if tmp_https_domains != None and len(tmp_https_domains)!= 0:
                    self.https_domains= self.https_domains|tmp_https_domains
        if url_data == None:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'craw fail'
            self.avilible = False
            return 
        try:
            if type(url_data["status"]) == types.StringType:
                self.status = url_data["status"]
            self.deep = 1 
            self.next_links = []
            self.update_resource_and_links(url_data['resource'],url_data['links'])
            self.refreshtime = datetime.datetime.now().strftime("%Y/%m/%d/-%H:%M:%S")
            print time.strftime("%Y/%m/%d/-%I:%M:%S  "),self.url,' crawl_deep:',self.deep,' craw_num:',1
        except Exception,e:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e
 
    def craw_again(self):
        global waitflag
        once_craw_num =self.once_craw_num
        if not self.avilible:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.url,'craw again fail'
            return
            
        proccess_num = self.proccess_num

        pool = multiprocessing.Pool(proccess_num)
        multi_results = []
        
        first_craw = []
        sec_craw = []
        for tmp_data in self.next_links:
            if not self.craw_md5(tmp_data['md5']):
                continue
            if self.switch and tmp_data['host'] == self.host :
                first_craw.append(tmp_data)
            else :
                sec_craw.append(tmp_data)
        self.next_links = []
        
        self.deep += 1
        craw_num = 0
        
        for tmp_data in first_craw:
            if craw_num >= once_craw_num:
                break
            multi_results.append(pool.apply_async(pros,({'md5':tmp_data['md5'],'avilibletime':self.avilibletime,'url':tmp_data['url'],'endtime':self.endtime},)))
            craw_num=craw_num + 1
            
        for tmp_data in sec_craw:
            if craw_num >= once_craw_num:
                break
            multi_results.append(pool.apply_async(pros,({'md5':tmp_data['md5'],'avilibletime':self.avilibletime,'url':tmp_data['url'],'endtime':self.endtime},)))
            craw_num=craw_num + 1  
        pool.close()
        pool.join()
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'end'
        
        for result in multi_results:
            try:
                result = result.get()
                if result == None:
                    continue
                #print result
                url_data = result['url_data']
                tmp_https_domains = result['tmp_https_domains']
                if tmp_https_domains != None and len(tmp_https_domains)!=0:
                    self.https_domains= self.https_domains|tmp_https_domains
                if url_data == None:
                    continue
                self.update_resource_and_links(url_data['resource'],url_data['links'])
            except Exception,e:
                print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'new fail',e
                continue

        print time.strftime("%Y/%m/%d/-%I:%M:%S  "),self.url,' crawl_deep:',self.deep,' craw_num:',craw_num
        self.refreshtime = datetime.datetime.now().strftime("%Y/%m/%d/-%H:%M:%S")
        
    def craw_md5(self,md5):
        if md5 not in self.link_md5:
            self.link_md5.add(md5)
            return True
        return False

    def update_resource_and_links(self,resources,links):
        resources = resources.split(',')
        for resource in resources:
            if resource in self.resource:
                continue
            self.resource.add(resource)
        self.next_links = self.next_links + links
     
    def resource_statistics(self):
        global static_content_types
        global resource_path
        domain_set = set()
        domain_dict = {}
        for resource_file in self.resource:
            str = StaticFile.cat_file(resource_path + resource_file)
            try :
                resource = eval(str)
                if len(resource)== 0:
                    continue 
                domain = resource["domain"]
                port = resource["port"]
                request_type = resource["request_type"]
                contentType = resource["contentType"]
                contentLength = int(resource["contentLength"])
            except Exception,e:
                #print 'error:',e,resource_file
                #print str  部分资源content-Type 为 null
                continue
            
            
            if domain not in domain_set:
                domain_dict[domain] = {'HttpNum':0,'HttpStaticNum':0,'HttpTotalSize':0,'HttpStaticSize':0,'HttpsNum':0,'HttpPorts':''}
                domain_set.add(domain)
                
            domain_data = domain_dict[domain]
            
            if port != '80':
                domain_data['HttpPorts'] = domain_data['HttpPorts'] + ' ' + port
            
            if request_type == 'https':
                domain_data['HttpsNum'] += 1
                continue
            domain_data['HttpNum'] += 1
            domain_data['HttpTotalSize'] += contentLength
            if contentType == None or contentType == '':
                continue
            for tmp in static_content_types:
                if contentType.find(tmp)>=0:
                    domain_data['HttpStaticNum'] += 1
                    domain_data['HttpStaticSize'] += contentLength
        return domain_dict
        
    def consolo_state(self):
        if not self.avilible:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.url,'consolo_state fail'
            return 
        domain_dict = self.resource_statistics()
        for domain in domain_dict.keys():
            tmp = domain_dict[domain]
            tmp["StaticNumPrecent"] = StaticUtil.get_precent(tmp["HttpStaticNum"],tmp["HttpNum"])
            tmp["StaticSizePrecent"] = StaticUtil.get_precent(tmp["HttpStaticSize"],tmp["HttpTotalSize"])
            port_dict = {}
            ports = tmp["HttpPorts"].split()
            _str = ''
            for port in ports:
                if port not in port_dict.keys():
                    port_dict[port] = 0
                port_dict[port] += 1
            for key in port_dict.keys():
                _str = _str + key + "["+ str(port_dict[key])+"] "
            tmp["HttpPorts"] = _str
        data =  {"MainDomain":self.main_domain,"Children":domain_dict,'Status':self.status,'Deep':self.deep,'Url':self.url,'RefreshTime':self.refreshtime,'Url':self.url}
        return data
    
    def get_https_domains(self):
        return self.https_domains
 
#全局变量
reload(sys)
sys.setdefaultencoding('utf8')

GET_TASK_URL='http://183.251.100.12:8888/logserver/api/requestAnalysisTask.html'
UPLOAD_DATA_URL='http://183.251.100.12:8888/logserver/api/analysisData.html'
TASK_REFUSE_URL='http://183.251.100.12:8888/logserver/api/rejectCacheAnalysis.html'
CONFIG=Config()

TASKID=None
PWD='/usr/local/cacheAnalysis/'
FILENAME=os.path.basename(__file__)
TASKURLS=[]
MACS = getAllMac().encode('utf-8')	
URLS = []
DONE = set()
REALPATH=os.path.realpath(__file__)

#输出文件路径
result_path = PWD + 'referer/data/result/'
craw_url_path = PWD + 'referer/data/crawurl/'
resource_path = PWD + 'referer/data/resource/'


url_file = 'url.txt'            #爬取的url列表
tmp_file = PWD + 'referer/data/tmpcheck' 
https_domains_file = PWD + 'referer/data/HTTPS.txt' #https域名路径
static_content_types = []
check_request_https_switch=False
https_domains=set()
waitflag = False

#进程管理
upload_pid=None
detection_pid=None
rate_txt=PWD+'referer/data/rate'
       
def pros(d):
    md5 = d['md5']
    avilibletime = d['avilibletime']
    endtime = d['endtime']
    url = d['url']
    update_flag = False
    json_data = StaticUtil.craw_file(md5,avilibletime)
    tmp_https_domains = set()
    if json_data == None:
        json_data = StaticUtil.craw_one_url(url,endtime)
        update_flag = True
    if update_flag and json_data != None:
        try :
            json_data,tmp_https_domains = Static_Resource.updata_craw_data(json_data,md5)
        except Exception,e:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'update error:',e
    data = {'url_data':json_data,'tmp_https_domains':tmp_https_domains}
    return data
            
def try_https_request():
    global tmp_file,check_request_https_switch,waitflag

    if not check_request_https_switch or not os.path.exists(tmp_file):
        return
    global https_domains
    print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'start check_request_https'
    domain_set = set()
    fp = open(tmp_file,'rb+')
    processes = 5
    pool = multiprocessing.Pool(processes)

    line = fp.readline()
    multi_results = []
    waitflag = True
    while line:
        tmp = line
        line = fp.readline()
        try:
            tmp = tmp.split()
            if len(tmp)!= 2 or tmp[0] in domain_set or tmp[0] in https_domains:
                continue
            domain_set.add(tmp[0])
            multi_results.append(pool.apply_async(do_https_check,(tmp[0],tmp[1])))

        except:
            continue
    fp.close()       
    StaticFile.remove_file(tmp_file)
    waitflag = False
    pool.close()
    pool.join()
        
    for result in multi_results:
        tmp_https_domains = result.get()
        if tmp_https_domains == None:
            continue
        https_domains = https_domains|tmp_https_domains
        
def update_https_domains():
    global https_domains,https_domains_file
    print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'start update https domains'
    try_https_request()
    while '' in https_domains:
        https_domains.remove('')
    domains = ','.join(str(tmp) for tmp in https_domains)
    StaticFile.re_write_file(https_domains_file,domains)
    
def crawtask(url,CONFIG,Done):
    global HTTPS,https_domains
    request_type = StaticHTTP.get_url_request_type(url)
    if request_type < 0:
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'invalid url:',url
        return 
        
    step = 1
    md5 = StaticUtil.get_md5_value(url)
    
    
    resultfile = result_path + md5
    
    ps = Class_CrawState(url,md5,CONFIG.CrawMinute,CONFIG.UpdateMinuteBefore,CONFIG.OnceCrawNum,CONFIG.HostSwitch,CONFIG.ProccessNum)
    if not ps.avilible:
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'this url craw invalid,url',url
        return 
    while step <= CONFIG.Deep:
        try:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'crawdeep',step,'start'
            ps.craw()
            tmp_data = ps.consolo_state()
            StaticFile.re_write_json(resultfile,tmp_data)
            tmp_https_domains = ps.get_https_domains()
            if tmp_https_domains!= None and len(tmp_https_domains)!=0:
                https_domains = https_domains|tmp_https_domains
            update_https_domains()
            step += 1
        except Exception,e:
            traceback.print_exc()
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), url,'craw resource error',e
            break
    if md5 not in Done:
        Done.add(md5)
                                    
def run():
    if not Static.is_instance():
        Static.refuse_duty('refuse task,because another task is running...',0)
        
    Module.init()
    
    pid = os.fork()
    if pid == 0:
        Module.detetion()
    else :
        pid = os.fork()      
        if pid == 0:
            while True:
                time.sleep(60*30)
                Module.upload()
        else :
            Module.run()
            Static.stopChildPid()
            Module.upload(100)
              
if __name__=='__main__':
    command=None
    print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'---------------------start a new script....!--------------------------'
    opts, args = getopt.getopt(sys.argv[1:], "i:c:")
    for op, value in opts:
        if op == "-i":
            TASKID = value
        if op == "-c":
            command = value
    if command != None and command != '':       
        Static.deal_command(command)
        
    if TASKID == None or TASKID == '':
        TASKID = -1
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'TASKID empty'
        Static.sys_exit()
    try:
        TASKID = int(TASKID)
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'reach TASKID:',TASKID
    except:
        tmp = TASKID
        TASKID = -1
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'TASKID error'
        Static.refuse_duty('refuse task,because get error taskid:['+tmp+']')
    #Module.upload(100)    
    run();
    print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'---------------------end this script ...!-------------'
        
