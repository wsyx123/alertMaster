'''
Created on Dec 30, 2019

@author: yangxu
'''

from wsgiref.simple_server import make_server
import os
import json
import pickle
import logging

class APIServer():
    def __init__(self,host='localhost',port=9001,queue=None):
        self.host = host
        self.port = port
        self.manager_queue = queue
        
    def routers(self):
        urlpatterns = (
            ('/status', self.get_status),
            ('/rule/add',self.add_rule),
            ('/rule/delete',self.delete_rule),
            ('/worker/get',self.get_workers),
            ('/worker/stop',self.stop_worker)
        )
        return urlpatterns
        
    def handle_request(self,environment,start_response):
        start_response('200 OK',[('Content-Type','application/json')])
        url = environment['PATH_INFO']
        urlpatterns = self.routers()
        func = None
        for item in urlpatterns:
            if item[0] == url:
                func = item[1]
                break
        if func:
            return func(environment)
        else:
            return ['{"status":404}'.encode('utf-8')]
        
        
    def start(self):
        try:
            httpd = make_server('',self.port,self.handle_request)
            logging.info("Listen on : {}:{}".format(self.host,self.port))
            httpd.serve_forever()
        except Exception as e:
            logging.error(e.message)
            os._exit(1)
    
    def get_status(self,environment):
        rule_data = self.get_rule() # 这里获取到的dict 数据 是使用单引号, 不能直接使用str(rule_data), 浏览器解析会出错
        rule_data = json.dumps(rule_data) # 需要使用json.dumps() 转成str , 这样数据是使用双引号
        return [rule_data.encode('utf-8')]
    
    def get_workers(self,environment):
        if os.path.isfile('data/worker.pk'):
            with open('data/worker.pk', 'rb') as f:
                workers = pickle.load(f)
                return [json.dumps(workers).encode('utf-8')]
        else:
            return ['{}'.encode('utf-8')]
    
    def get_rule(self):
        if os.path.isfile('data/rule.pk'):
            with open('data/rule.pk', 'rb') as f:
                rule_data = pickle.load(f)
                return rule_data
        else:
            return {}
    
    def save_rule(self,rule_data):
        with open('data/rule.pk','wb') as f:
            pickle.dump(rule_data,f)
       
    def add_rule(self,environment):
        try:
            request_body_size = int(environment.get('CONTENT_LENGTH', 0))
        except (ValueError):
            request_body_size = 0
            return ['{"status":400,"message":"Bad request"}'.encode('utf-8')]
        else:
            data_json = json.loads(environment['wsgi.input'].read(request_body_size))
            rule_data = self.get_rule()
            if rule_data.get(data_json['id']):
                del rule_data[data_json['id']]
            rule_data[data_json['id']] = data_json
            self.save_rule(rule_data)
            self.manager_queue.put({'action':1,"rule":data_json})
            
        return ['{"status":200}'.encode('utf-8')]
    
    def delete_rule(self,environment):
        try:
            request_body_size = int(environment.get('CONTENT_LENGTH', 0))
        except (ValueError):
            request_body_size = 0
            return ['{"status":400,"message":"Bad request"}'.encode('utf-8')]
        else:
            data_json = json.loads(environment['wsgi.input'].read(request_body_size))
            rule_data = self.get_rule()
            ruleid = int(data_json['ruleid'])
            if rule_data.get(ruleid):
                del rule_data[ruleid]
                self.save_rule(rule_data)
                self.manager_queue.put({"action":2,"ruleid":ruleid})
                return ['{"status":200,"message":"rule has deleted"}'.encode('utf-8')]
            else:
                return ['{"status":404,"message":"rule not found"}'.encode('utf-8')]
    
    
    def stop_worker(self,environment):
        try:
            request_body_size = int(environment.get('CONTENT_LENGTH', 0))
        except (ValueError):
            request_body_size = 0
            return ['{"status":400,"message":"Bad request"}'.encode('utf-8')]
        else:
            data_json = json.loads(environment['wsgi.input'].read(request_body_size))
            worker_name = data_json['worker']
            self.manager_queue.put({'action':3,"worker":worker_name})
            
        return ['{"status":200}'.encode('utf-8')]
        
    