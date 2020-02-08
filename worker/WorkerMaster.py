'''
Created on Dec 30, 2019

@author: yangxu
'''
import threading
import queue
import os

from .data_read import data_read_thread
from .data_process import data_process_thread
from .alert_process import alert_process_thread

class Stack():
    def __init__(self):
        self.stack = []
        self.length = 0
    
    def push(self,obj):
        '''
        压入
        '''
        self.stack.append(obj)
        self.length = self.length+1
    
    def pop(self):
        '''
        弹出
        '''
        if not self.empty():
            self.length = self.length - 1
            return self.stack.pop()
            
        else:
            return None
    
    def empty(self):
        '''
        是否为空
        '''
        if self.length == 0:
            return True
        else:
            return False
    
    def list(self):
        return self.stack

class WorkerMaster():
    '''
    新建一个Worker==workerMaster, 只支持传一个告警规则, 如果有多个告警规则需要一个Worker处理, 需要多次传递, 流程如下:
    1 新建worker, 传一个rule, 启动数据读取，数据处理，告警压缩
    2 workermanager 使用queue 新增rule, rule判断(判断rule所属业务, worker中如果没有同业务的rule就需要额外启动数据读取), 
    启动数据处理
    '''
    def __init__(self,rule=None,worker_queue=None,workername=None):
        self.ruleid = rule['id']
        self.rules = {}
        self.rules[rule['id']] = {'rule':rule}
        self.worker_queue = worker_queue
        self.workername = workername
        self.kafka_consumer_reload = False
        self.exit = False
        self.dataprocess_queue = {}
        self.slide_window_records = {}
        self.alert_compress_queue = queue.Queue(maxsize=500)
        
    def start_master(self):
        self.start_data_process(self.ruleid)
        self.start_data_read()
        self.start_alert_process()
        
        while True:
            signal = self.worker_queue.get()
            if signal['action'] == 1:
                rule = signal['rule']
                self.rules[rule['id']] = {'rule':rule}
                self.start_data_process(rule['id'])
                self.kafka_consumer_reload = signal['reload']
                
            elif signal['action'] == 2:
                self.stop_data_process(signal['ruleid'])
            
            elif signal['action'] == 3:
                os._exit(0)
    
    def start_data_read(self):
        data_read = threading.Thread(target=data_read_thread,args=(self,))
        data_read.setDaemon(True)
        data_read.start()
        
    def stop_data_read(self):
        pass
        
    
    def start_data_process(self,ruleid):
        data_process_queue = queue.Queue(maxsize=500)
        thread_name = 'data_process_'+str(ruleid)
        self.rules[ruleid]['thread_name'] = thread_name
        data_process = threading.Thread(target=data_process_thread,args=(self,ruleid,data_process_queue),name=thread_name)
        data_process.setDaemon(True)
        data_process.start()
        self.dataprocess_queue[thread_name] = data_process_queue
    
    def stop_data_process(self,ruleid):
        self.rules[ruleid]['data_process'] = False
    
    def start_alert_process(self):
        alert_process = threading.Thread(target=alert_process_thread,args=(self,))
        alert_process.setDaemon(True)
        alert_process.start()
    