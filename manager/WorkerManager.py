'''
Created on Dec 30, 2019

@author: yangxu
'''

import multiprocessing
import pickle
import time
from worker.WorkerMaster import WorkerMaster
from config.master import worker_maximum_businesses,worker_maximum_rules
from config.master import rule_filename
from utils.file_api import FileOps
class WorkerManager():
    def __init__(self,queue=None,lock=None):
        self.manager_queue = queue
        self.rule_lock = lock
        self.worker_number = 0
        self.worker_table = {}
        self.worker_queue = {}
        
    def _init_worker(self):
        '''
        获取规则,初始化worker进程
        '''
        file_ops = FileOps(path=rule_filename)
        data = file_ops.read()
        for rule in data.values():
            self.add_rule_judge(rule)
        
    def get_worker(self):
        try:
            with open('data/worker.pk', 'rb') as f:
                rule_data = pickle.load(f)
                return rule_data
        except:
            return {}
    
    def save_worker(self,worker_table):
        self.rule_lock.acquire()
        with open('data/worker.pk','wb') as f:
            pickle.dump(worker_table,f)
        self.rule_lock.release()
            
    def add_rule_judge(self,rule):
        '''
        新规则进来:
        1 检查worker_table里有无worker
        2 检查worker当前处理的规则数是否已到maximum
        3 检查worker能同时处理的业务数
        '''
        def has_business():
            res = {'worker':None,'status':False}
            for worker,value in self.worker_table.items():
                if rule['business'] in value['businesses']:
                    return {'worker':worker,'status':True}
            return res
        
        def dispatch_business():
            '''
            分发新规则(此规则所属业务还没订阅):
            1 如果一个worker可以处理不同业务，那么分发
            2 如果一个worker不可以处理不同业务，那么新建worker
            '''
            if worker_maximum_businesses > 1:
                #同一个worker 处理不同business需要重载 kafka consumer,通过给data_read_thread 发信号实现
                for worker,value in self.worker_table.items():
                    if len(value['businesses']) < worker_maximum_businesses and len(value['ruleid']) < worker_maximum_rules:
                        self.add_rule(worker, rule,reload_consumer=True)
                        self.worker_table[worker]['businesses'].append(rule['business'])
                        return
            self.create_worker_process(rule)
                    
        
        if self.worker_table.keys():
            has_business_res = has_business()
            if has_business_res['status']:
                worker = has_business_res['worker']
                if len(self.worker_table[worker]['ruleid']) < worker_maximum_rules:
                    self.add_rule(worker,rule)
                else:
                    self.create_worker_process(rule)
            else:
                dispatch_business()
                
        else:
            self.create_worker_process(rule)
                
        
    def add_rule(self,worker,rule,reload_consumer=False):
        '''
        需要判断规则是否已经存在(这个bug应该在前端处理,处理如下:)
        1 前端启用规则时,判断告警程序的返回状态, 成功就状态改变, 未成功就不改变
        '''
        if rule['id'] not in self.worker_table[worker]['ruleid']:
            self.worker_table[worker]['ruleid'].append(rule['id'])
            self.worker_table[worker]['rules'][rule['id']] = rule
            self.save_worker(self.worker_table)
            self.worker_queue[worker].put({'action':1,'rule':rule,'reload':reload_consumer})
        
    
    def delete_rule(self,ruleid):
        '''
        在删除rule时, 不能直接遍历self.worker_table, 并在期间删除self.worker_table中的内容
        所以需要从worker.pk 中获取, 然后在self.worker_table中删除再更新到worker.pk
        
        备注: self.worker_table, self.worker_queue, file_worker_table 可能数据不一致(当程序意外重启)
        '''
        file_worker_table = self.get_worker()
        for worker in file_worker_table.keys():
            if ruleid in file_worker_table[worker]['ruleid']:
                self.delete_data_process(worker, ruleid)
                if self.worker_table.get(worker):
                    if len(self.worker_table[worker]['ruleid']) == 0:
                        self.stop_worker(worker)
    
    def delete_data_process(self,workername,ruleid):
        '''
        从进程中删除 data_process
        '''
        if self.worker_queue.get(workername):
            self.worker_queue[workername].put({'action':2,'ruleid':ruleid})
            self.worker_table[workername]['ruleid'].remove(ruleid)
            del self.worker_table[workername]['rules'][ruleid]
            self.save_worker(self.worker_table)
            print('workermanager 122 send action=2 ruleid={} success'.format(ruleid))
        else:
            print('workername:',workername,' is not exist')
                
    def stop_worker(self,workername):
        if self.worker_queue.get(workername):
            self.worker_queue[workername].put({'action':3})
            print('workermanager 129 send action=3 workername={} success'.format(workername))
            del self.worker_queue[workername]
        self.remove_worker_from_db(workername)
        
    def remove_worker_from_db(self,workername):
        if self.worker_table.get(workername):
            del self.worker_table[workername]
            self.save_worker(self.worker_table)
    
    def create_worker_process(self,rule):
        workername = 'worker'+str(self.worker_number)
        q = multiprocessing.Queue()
        self.worker_queue[workername] = q
        self.worker_table[workername] =  {}
        self.worker_table[workername]['ruleid'] = [rule['id']]
        workermaster = WorkerMaster(rule=rule,worker_queue=q,workername=workername)
        p = multiprocessing.Process(target=workermaster.start_master,name=workername)
        p.start()
        self.worker_table[workername]['pid'] = p.pid
        self.worker_table[workername]['rules'] = {}
        self.worker_table[workername]['rules'][rule['id']] = rule
        self.worker_table[workername]['businesses'] = [rule['business']]
        self.save_worker(self.worker_table)
        self.worker_number = self.worker_number + 1
        
    def start(self):
        #action_define = {1:{}}
        self._init_worker()
        while True:
            if self.manager_queue.empty():
                time.sleep(5)
            else:
                signal = self.manager_queue.get()
                
                if signal['action'] == 1:
                    self.add_rule_judge(signal['rule'])
                    
                elif signal['action'] == 2:
                    self.delete_rule(signal['ruleid'])
                    
                elif signal['action'] == 3:
                    workername = signal['worker']
                    self.stop_worker(workername)
                    
                else:
                    print('unknown signal: %s' %(signal))