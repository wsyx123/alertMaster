'''
Created on Dec 30, 2019

@author: yangxu
'''

import multiprocessing
import pickle
import os
import time
from worker.master import WorkerMaster

class WorkerManager():
    def __init__(self,queue=None):
        self.manager_queue = queue
        self.worker_number = 0
        self.worker_table = {}
        self.worker_queue = {}
        
    def get_worker(self):
        if os.path.isfile('data/worker.pk'):
            with open('data/worker.pk', 'rb') as f:
                rule_data = pickle.load(f)
                return rule_data
        else:
            return {}
    
    def save_worker(self,worker_table):
        with open('data/worker.pk','wb') as f:
            pickle.dump(worker_table,f)
            
    def add_rule_judge(self,rule):
        '''
        新规则进来, 检查是否有相同业务的规则
        '''
        for worker,value in self.worker_table.items():
            for ruleid,rulevalue in value['rules'].items():
                if rulevalue['business'] == rule['business']:
                    self.add_rule(worker,rule)
                    return
        self.create_worker_process(rule)
                
        
    def add_rule(self,worker,rule):
        self.worker_table
        self.worker_table[worker]['ruleid'].append(rule['id'])
        self.worker_table[worker]['rules'][rule['id']] = rule
        self.save_worker(self.worker_table)
        self.worker_queue[worker].put({'action':1,'rule':rule})
        
    
    def delete_rule(self,ruleid):
        '''
        在删除rule时, 不能直接遍历self.worker_table, 并在期间删除self.worker_table中的内容
        所以需要从worker.pk 中获取, 然后在self.worker_table中删除再更新到worker.pk
        '''
        worker_table = self.get_worker()
        for worker in worker_table.keys():
            if ruleid in worker_table[worker]['ruleid']:
                self.delete_data_process(worker, ruleid)
                if len(self.worker_table[worker]['ruleid']) == 0:
                    self.stop_worker(worker)
                    self.remove_worker_from_db(worker)
    
    def delete_data_process(self,workername,ruleid):
        self.worker_queue[workername].put({'action':2,'ruleid':ruleid})
        self.worker_table[workername]['ruleid'].remove(ruleid)
        del self.worker_table[workername]['rules'][ruleid]
        self.save_worker(self.worker_table)
                
    def stop_worker(self,workername):
        self.worker_queue[workername].put({'action':3})
        
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
        workermaster = WorkerMaster(rule=rule,worker_queue=q)
        p = multiprocessing.Process(target=workermaster.start_master,name=workername)
        p.start()
        self.worker_table[workername]['pid'] = p.pid
        self.worker_table[workername]['rules'] = {}
        self.worker_table[workername]['rules'][rule['id']] = rule
        self.save_worker(self.worker_table)
        self.worker_number = self.worker_number + 1
        
    def start(self):
        #action_define = {1:{}}
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