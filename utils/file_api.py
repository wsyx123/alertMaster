'''
文件操作api
Created on Feb 7, 2020

@author: yangxu
'''
import os
import pickle

class FileOps():
    def __init__(self,path=None,lock=None):
        self.path = path
        self.lock = lock
    
    def read(self):
        if self.is_exist():
            with open(self.path, 'rb') as f:
                rule_data = pickle.load(f)
                return rule_data
        else:
            return {}
    
    def write(self,data):
        self.lock.acquire()
        with open(self.path, 'wb') as f:
            pickle.dump(data,f)
        self.lock.release()
    
    def is_exist(self):
        return os.path.isfile(self.path)
    
    def close(self):
        pass
    
    def _flush(self):
        pass
    
    
    
        

