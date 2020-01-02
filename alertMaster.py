'''
Created on Dec 30, 2019

@author: yangxu
'''
import queue
import threading

from utils.logger import logger_config
from api.server import APIServer
from manager.WorkerManager import WorkerManager  

def main():
    manager_queue = queue.Queue(maxsize=1)
    threads = []
    server = APIServer(queue=manager_queue)
    serverThread = threading.Thread(target=server.start)
    #serverThread.setDaemon(True)
    threads.append(serverThread)
    
    manager = WorkerManager(queue=manager_queue)
    managerThread = threading.Thread(target=manager.start)
    #managerThread.setDaemon(True)
    threads.append(managerThread)
    
    for thread in threads:
        thread.start()
    
    '''
    Daemon(daemon=True)线程会被粗鲁的直接结束,
    因此如果需要线程被优雅的结束，请设置为非Daemon(daemon=False)线程，并使用合理的信号方法，如事件Event
    
    thread join 方法是一个与daemon设置无关的, 如果 上面设置了 thread.join() , 那么主线程会一直等待在那里,下面的代码不会执行
    直到所有子线程运行完(退出)
    '''
    print('aa')
    
    
    
if __name__ == "__main__":
    #日志初始化
    logger_config('log/alertMaster.log')
       
    main()
    
# def format_alert_variable():
#     '''
#     变量识别, 格式化成字符串
#     实现使用正则表达式
#     '''
#     import re
#     variable_str = '${hostname}-502-${message}'
#     log_data = {'hostname':'172.16.149.2','message':'test'}
#     pattern = re.compile('\${[\w\d]+}')
#     variable_list = pattern.findall(variable_str)
#     for variable in variable_list:
#         key = variable[2:-1]
#         value = log_data[key]
#         new_variable = '\{}'.format(variable)
#         new_variable_str = re.sub(new_variable,value,variable_str)
#         variable_str = new_variable_str
#     print(variable_str)

# class Stack():
#     def __init__(self):
#         self.stack = []
#         self.length = 0
#     
#     def push(self,obj):
#         '''
#         压入
#         '''
#         self.stack.append(obj)
#         self.length = self.length+1
#     
#     def pop(self):
#         '''
#         弹出
#         '''
#         if not self.empty():
#             self.length = self.length - 1
#             return self.stack.pop()
#             
#         else:
#             return None
#     
#     def empty(self):
#         '''
#         是否为空
#         '''
#         if self.length == 0:
#             return True
#         else:
#             return False
#     
#     def list(self):
#         return self.stack
#     
# def infix_to_prefix():
#     #logic_expression = 'A&(B|C)'
#     logic_expression = '(A|B)&C'
#     logic_expression_list = [x for x in logic_expression]
#     logic_expression_list.reverse()
#     s1 = Stack()
#     s2 = Stack()
#     for obj in logic_expression_list:
#         if obj in ['(',')','|','&']:
#             if obj == '(':
#                 while not s1.empty():
#                     tmpobj = s1.pop()
#                     if tmpobj != ')':
#                         s2.push(tmpobj)
#                     else:
#                         continue
#             else:
#                 s1.push(obj)
#         else:
#             s2.push(obj)
#     
#     while not s1.empty():
#         s2.push(s1.pop())
#         
#     prefix_list = s2.list()
#     
#     expression_result = {'A':True,'B':False,'C':False}
#     
#     key_list = [key for key in expression_result.keys()]
#     final_key = ''.join(key_list)
#     
#     s3 = Stack()
#     for obj in prefix_list:
#         if obj in ['|','&']:
#             expression_id1 = s3.pop()
#             expression_id2 = s3.pop()
#             new_id = expression_id1+expression_id2
#             if obj == '|':
#                 if expression_result[expression_id1] or expression_result[expression_id2]:
#                     expression_result[new_id] = True
#                 else:
#                     expression_result[new_id] = False
#             else:
#                 if expression_result[expression_id1] and expression_result[expression_id2]:
#                     expression_result[new_id] = True
#                 else:
#                     expression_result[new_id] = False
#             s3.push(new_id)
#         else:
#             s3.push(obj)
#     
#     print(final_key)
#     print(expression_result[final_key])
#     
# infix_to_prefix()