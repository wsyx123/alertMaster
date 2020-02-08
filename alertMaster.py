'''
Created on Dec 30, 2019

@author: yangxu
'''
import queue
import threading

from utils.logger import logger_config
from api.server import APIServer
from manager.WorkerManager import WorkerManager
from config.master import apiserver_port

def main():
    manager_queue = queue.Queue(maxsize=1)
    lock = threading.Lock()
    threads = []
    server = APIServer(port=apiserver_port,queue=manager_queue,lock=lock)
    serverThread = threading.Thread(target=server.start)
    #serverThread.setDaemon(True)
    threads.append(serverThread)
    
    manager = WorkerManager(queue=manager_queue,lock=lock)
    managerThread = threading.Thread(target=manager.start,)
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
    logger_config('log/alertMaster')
       
    main()