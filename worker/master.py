'''
Created on Dec 30, 2019

@author: yangxu
'''
import threading
import queue
import time
import os
import re
import datetime

from utils.email_way import send_email

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
    rules = {}
    def __init__(self,rule=None,worker_queue=None):
        self.ruleid = rule['id']
        self.rules[rule['id']] = {'rule':rule}
        self.worker_queue = worker_queue
        self.exit = False
        self.dataprocess_queue = {}
        self.slide_window_records = {}
        self.alert_compress_queue = queue.Queue(maxsize=500)
        
    def start_master(self):
        self.start_data_process(self.ruleid)
        self.start_data_read()
        self.start_alert_process()
        
        while True:
            if self.worker_queue.empty():
                time.sleep(5)
            else:
                signal = self.worker_queue.get()
                if signal['action'] == 1:
                    rule = signal['rule']
                    self.rules[rule['id']] = rule
                    self.start_data_process(rule['id'])
                    
                elif signal['action'] == 2:
                    self.stop_data_process(signal['ruleid'])
                
                elif signal['action'] == 3:
                    os._exit(0)
    
    def data_read_thread(self):
        message = {'business':'smart','component':'nginx','alarm_depth':2,
                   'hostname':'172.16.149.2','code':502,'message':'威胁攻击可视化平台 log Error 关键字告警测试 !'}
        while True:
            for key,data_queue in self.dataprocess_queue.items():
                read_timestamp = datetime.datetime.now()
                read_timestamp = int(time.mktime(read_timestamp.timetuple()))
                message['read_timestamp'] = read_timestamp
                data_queue.put(message)
            time.sleep(1)
    
    def start_data_read(self):
        data_read = threading.Thread(target=self.data_read_thread)
        data_read.setDaemon(True)
        data_read.start()
    
    def data_process_thread(self,ruleid,data_process_queue):
        self.rules[ruleid]['data_process'] = True
        while self.rules[ruleid]['data_process']:
            if data_process_queue.empty():
                time.sleep(2)
            else:
                '''
                1 获取日志
                2 判断告警深度, 如果到组件级别,那么不属于此组件的直接过滤掉
                3 表达式执行
                '''
                log_data = data_process_queue.get()
                if self.rules[ruleid]['rule']['alarm_depth'] ==2:
                    if self.rules[ruleid]['rule']['component'] != log_data['component']:
                        continue
                
                expression_result = {}
                for expression in self.rules[ruleid]['rule']['expressions']:
                    res = self.expression_execute(expression, log_data)
                    expression_result[expression['expression_id']] = res
                logic_expression = self.rules[ruleid]['rule']['logic_expression']
                
                if self.logic_expression_execute(logic_expression, expression_result):
                    log_data['ruleid'] = ruleid # 为数据打上ruleid
                    self.alert_compress_queue.put(log_data)
        
        #停止数据处理后的操作        
        del self.dataprocess_queue[self.rules[ruleid]['thread_name']]
        del self.rules[ruleid]
        
    def logic_expression_execute(self,logic_expression,expression_result):
        '''
        (A|B)&C  ,  A&(B|C)  这两个表达式 为中缀表达式, 需要转换为前缀表达式
        转换使用栈: 符号栈(存符号)s1, 字符栈(存字符)s2
        转换思路: 从右 到 左 扫描字符串, 当遇到左括号 '(' 时 从s1 弹出符号 入栈 s2 直到遇到 右括号 ')' ,再继续把剩下的字符入栈,
                完成后把s1 的符号出栈 入到 s2,  至此, s2 就是前缀表达式
        
        前缀表达式运算: 从右 到 左扫描 s2字符串, 遇到字符直接入栈, 遇到符号 从栈里弹出两个字符进行运算, 运算结果再入栈, 再重复操作
        '''
        
        key_list = [key for key in expression_result.keys()]
        final_key = ''.join(key_list)
        
        '''
        下面是中缀转前缀
        '''
        #logic_expression = 'A&(B|C)'
        #logic_expression = '(A|B)&C'
        logic_expression_list = [x for x in logic_expression]
        logic_expression_list.reverse()
        s1 = Stack()
        s2 = Stack()
        for obj in logic_expression_list:
            if obj in ['(',')','|','&']:
                if obj == '(':
                    while not s1.empty():
                        tmpobj = s1.pop()
                        if tmpobj != ')':
                            s2.push(tmpobj)
                        else:
                            continue
                else:
                    s1.push(obj)
            else:
                s2.push(obj)
        
        while not s1.empty():
            s2.push(s1.pop())
        prefix_list = s2.list()
        
        '''
        下面进行逻辑运算
        '''
        s3 = Stack()
        for obj in prefix_list:
            if obj in ['|','&']:
                expression_id1 = s3.pop()
                expression_id2 = s3.pop()
                new_id = expression_id1+expression_id2
                if obj == '|':
                    if expression_result[expression_id1] or expression_result[expression_id2]:
                        expression_result[new_id] = True
                    else:
                        expression_result[new_id] = False
                else:
                    if expression_result[expression_id1] and expression_result[expression_id2]:
                        expression_result[new_id] = True
                    else:
                        expression_result[new_id] = False
                s3.push(new_id) # 把计算结果再压入栈
            else:
                s3.push(obj)
        return expression_result[final_key]
        
               
    def expression_execute(self,expression_dict,log_data):
        '''
        判断日志是否有相应字段,如果没有返回 {expression_id:False}
        field type : (1,'Integer'), (2,'String')
        '''
        if log_data.get(expression_dict['field']):
            '''
            首先类型转换一致, 有可能转换异常, 有异常, 直接返回 False
            如果是字符串可能有大小写问题, 所以需要都转成小写
            '''
            try:
                if expression_dict['type'] == 1:
                    expression_value = int(expression_dict['value'])
                    log_value = int(log_data[expression_dict['field']])
                else:
                    expression_value = str(expression_dict['value']).lower()
                    log_value = str(log_data[expression_dict['field']]).lower()
            except:
                return False
            else:
                if expression_dict['operator'] == 'eq':
                    if expression_value == log_value:
                        return True
                elif expression_dict['operator'] == 'ne':
                    if expression_value != log_value:
                        return True
                elif expression_dict['operator'] == 'gt':
                    if expression_value > log_value:
                        return True
                elif expression_dict['operator'] == 'ge':
                    if expression_value >= log_value:
                        return True
                elif expression_dict['operator'] == 'lt':
                    if expression_value < log_value:
                        return True
                elif expression_dict['operator'] == 'le':
                    if expression_value <= log_value:
                        return True
                elif expression_dict['operator'] == 'in':
                    if expression_value in log_value:
                        return True
                    
        return False
            
        
    
    def start_data_process(self,ruleid):
        data_process_queue = queue.Queue(maxsize=500)
        thread_name = 'data_process_'+str(ruleid)
        self.rules[ruleid]['thread_name'] = thread_name
        data_process = threading.Thread(target=self.data_process_thread,args=(ruleid,data_process_queue),name=thread_name)
        data_process.setDaemon(True)
        data_process.start()
        self.dataprocess_queue[thread_name] = data_process_queue
    
    def stop_data_process(self,ruleid):
        self.rules[ruleid]['data_process'] = False
        
    def alert_process_thread(self):
        while True:
            if self.alert_compress_queue.empty():
                time.sleep(3)
            else:
                log_data = self.alert_compress_queue.get()
                self.alert_compress_process(log_data)
    
    def alert_compress_process(self,log_data):
        '''
        告警压缩处理
        1 是否启用压缩
        2 延迟检查
        3 告警间隔检查
        4 最大告警数检查
        5 告警时段检查
        '''
        ruleid = log_data['ruleid']
        read_timestamp = log_data['read_timestamp']
        #1
        if not self.rules[ruleid]['rule']['compress_enabled']:
            self.alert_send(log_data)
            return
        #2
        if self.log_is_delay(ruleid,read_timestamp):
            self.log_delay_alert()
        else:
            self.init_slide_window_record(ruleid) #滑动窗口初始化
            #3
            if self.alert_interval_check(ruleid,read_timestamp):
                #4,5
                total_result = self.alert_total_check(ruleid,read_timestamp)
                time_result = self.alert_time_check(ruleid)
                if total_result and time_result:
                    self.alert_send(log_data)
                    return
            self.alert_storage(log_data)
    
    def init_slide_window_record(self,ruleid):
        if not self.slide_window_records.get(ruleid):
            self.slide_window_records[ruleid] = {}
            self.slide_window_records[ruleid]['log_list'] = []
    
    def log_is_delay(self,ruleid,read_timestamp):
        '''
        延迟检查
        '''
        now_timestamp = datetime.datetime.now()
        now_timestamp = int(time.mktime(now_timestamp.timetuple()))
        total_time = self.rules[ruleid]['rule']['total_time']
        if now_timestamp - read_timestamp > int(total_time):
            return True
        else:
            return False
    
    def alert_interval_check(self,ruleid,read_timestamp):
        '''
        告警间隔检查
        '''
        if len(self.slide_window_records[ruleid]['log_list']) > 0:
            last_log_timestamp = self.slide_window_records[ruleid]['log_list'][-1]
            if read_timestamp - last_log_timestamp < self.rules[ruleid]['rule']['interval_time']:
                return False
        return True
    
    def alert_total_check(self,ruleid,read_timestamp):
        '''
        最大告警数检查
        '''
        if len(self.slide_window_records[ruleid]['log_list']) < self.rules[ruleid]['rule']['total_number']:
            return True
        else:
            '''
            检查最旧的一条日志时间戳还在不在最近total_time 内, 这个 '最近' 是以当前这条日志的时间为基准
            '''
            oldest_log = self.slide_window_records[ruleid]['log_list'][0]
            if read_timestamp - oldest_log > self.rules[ruleid]['rule']['total_time']:
                self.slide_window_records[ruleid]['log_list'] = self.slide_window_records[ruleid]['log_list'][1:]
                self.alert_total_check(ruleid, read_timestamp)
            else:
                return False
            
    
    def alert_time_check(self,ruleid):
        '''
        告警时段检查
        '''
        alarm_date = self.rules[ruleid]['rule']['alarm_date']
        time_type = self.rules[ruleid]['rule']['time_type']
        alarm_date_result = self.alarm_date_check(alarm_date)
        time_type_result = self.time_type_check(time_type,ruleid)
        if alarm_date_result and time_type_result:
            return True
        else:
            return False
    
    def alarm_date_check(self,alarm_date):
        '''
        检查告警周期
        '''
        if alarm_date == 1: #全年
            return True
        else: #工作日
            d=datetime.datetime.now()
            if d.weekday() != 5 or d.weekday() !=6:
                return True
            else:
                return False
    
    def time_type_check(self,time_type,ruleid):
        '''
        检查告警时段
        '''
        if time_type == 1: #全天
            return True
        else: #时段
            now_time = str(datetime.datetime.now()).split(' ')[1].split('.')[0]
            start_time = self.rules[ruleid]['rule']['start_time']
            end_time = self.rules[ruleid]['rule']['end_time']
            if now_time >= start_time and now_time <=end_time:
                return True
            else:
                return False
            
    
    def log_delay_alert(self):
        '''
        日志延迟告警
        '''
        pass
    
    def alert_storage(self,log_data):
        '''
        告警入库
        '''
        print('告警入库')
    
    def alert_send(self,log_data):
        '''
        告警发送
        日志发送后, 需要把已发送的日志时间戳append 进 slide_window_records
        '''
        ruleid = log_data['ruleid']
        read_timestamp = log_data['read_timestamp']
        self.slide_window_records[ruleid]['log_list'].append(read_timestamp)
       
        subject = self.format_alert_variable(self.rules[ruleid]['rule']['title'],log_data)
        message = self.format_alert_variable(self.rules[ruleid]['rule']['message'],log_data)
        notifier_list = self.rules[ruleid]['rule']['notifier']
        to_list = []
        for notifier in notifier_list:
            if notifier.get('1'):
                to_list = notifier['1']
                
        if not to_list:
            to_list = ['690943136@qq.com']
        #to_list = ['690943136@qq.com','1615586522@qq.com']
        send_email(to_list, subject, message)
        self.alert_storage(log_data)
    
    def format_alert_variable(self,variable_str,log_data):
        '''
        变量识别, 格式化成字符串
        实现使用正则表达式
        '''
        pattern = re.compile('\${[\w\d]+}')
        variable_list = pattern.findall(variable_str)
        for variable in variable_list:
            key = variable[2:-1]
            value = log_data[key]
            new_variable = '\{}'.format(variable)
            new_variable_str = re.sub(new_variable,value,variable_str)
            variable_str = new_variable_str
        
        return variable_str
    
    def start_alert_process(self):
        alert_process = threading.Thread(target=self.alert_process_thread)
        alert_process.setDaemon(True)
        alert_process.start()
    