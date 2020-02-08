'''
Created on Dec 30, 2019

@author: yangxu
'''
import time
import datetime

from .alert_storage import log_delay_alert
from .alert_send import alert_send_email

def alert_process_thread(self):
    while True:
        log_data = self.alert_compress_queue.get()
        alert_compress_process(self,log_data)

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
    level = self.rules[ruleid]['rule']['level']
    read_timestamp = log_data['@timestamp']
    title_expression = self.rules[ruleid]['rule']['title']
    message_expression = self.rules[ruleid]['rule']['message']
    notifier_list = self.rules[ruleid]['rule']['notifier']
    now_timestamp = datetime.datetime.now()
    now_timestamp = int(time.mktime(now_timestamp.timetuple()) * 1000 + now_timestamp.microsecond/1000)
    total_time = self.rules[ruleid]['rule']['total_time']
    #1
    if not self.rules[ruleid]['rule']['compress_enabled']:
        alert_send_email(level,title_expression, message_expression, log_data, notifier_list)
        return
    #2
    log_delay_check = log_is_delay(now_timestamp,total_time,read_timestamp)
    if log_delay_check['status']:
        log_delay_alert(log_delay_check['delay'],log_data)
    else:
        init_slide_window_record(self,ruleid) #滑动窗口初始化
        #3
        if alert_interval_check(self,ruleid,read_timestamp):
            #4,5
            total_result = alert_total_check(self,ruleid,read_timestamp)
            time_result = alert_time_check(self,ruleid)
            if total_result and time_result:
                alert_send_email(level,title_expression, message_expression, log_data, notifier_list)
                self.slide_window_records[ruleid]['log_list'].append(read_timestamp)

def init_slide_window_record(self,ruleid):
    if not self.slide_window_records.get(ruleid):
        self.slide_window_records[ruleid] = {}
        self.slide_window_records[ruleid]['log_list'] = []

def log_is_delay(now_timestamp,total_time,read_timestamp):
    '''
    延迟检查
    '''
    def timestamp_to_humandate(delay_time):
        secon_time = delay_time//1000 # python3 取整 是 '//'
        days = secon_time//86400
        hours = (secon_time%86400)//3600
        minutes = ((secon_time%86400)%3600)//60
        seconds = ((secon_time%86400)%3600)%60
        return '{}天{}小时{}分钟{}秒'.format(days,hours,minutes,seconds)
    
    delay_time = now_timestamp - read_timestamp
    if delay_time > int(total_time)*1000:
        humandate = timestamp_to_humandate(delay_time)
        return {'status':True,'delay':humandate}
    else:
        return {'status':False}

def alert_interval_check(self,ruleid,read_timestamp):
    '''
    告警间隔检查
    '''
    if len(self.slide_window_records[ruleid]['log_list']) > 0:
        last_log_timestamp = self.slide_window_records[ruleid]['log_list'][-1]
        if read_timestamp - last_log_timestamp < self.rules[ruleid]['rule']['interval_time']*1000:
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
        if read_timestamp - oldest_log > self.rules[ruleid]['rule']['total_time']*1000:
            self.slide_window_records[ruleid]['log_list'] = self.slide_window_records[ruleid]['log_list'][1:]
            alert_total_check(self,ruleid, read_timestamp)
        else:
            return False
        

def alert_time_check(self,ruleid):
    '''
    告警时段检查
    '''
    alarm_date = self.rules[ruleid]['rule']['alarm_date']
    time_type = self.rules[ruleid]['rule']['time_type']
    alarm_date_result = alarm_date_check(self,alarm_date)
    time_type_result = time_type_check(self,time_type,ruleid)
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
