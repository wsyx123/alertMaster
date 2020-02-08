'''
Created on Jan 6, 2020

@author: yangxu
'''
from .common import format_alert_variable

def alert_send_email(level,title_expression,message_expression,log_data,notifier_list):
    '''
    告警发送
    日志发送后, 需要把已发送的日志时间戳append 进 slide_window_records
    '''
    ruleid = log_data['ruleid']
    #read_timestamp = log_data['read_timestamp']
    subject = format_alert_variable(title_expression,log_data)
    message = format_alert_variable(message_expression,log_data)
    
    to_list = []
    for notifier in notifier_list:
        if notifier.get('1'):
            to_list = notifier['1']
            
    if not to_list:
        to_list = ['690943136@qq.com']
    #to_list = ['690943136@qq.com','1615586522@qq.com']
    #send_email(to_list, subject, message)
    print('告警发送',log_data['id'])