'''
Created on Jan 9, 2020

@author: yangxu
'''
import re

def format_alert_variable(variable_str,log_data):
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