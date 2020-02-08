'''
Created on Jan 6, 2020

@author: yangxu
'''

from elasticsearch import Elasticsearch
from elasticsearch import helpers
import cx_Oracle

#解决oracle乱码问题
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

from .common import format_alert_variable

def create_index():
    es = Elasticsearch(['172.16.149.10:9200'])
    mapping = {
      "mappings" : {
        "doc" : {
        "properties" : {
        "@timestamp" : {"type" : "date"},
        "level" : {"type" : "integer"}
        }
    }
    }
    }
    es.indices.create('alert-save',body=mapping)

def alert_to_es(level,title_expression,message_expression,log_data):
    '''
    告警入库
    '''
    log_data['level'] = level
    log_data['title'] = format_alert_variable(title_expression, log_data)
    log_data['message'] = format_alert_variable(message_expression, log_data)
    es = Elasticsearch(['172.16.149.10:9200'])
    action = [{"_index": "alert_save",
            "_type": "doc",
            "_source": log_data}]
    helpers.bulk(es, action)
    print('告警写入')
    

def log_delay_alert(delaytime,log_data):
    '''
    日志延迟告警
    '''
    print('日志延迟告警，日志延迟:',delaytime)
    
def alert_to_oracle():
    '''
    1 下载cx_Oracle-7.3.0.tar.gz,oracle-instantclient18.3-basic-18.3.0.0.0-3.x86_64.rpm
    2 安装, 配置vi /etc/ld.so.conf, 加入/usr/lib/oracle/18.3/client64/lib, 运行ldconfig
    '''
    db = cx_Oracle.connect('dxpt_tda/Az123456@10.112.220.86/zyyxdb')
    import datetime
    datetime.datetime.now
    