'''
Created on Jan 11, 2020

@author: yangxu
'''

#API server 端口
apiserver_port = 9001

#日志从kafka读取配置
kafka_config = {
    'bootstrap_servers': '172.16.149.12:9092',
    #'group_id' : '', 不同worker使用 worker_name做group_id
    'auto_offset_reset': 'earliest'
}
#kafka topics
'''
不使用这里的配置，topics也是动态的, 如： 当前读数据只从业务A,当增加新规则后，新规则属于业务B, 如果使用同一个worker处理,
1 停止现在的 read_data 线程
2 重写生成topics， 启动新read_data线程
3 接收数据
'''
kafka_topics = ('topic1','topic2')

#一个worker可以同时订阅几个业务
worker_maximum_businesses = 2
#一个worker可以最多处理的规则数
worker_maximum_rules = 5

#告警写入到elasticsearch 配置
es_hosts = ['172.16.149.10:9200']
alert_save_index = 'alert-save'
alert_send_index = 'alert-send'

# oracle infomation
oracle_host = '10.112.220.86'
oracle_port = 1521
oracle_user = 'dxpt_tda'
oracle_passwd = 'Az123456'
oracle_servicename = 'zyyxdb'
oracle_fields = {}

# 数据存储配置
backend = 'file'
rule_filename = 'data/rule.pk'
worker_filename = 'data/worker.pk'
