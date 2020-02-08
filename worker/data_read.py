'''
Created on Dec 30, 2019

@author: yangxu
'''
import json

from kafka import KafkaConsumer
from kafka import KafkaProducer
from config.master import kafka_config

def generate_topics(rules):
    topices = tuple(rule['rule']['business'] for rule in rules.values())
    return topices

def data_read_thread(self):
    '''
    对于时间戳
    1 日志进入kafka有时间戳 13位
    2 告警压缩使用的时间戳是10 位
    3 入库es需要时间戳 13位  ---- 使用日志进入kafka 的时间戳
    '''
    while True:
        topics = generate_topics(self.rules)
        consumer = data_from_kafka(topics,self.workername)
        for record in consumer:
            if self.kafka_consumer_reload:
                break
            timestamp = record.timestamp
            message = json.loads(record.value)
            message['@timestamp'] = timestamp
            for data_queue in self.dataprocess_queue.values():
                data_queue.put(message)
    
def data_to_kafka():
    producer = KafkaProducer(bootstrap_servers=['172.16.149.12:9092'])
    message = {'business':'smart','component':'nginx','alarm_depth':2,
               'hostname':'172.16.149.2','code':502,'message':'威胁攻击可视化平台 log Error 关键字告警测试 !'}
    
    jsonmessage = json.dumps(message)
    producer.send('log-alert', jsonmessage.encode('utf-8'))
    producer.flush(3)
    producer.close()

def data_from_kafka(topics,workername):
    '''
    kafka server.properties 设置
    advertised.listeners=PLAINTEXT://172.16.149.12:9092
    '''
    kafka_config['group_id'] = workername
    try:
        consumer = KafkaConsumer(*topics,**kafka_config)
    except:
        return []
    return consumer