'''
Created on Dec 30, 2019

@author: yangxu
'''

from .alert_storage import alert_to_es

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

def data_process_thread(self,ruleid,data_process_queue):
    self.rules[ruleid]['data_process'] = True
    while self.rules[ruleid]['data_process']:
        '''
        1 获取日志
        2 判断告警深度, 如果到组件级别,那么不属于此组件的直接过滤掉
        3 表达式执行
        '''
        log_data = data_process_queue.get()
        if self.rules[ruleid]['rule']['alarm_depth'] ==2:
            if log_data.get('component') and (self.rules[ruleid]['rule']['component'] != log_data['component']):
                continue
        
        expression_result = {}
        for expression in self.rules[ruleid]['rule']['expressions']:
            res = expression_execute(expression, log_data)
            expression_result[expression['expression_id']] = res
        logic_expression = self.rules[ruleid]['rule']['logic_expression']
        
        if logic_expression_execute(logic_expression, expression_result):
            log_data['ruleid'] = ruleid # 为数据打上ruleid
            self.alert_compress_queue.put(log_data)
            '''
            入库需要title,message,level等
            '''
            title_expression = self.rules[ruleid]['rule']['title']
            message_expression = self.rules[ruleid]['rule']['message']
            level = self.rules[ruleid]['rule']['level']
            alert_to_es(level,title_expression,message_expression,log_data) #告警入库
    
    #停止数据处理后的操作        
    del self.dataprocess_queue[self.rules[ruleid]['thread_name']]
    del self.rules[ruleid]
        
def logic_expression_execute(logic_expression,expression_result):
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
        
               
def expression_execute(expression_dict,log_data):
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