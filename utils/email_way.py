#!/usr/bin/env python
#_*_ coding:utf-8 _*_
'''
Created on 2018年4月11日

@author: yangxu
'''

import smtplib
from email.utils import formataddr
from email.mime.text import MIMEText
from email.header import Header
import datetime

def send_email(to_list,subject,message):
    #构造MIMEText对象,第一个参数就是邮件正文,第二个参数是MIME的subtype
    # 传入'plain'，最终的MIME就是'text/plain'，最后一定要用utf-8编码保证多语言兼容性。
    msg = MIMEText(message, 'html', 'utf-8')   #message为传入的参数,为发送的消息.
    """msg = MIMEText('<html><body><h1>Hello</h1>' +
    '<p>send by <a href="http://www.python.org">Python</a>...</p>' +
    '</body></html>', 'html', 'utf-8') """
    #标准邮件需要三个头部信息： From, To, 和 Subject。
    msg['From'] = formataddr(["威胁攻击可视化平台",'2414972909@qq.com'])     #显示发件人信息
    #msg['To'] = formataddr(to_pair_list)        #显示收件人信息
    msg['To'] = ','.join(to_list)
    msg['Subject'] = Header(subject)      #定义邮件主题
    msg['date'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        #创建SMTP对象
        # 587  smtp port (TLS)
        # 465 smtp  port (SSL)
#         server = smtplib.SMTP_SSL("smtp.qq.com", 465)
        server = smtplib.SMTP("smtp.qq.com", 587)
        server.ehlo() 
        server.starttls() 
        server.ehlo()
        
        '''
        如果使用  server = smtplib.SMTP("smtp.qq.com", 587) 这种模式，必须设置如下  3行
        server.ehlo() 
        server.starttls() 
        server.ehlo()
        '''
        #set_debuglevel(1)可以打印出和SMTP服务器交互的所有信息
        #server.set_debuglevel(1)
        #login()方法用来登录SMTP服务器
        server.login("2414972909@qq.com","ciujbjvwyqiceafd")
        #sendmail()方法就是发邮件，由于可以一次发给多个人，所以传入一个list，邮件正文是一个str，as_string()把MIMEText对象变成str
        server.sendmail('2414972909@qq.com', to_list, msg.as_string())
        server.quit()
        return {'status':True}
    except Exception as e:
        print(e)
        return {'status':False,'msg':e}
