#!/usr/bin/env python
#-*- coding: utf8 -*-
#date: 2020-03-02
#author: lianghang
#comment: 监控keepalvied master状态，切换短信告警
import MySQLdb
import time,re
import sys,os,logging
import MySQLdb.cursors
import urllib
import urllib2
import requests,socket,json,ast
from collections import Counter
import socket
import fcntl
import struct



ErrorCnt = 0

class MyLogging:
    '''
    logging的初始化操作，以类封装的形式进行
    '''
    def __init__(self):
        '''
        '''
        timestr = "keepalived_vip_change"+"_"+time.strftime('%Y_%m_%d')
        lib_path = "/var/log/keepalived"
        if os.path.isdir(lib_path) == False:
            os.system("mkdir /var/log/keepalived/")

        filename = lib_path + '/' + timestr + '.log'  # 日志文件的地址
        self.logger = logging.getLogger()  # 定义对应的程序模块名name，默认为root
        self.logger.setLevel(logging.INFO)  # 必须设置，这里如果不显示设置，默认过滤掉warning之前的所有级别的信息

        fh = logging.FileHandler(filename=filename)  # 向文件filename输出日志信息
        fh.setLevel(logging.INFO)  # 设置日志等级

        # 设置格式对象
        formatter = logging.Formatter(
            "%(asctime)s %(filename)s[line:%(lineno)d]%(levelname)s - %(message)s")  # 定义日志输出格式

        # 设置handler的格式对象
        fh.setFormatter(formatter)

        # 将handler增加到logger中
        self.logger.addHandler(fh)


class mysqlConn():
    def __init__(self, host, port, user, passwd, db):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd, db=self.db,
                                    cursorclass=MySQLdb.cursors.DictCursor)
        self.cursor = self.conn.cursor()

    def sqlQuery(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def closeDB(self):
        self.cursor.close()
        self.conn.close()


####配置告警方式
class sendWarning():
    def __init__(self, messager):
            self.message = messager

    def getPhone(self):
            sql = "select sendto from usrgrp s join users_groups u on u.usrgrpid=s.usrgrpid join users p on p.userid=u.userid join media m on m.userid=p.userid where s.name='DBA' and sendto REGEXP('[0-9]+.[0-9]+');"
            phone_info = zabbixConn.sqlQuery(sql)
            self.sendUC()
            for i in range(len(phone_info)):
                    self.sendSMS(phone_info[i]['sendto'])

    def sendSMS(self, phone):
            url = "http://192.168.5.108:8802/get_user?phone=" + phone + "&message=" + self.message
            data = requests.get(url).json

    def sendUC(self):
            sql = "select sendto from usrgrp s join users_groups u on u.usrgrpid=s.usrgrpid join users p on p.userid=u.userid join media m on m.userid=p.userid where s.name='DBA' and sendto REGEXP('[a-z]+.[a-z]+') group by sendto;"
            ucTosData = zabbixConn.sqlQuery(sql)

            url = "http://192.168.5.11:8888/api/v1/sender/bee?action=bee"
            for i in range(len(ucTosData)):
                    ucTos = ucTosData[i]['sendto']
                    flush_data = {
                            "tos": ucTos,
                            "content": self.message,
                            "tag": "cmdb"
                    }
                    requests.post(url, data=flush_data)

class getKeepalivedStatus():
    def __init__(self):
        self.vipAddress = sys.argv[1]
        self.vipEth = ''
        self.ErrorCnt = 0


    #获取网卡下的vip地址
    def get_ip_address(self,ifname):
        s = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        try:
            return socket.inet_ntoa(fcntl.ioctl(
                s.fileno(),
                0x8915,
                struct.pack('256s',ifname[:15])
            )[20:24])
        except IOError:
            return ''

    #判断输入的vip是否在keepalvied中
    def get_keepalvied_vip(self):
        file_name = '/etc/keepalived/keepalived.conf'
        with open(file_name) as file_obj:
            for content in file_obj:
                if self.vipAddress in content:
                    self.vipEth = content.rstrip().split()[-1]
        if self.vipEth == '':
           print("vip地址输入错误或为空，和配置文件不符，请检查配置文件/etc/keepalived/keepalived.conf")

    def get_df(self):
        self.get_keepalvied_vip()
        filename = '/tmp/python_monitor_keepalived.log'
        if self.vipEth != '':
            if self.get_ip_address(self.vipEth) == '':
                hostAddress = self.get_ip_address(self.vipEth.split(':')[0])
                hostname = socket.gethostname()
                ERROR_message = """{0} vip Address: {1} 发生故障切换。请检查配置\n""".format(time.strftime('%Y-%m-%d %H:%M:%S'),self.vipAddress)
                MyLogging.logger.error(ERROR_message)
                messager = """ xxxxxxxx Keepalive 重要警告 xxxxxxxx \n 故障时间：{0} \n 主机名称：{1} \n DB宿主IP：{2} \n VIP Address：{3} \n Messager：发生vip故障切换，请检查！！！""".format(time.strftime('%Y-%m-%d %H:%M:%S'),hostname,hostAddress,self.vipAddress)
                sendWarning(messager).getPhone()
                global ErrorCnt
                ErrorCnt += 1


            else:
                INFO_message = """{0} vip Address: {1} 正常，无需担心\n""".format(time.strftime('%Y-%m-%d %H:%M:%S'),self.vipAddress)
                MyLogging.logger.info(INFO_message)

zabbixConn = mysqlConn('192.168.5.108', 6033, 'zabbix', 'xxxx', 'zabbix')
MyLogging = MyLogging()

if __name__ == "__main__":
    while ErrorCnt <3:
        getKeepalivedStatus().get_df()
        time.sleep(60)
