#!/usr/bin/env python
#-*- coding: utf8 -*-
#date: 2020-01-03
#author: lianghang
#comment: 监控mycat
import MySQLdb
import time,re
import sys,os
import MySQLdb.cursors
import urllib
import urllib2
import requests,socket,json,ast
from collections import Counter

class MyCATMonitor():
        def __init__(self, host, port, user, passwd, db):
                self.host = host
                self.port = port
                self.user = user
                self.passwd = passwd
                self.db = db
                self.conn = MySQLdb.connect(host=self.host,port=self.port,user=self.user,passwd=self.passwd,db=self.db,cursorclass=MySQLdb.cursors.DictCursor)
                self.cursor = self.conn.cursor()
                self.monitorValues = sys.argv[1].upper()

        def sqlQuery(self,sql):
                self.cursor.execute(sql)
                return self.cursor.fetchall()

        def closeDB(self):
                self.cursor.close()
                self.conn.close()

        def getDirectMemory(self,sql):
            key = self.monitorValues
            data = self.sqlQuery(sql)
            for i in range(len(data)):
                print(data[i])
                if data[i][key].endswith('GB'):
                   print(int(data[i][key].split('GB')[0]))
                   print(int(data[i][key].split('GB')[0]) * 1024 * 1024)
                elif data[i][key].endswith('MB'):
                   print(int(data[i][key].split('MB')[0]) * 1024)
                elif data[i][key].endswith('KB'):
                   print(int(data[i][key].split('KB')[0]))

        def getCache(self,sql):
            data = self.sqlQuery(sql)
            for i in range(len(data)):
                CacheName = sys.argv[1].split('_')[0]
                CacheKey = sys.argv[1].split('_')[1]
                # if data[i]['CACHE'] == 'SQLRouteCache':
                if data[i]['CACHE'] == CacheName:
                    print(data[i][CacheKey])

        def getDataSource(self,sql):
            dataBase = sys.argv[1].split('_',2)[0]
            dataNodeSql = "show @@datanode where schema = %s;" %dataBase
            datanode = self.sqlQuery(dataNodeSql)[0]['NAME']
            data = self.sqlQuery(sql)
            dataName = sys.argv[1].split('_',2)[1]
            dataItem = sys.argv[1].split('_',2)[2]
            for i in range(len(data)):
                if data[i]['DATANODE'] == datanode:
                    if data[i]['NAME'] == dataName:
                        print(int(data[i][dataItem]))

        def getProcessor(self,sql):
            dataProcessorName = sys.argv[1][:sys.argv[1].index('_')]
            dataProcessor = sys.argv[1][sys.argv[1].index('_')+1:]
            data = self.sqlQuery(sql)
            for i in range(len(data)):
                if data[i]['NAME'] == dataProcessorName:
                   print(data[i][dataProcessor])


        def getDataNode(self,sql):
            dataBaseName = sys.argv[1][:sys.argv[1].rindex("_")]
            dataBaseItem = sys.argv[1][sys.argv[1].rindex("_")+1:]
            dataNodeSql = "show @@datanode where schema = %s;" %dataBaseName
            datanode = self.sqlQuery(dataNodeSql)[0][dataBaseItem]
            print(datanode)

        def getHeartBeat(self,sql):
            data = self.sqlQuery(sql)
            for i in range(len(data)):
                if data[i]['RS_CODE'] != 1:
                    hostname = socket.gethostname()
                    message = """ xxxxxxxx myCat 重要警告 xxxxxxxx \n 故障时间：{0} \n 主机名称：{1} \n MySQL后端IP：{2} \n 状态值：{3} \n MyCAT检测后端DB异常，请检查改数据库状态""".format(
                        time.strftime('%Y-%m-%d %H:%M:%S'), hostname,data[i]['HOST'] ,data[i]['RS_CODE'])
                    # sendWarning(message).getPhone()
                    # sendWarning(message).sendUC()
                    print(message)


        def getBackend(self,sql):
            data = self.sqlQuery(sql)
            for i in range(len(data)):
                print(data[i]['id'])

        def getZabbixTrigger(self):
            ip_cnt = "select substring_index(expression,'>',-1) as yuzhi from functions fu join items it on fu.itemid=it.itemid join triggers tr on tr.triggerid = fu.triggerid  where it.key_='myCat.%s' and it.templateid is NULL;" %sys.argv[1]
            ip_process_cnt = int(zabbixConn.sqlQuery(ip_cnt)[0]['yuzhi'])
            return ip_process_cnt

        def getIpProcessCnt(self,sql):
            list1 = []
            data = self.sqlQuery(sql)
            if data:
                for i in range(len(data)):
                    list1.append(data[i]['HOST'])
                res = int(max(Counter(list1).values()))
                print res
                # zabbixTriggerWarningYuZhi = self.getZabbixTrigger()
                zabbixTriggerWarningYuZhi = 40
                for k,v in Counter(list1).iteritems():
                        if v > zabbixTriggerWarningYuZhi:
                            hostname = socket.gethostname()
                            message = """ xxxxxxxx myCat 重要警告 xxxxxxxx \n 故障时间：{0} \n 主机名称：{1} \n 应用IP：{2} \n IP并发数：{3}""".format(time.strftime('%Y-%m-%d %H:%M:%S'),hostname,k,v)
                            # sendWarning(message).getPhone()
                            # sendWarning(message).sendUC()
                            print(message)
            else:
                print(0)


####配置告警方式
class sendWarning():
    def __init__(self, messager):
            self.message = messager

    def getPhone(self):
            sql = "select sendto from usrgrp s join users_groups u on u.usrgrpid=s.usrgrpid join users p on p.userid=u.userid join media m on m.userid=p.userid where s.name='DBA' and sendto REGEXP('[0-9]+.[0-9]+');"
            phone_info = zabbixConn.sqlQuery(sql)
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


zabbixConn = MyCATMonitor('192.168.5.108', 6033, 'zabbix', 'zabbix@123', 'zabbix')
# MyCATMon = MyCATMonitor('127.0.0.1',9066,'ums_user','ums1qaz2wsx#EDC','ums')
MyCATMon = MyCATMonitor('10.156.70.19',9066,'umsapi','umsapi6gtrt','ums')

"""
Show directmemory=1 or 2
显示直连内存使用情况
"""
Direct_Memory = ["MDIRECT_MEMORY_MAXED","DIRECT_MEMORY_USED","DIRECT_MEMORY_AVAILABLE","DIRECT_MEMORY_RESERVED"]
if sys.argv[1].upper() in Direct_Memory:
   sql = "show @@directmemory=1 or 2;"
   MyCATMon.getDirectMemory(sql)

"""
Show @@connection
显示当前前端客户端连接情况，已经网络流量信息
"""
if sys.argv[1] == 'IP_PROCESS':
    sql = "show @@connection;"
    MyCATMon.getIpProcessCnt(sql)

"""
show @@cache 显示缓存的使用情况，对于性能监控和调优很有价值
MAX为缓存的最大值（记录个数），CUR为当前已经在缓存中的数量，ACESS为缓存读次数，HIT为缓存命中次数，PUT 为写缓存次数，LAST_XX为最后操作时间戳，比较重要的几个参数：CUR：若CUR接近MAX，而PUT大于MAX很多，则表明MAX需要增大，HIT/ACCESS为缓存命中率，这个值越高越好。
"""
CacheList = ['SQLRouteCache_MAX','SQLRouteCache_CUR','SQLRouteCache_HIT','SQLRouteCache_ACCESS','SQLRouteCache_PUT']
if sys.argv[1] in CacheList:
    sql = "show @@cache;"
    MyCATMon.getCache(sql)


"""
Show @@datasource
显示数据源的信息，是否是读写节点等。
"""
dataSourceList = ['ums_hostM1_ACTIVE','ums_hostM1_IDLE','ums_hostM1_EXECUTE','ums_hostM1_READ_LOAD','ums_hostM1_WRITE_LOAD',
'ums_notify_hostM1_ACTIVE','ums_notify_hostM1_IDLE','ums_notify_hostM1_EXECUTE','ums_notify_hostM1_READ_LOAD','ums_notify_hostM1_WRITE_LOAD'
                  ]
if sys.argv[1] in dataSourceList:
    sql = "show @@datasource;"
    MyCATMon.getDataSource(sql)

"""
Show @@processor
显示当前processors的处理情况，包括每个processor的IO吞吐量(NET_IN/NET_OUT)、IO队列的积压情况(R_QUEY/W_QUEUE)，Socket Buffer Pool的使用情况BU_PERCENT为已使用的百分比、BU_WARNS为Socket Buffer Pool不够时，临时创新的新的BUFFER的次数，若百分比经常超过90%并且BU_WARNS>0，则表明BUFFER不够，需要增大，参见性能调优手册。
"""

if "Processor" in sys.argv[1]:
    sql = "show @@processor;"
    MyCATMon.getProcessor(sql)

"""
Show @@datanode
显示数据节点的访问情况，包括每个数据节点当前活动连接数(active),空闲连接数（idle）以及最大连接数(maxCon) size，EXECUTE参数表示从该节点获取连接的次数，次数越多，说明访问该节点越多。
"""
dataNodeList = ["ums_ACTIVE","ums_IDLE","ums_EXECUTE",
                "ums_notify_ACTIVE", "ums_notify_IDLE", "ums_notify_EXECUTE"]
if sys.argv[1] in dataNodeList:
    sql = "show @@datanode;"
    MyCATMon.getDataNode(sql)

"""
Show @@heartbeat
当前后端物理库的心跳检测情况,RS_CODE为1表示心跳正常
"""

"""
Show @@backend
显示后端物理库连接信息，包括当前连接数，端口
"""
if sys.argv[1] == "ff":
    sql = "show @@backend;"
    MyCATMon.getBackend(sql)


if __name__ == '__main__':
    MyCATMon.getHeartBeat("show @@heartbeat;")
