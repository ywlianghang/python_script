#!/usr/bin/env python
#-*- coding: utf8 -*-
#date: 2019-12-23
#author: lianghang
#comment: 计算proxysql的command
import MySQLdb
import time,re
import sys,os
import MySQLdb.cursors
import urllib
import urllib2
import requests,socket,json,ast

class proxySQLMonitor():
        def __init__(self, host, port, user, passwd, db):
                self.host = host
                self.port = port
                self.user = user
                self.passwd = passwd
                self.db = db
        # def mysql_conn(self):
                self.conn = MySQLdb.connect(host=self.host,port=self.port,user=self.user,passwd=self.passwd,db=self.db,cursorclass=MySQLdb.cursors.DictCursor)
                self.cursor = self.conn.cursor()
                self.monitorValues = sys.argv[1].upper()

        def sqlQuery(self,sql):
                self.cursor.execute(sql)
                return self.cursor.fetchall()

        def closeDB(self):
                self.cursor.close()
                self.conn.close()

        def valueCnt(self,sql,key,value):
            dict1 = {}
            data = self.sqlQuery(sql)
            for i in range(len(data)):
                dict1[data[i][key].upper()] = data[i][value]
                dict1[data[i][key].upper()] = data[i][value]
            return int(dict1[self.monitorValues])

        def valuerDiffer(self,tmpfile,sql,key,value):
            tmpFile = tmpfile
            dict1 = {}
            data = self.sqlQuery(sql)
            v_last = self.valueCnt(sql,key,value)
            if os.path.exists(tmpFile) and os.path.getsize(tmpFile) != 0:
                with open(tmpFile) as f:
                    for line in f.readlines():
                        v_last = int(ast.literal_eval(line)[self.monitorValues])
            time.sleep(1)
            for i in range(len(data)):
                dict1[data[i][key].upper()] = data[i][value]
                dict1[data[i][key].upper()] = data[i][value]
            v_end = int(dict1[self.monitorValues])
            print(v_end - v_last)
            with open(tmpFile, 'w') as f:
                    f.write(json.dumps(dict1))
            self.closeDB()

        #统计ip连接的信息
        def ipProcessCnt(self):
            sql = "select cli_host as source_ip ,user,db,srv_host,srv_port,count(*) as cnt from stats_mysql_processlist where command!= 'Sleep' group by source_ip;"
            ip_cnt = "select substring_index(expression,'>',-1) as yuzhi from functions fu join items it on fu.itemid=it.itemid join triggers tr on tr.triggerid = fu.triggerid  where it.key_='proxySQL.%s' and it.templateid is NULL;" %sys.argv[1]
            data = self.sqlQuery(sql)
            ip_process_cnt = int(zabbixConn.sqlQuery(ip_cnt)[0]['yuzhi'])

            if data:
                res = map(lambda x: int(x['cnt']), data)
                print(int(max(res)))

                for i in range(len(data)):
                    if int(data[i]['cnt']) > ip_process_cnt:
                        hostname = socket.gethostname()
                        message = """ xxxxxxxx ProxySQL 重要警告 xxxxxxxx \n 故障时间：{0} \n 主机名称：{1} \n 应用IP：{2} \n 访问user：{3} \n 访问DB：{4} \n 数据库地址：{5} \n 数据库端口：{6} \n IP并发数：{7}""".format(time.strftime('%Y-%m-%d %H:%M:%S'),hostname,data[i]['source_ip'],data[i]['user'],data[i]['db'],data[i]['srv_host'],data[i]['srv_port'],data[i]['cnt'])
                        sendWarning(message).getPhone()
                        sendWarning(message).sendUC()
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

zabbixConn = proxySQLMonitor('192.168.5.108', 6033, 'zabbix', 'zabbix@123', 'zabbix')
proxySQLMon = proxySQLMonitor('127.0.0.1',6032,'admin','admin!@#','stats')

#监控单个ip并发数，并告警
if sys.argv[1] == "ip_process":
    proxySQLMon.ipProcessCnt()

if sys.argv[1] == "writer_count":
    sql = "select count(1) as write_cnt from runtime_mysql_servers where hostgroup_id = 1;"
    print(int(proxySQLMon.sqlQuery(sql)[0]['write_cnt']))

#监控每秒sql命令执行数
sqlCommCnt = ['ALTER_TABLE','BEGIN','COMMIT','CREATE_DATABASE','CREATE_INDEX','CREATE_TABLE','CREATE_TEMPORARY','DELETE','DROP_DATABASE','DROP_INDEX','DROP_TABLE','DROP_USER','INSERT','KILL','SELECT','SELECT_FOR_UPDATE','START_TRANSACTION','TRUNCATE_TABLE','UNLOCK_TABLES','UPDATE','SHOW']
if sys.argv[1].upper() in sqlCommCnt:
    sql = "select Command,Total_cnt from stats_mysql_commands_counters where Command in ('ALTER_TABLE','BEGIN','COMMIT','CREATE_DATABASE','CREATE_INDEX','CREATE_TABLE','CREATE_TEMPORARY','DELETE','DROP_DATABASE','DROP_INDEX','DROP_TABLE','DROP_USER','INSERT','KILL','SELECT','SELECT_FOR_UPDATE','START_TRANSACTION','TRUNCATE_TABLE','UNLOCK_TABLES','UPDATE','SHOW');"
    proxySQLMon.valuerDiffer('/tmp/proxysql_monitor_file_sqlCommand',sql,'Command','Total_cnt')

#监控proxysql 内存指标
memoryMetricsStats = ["JEMALLOC_RETAINED","JEMALLOC_ACTIVE","SQLITE3_MEMORY_BYTES","STACK_MEMORY_CLUSTER_THREADS","JEMALLOC_RESIDENT","QUERY_DIGEST_MEMORY","STACK_MEMORY_ADMIN_THREADS","JEMALLOC_ALLOCATED","STACK_MEMORY_MYSQL_THREADS","JEMALLOC_MAPPED","AUTH_MEMORY","JEMALLOC_METADATA"]
if sys.argv[1].upper() in memoryMetricsStats:
    sql = "select * from stats_memory_metrics;"
    print(proxySQLMon.valueCnt(sql,'Variable_Name','Variable_Value'))

#监控proxysql全局指标
proxyGlobalStats = ["ACTIVE_TRANSACTIONS","CLIENT_CONNECTIONS_ABORTED",'CLIENT_CONNECTIONS_CONNECTED','CLIENT_CONNECTIONS_CREATED',
'SERVER_CONNECTIONS_ABORTED','SERVER_CONNECTIONS_CONNECTED','SERVER_CONNECTIONS_CREATED','CLIENT_CONNECTIONS_NON_IDLE','BACKEND_QUERY_TIME_NSEC',
'MYSQL_THREAD_WORKERS','MYSQL_MONITOR_WORKERS','CONNPOOL_GET_CONN_SUCCESS','CONNPOOL_GET_CONN_FAILURE','CONNPOOL_GET_CONN_IMMEDIATE',
'QUESTIONS','SLOW_QUERIES','MYHGM_MYCONNPOLL_GET','MYHGM_MYCONNPOLL_GET_OK','MYHGM_MYCONNPOLL_PUSH','MYHGM_MYCONNPOLL_DESTROY','CONNPOOL_MEMORY_BYTES',
'QUERY_CACHE_MEMORY_BYTES','QUERY_CACHE_ENTRIES','QUERY_CACHE_PURGED','QUERY_CACHE_BYTES_IN','QUERY_CACHE_BYTES_OUT','QUERY_CACHE_COUNT_GET',
'QUERY_CACHE_COUNT_GET_OK','QUERY_CACHE_COUNT_SET','QUERY_PROCESSOR_TIME_NSEC']
if sys.argv[1].upper() in proxyGlobalStats:
    sql = "select * from stats_mysql_global where Variable_Name not like 'Stmt%';"
    proxySQLMon.valuerDiffer('/tmp/proxysql_monitor_file_mysqlGlobal',sql, 'Variable_Name', 'Variable_Value')
