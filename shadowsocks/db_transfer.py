#!/usr/bin/python
# -*- coding: UTF-8 -*-

import logging
import cymysql
import time
import sys
from server_pool import ServerPool
import Config
import random
import string
import os
#确定程序使用的是哪个用户表，主要用于区别收费用户和免费用户
if Config.FREE_USER == 1:
    userdb = "freeuser"
else:
    userdb = "user"
    
class DbTransfer(object):

    instance = None

    def __init__(self):
        self.last_get_transfer = {}

    @staticmethod
    def get_instance():
        if DbTransfer.instance is None:
            DbTransfer.instance = DbTransfer()
        return DbTransfer.instance

    def push_db_all_user(self):
        #更新用户流量到数据库
        last_transfer = self.last_get_transfer
        curr_transfer = ServerPool.get_instance().get_servers_transfer()
        #上次和本次的增量
        dt_transfer = {}
        for id in curr_transfer.keys():
            if id in last_transfer:
                if last_transfer[id][0] == curr_transfer[id][0] and last_transfer[id][1] == curr_transfer[id][1]:
                    continue
                elif curr_transfer[id][0] == 0 and curr_transfer[id][1] == 0:
                    continue
                elif last_transfer[id][0] <= curr_transfer[id][0] and \
                last_transfer[id][1] <= curr_transfer[id][1]:
                    dt_transfer[id] = [curr_transfer[id][0] - last_transfer[id][0],
                                       curr_transfer[id][1] - last_transfer[id][1]]
                else:
                    dt_transfer[id] = [curr_transfer[id][0], curr_transfer[id][1]]
            else:
                if curr_transfer[id][0] == 0 and curr_transfer[id][1] == 0:
                    continue
                dt_transfer[id] = [curr_transfer[id][0], curr_transfer[id][1]]

        self.last_get_transfer = curr_transfer
        query_head = 'UPDATE user'
        query_sub_when = ''
        query_sub_when2 = ''
        query_sub_in = None
        last_time = time.time()
        for id in dt_transfer.keys():
            query_sub_when += ' WHEN %s THEN u+%s' % (id, dt_transfer[id][0])
            query_sub_when2 += ' WHEN %s THEN d+%s' % (id, dt_transfer[id][1])
            if query_sub_in is not None:
                query_sub_in += ',%s' % id
            else:
                query_sub_in = '%s' % id
        if query_sub_when == '':
            return
        query_sql = query_head + ' SET u = CASE port' + query_sub_when + \
                    ' END, d = CASE port' + query_sub_when2 + \
                    ' END, t = ' + str(int(last_time)) + \
                    ' WHERE port IN (%s)' % query_sub_in
        #print query_sql
        conn = cymysql.connect(host=Config.MYSQL_HOST, port=Config.MYSQL_PORT, user=Config.MYSQL_USER,
                               passwd=Config.MYSQL_PASS, db=Config.MYSQL_DB, charset='utf8')
        cur = conn.cursor()
        cur.execute(query_sql)
        cur.close()
        conn.commit()
        conn.close()

   @staticmethod
    def pull_db_all_user():
        #数据库所有用户信息
        conn = cymysql.connect(host=Config.MYSQL_HOST, port=Config.MYSQL_PORT, user=Config.MYSQL_USER,
                               passwd=Config.MYSQL_PASS, db=Config.MYSQL_DB, charset='utf8')
        #取出节点相关限制要求
        cur = conn.cursor()        
        cur.execute("SELECT select_t, user_cid, unpay, node_method, mainid FROM ss_node WHERE id = %s " ,Config.NODE_ID)
        #mainid 预留，通过读取数据库判断这个节点是不是主节点，主节点负责更新过期用户信息
         
        rows = cur.fetchone()
        #取出node_method数据，写入config.json文件中，实现通过数据库控制加密方式
        f=open('config.json','r+')
        flist=f.readlines()
        #读取第8行，获取加密方式
        if flist[8].find(rows[3])==-1: 
          #print flist
          flist[8] = '''    "method":"%s"\n'''  %(rows[3])
          
          #print flist
          f=open('config.json','w+') 
          f.writelines(flist)
        
        select_key = rows[0]
        select_cid = rows[1]
        unpay_key = rows[2]
        if select_key == 1:            
            if unpay_key == 1:                
                nowtime=time.time()
                cur.execute("SELECT port, u, d, transfer_enable, passwd, switch, enable FROM %s WHERE cid > %s AND select_id = %s AND next_pay_date > %s" % (userdb,select_cid, Config.NODE_ID, nowtime))
            else:
                cur.execute("SELECT port, u, d, transfer_enable, passwd, switch, enable FROM %s WHERE cid > %s AND select_id = %s " % (userdb,select_cid, Config.NODE_ID))
        else:
            cur.execute("SELECT port, u, d, transfer_enable, passwd, switch, enable FROM %s WHERE cid > %s " % (userdb,select_cid))
        rows = []
        for r in cur.fetchall():
            rows.append(list(r))
        cur.close()
        conn.close()
        #自动降低欠费用户级别
        if Config.RESET_USER_CID == 1:
            #这个函数取得的是服务器时间，注意时差
            nowtimeh = int(time.strftime("%H", time.localtime()))
            #每天凌晨13点检查更新
            if nowtimeh == 13:
                print "现在已经是13点了，开始清理欠费用户"
                conn = cymysql.connect(host=Config.MYSQL_HOST, port=Config.MYSQL_PORT, user=Config.MYSQL_USER,passwd=Config.MYSQL_PASS, db=Config.MYSQL_DB, charset='utf8')
                cur = conn.cursor()
                cur.execute("SELECT uid, cid FROM user WHERE cid > 2 AND next_pay_date < %s", int(time.time()))
                cidrows = cur.fetchall()
                for cidrow in cidrows:
                    cur.execute("UPDATE user SET cid = 1 WHERE uid = %s ", cidrow[0])
                    m = "uid为"+str(cidrow[0])+"的级别已经被降低"
                    print m
                cur.close()
                conn.commit()
                conn.close()

        #更换免费节点密码
        if Config.RESET_FREEUSER_PASSWD == 1:
            nowweekday = int(time.strftime("%w", time.localtime()))
            #设置周一或周四更新
            if nowweekday == 1 or nowweekday == 4:
                conn = cymysql.connect(host=Config.MYSQL_HOST, port=Config.MYSQL_PORT, user=Config.MYSQL_USER,passwd=Config.MYSQL_PASS, db=Config.MYSQL_DB, charset='utf8')
                cur = conn.cursor()
                cur.execute("SELECT uid, setpasstime FROM freeuser")
                #设置更新频率为3天
                nowtime = int(time.time()-259300)
                frows = cur.fetchall()
                for frow in frows:
                    if frow[1] < nowtime:
                        salt = ''.join(random.sample(string.ascii_letters + string.digits, 8))
                        cur.execute("UPDATE freeuser SET passwd = %s , setpasstime = %s WHERE uid = %s ", (salt, int(time.time()), frow[0]))
                cur.close()
                conn.commit()
                conn.close()
        return rows


    @staticmethod
    def del_server_out_of_bound_safe(rows):
    #停止超流量的服务
    #启动没超流量的服务
        for row in rows:
            if ServerPool.get_instance().server_is_run(row[0]) > 0:
                if row[1] + row[2] >= row[3]:
                    logging.info('db stop server at port [%s]' % (row[0]))
                    ServerPool.get_instance().del_server(row[0])
            elif ServerPool.get_instance().server_run_status(row[0]) is False:
                if row[5] == 1 and row[6] == 1 and  row[1] + row[2] < row[3]:
                    logging.info('db start server at port [%s] pass [%s]' % (row[0], row[4]))
                    ServerPool.get_instance().new_server(row[0], row[4])
    @staticmethod
    def thread_db():
        import socket
        import time
        timeout = 60
        socket.setdefaulttimeout(timeout)
        while True:
            #logging.warn('db loop')
            try:
                DbTransfer.get_instance().push_db_all_user()
                rows = DbTransfer.get_instance().pull_db_all_user()
                DbTransfer.del_server_out_of_bound_safe(rows)
            except Exception as e:
                logging.warn('db thread except:%s' % e)
            finally:
                time.sleep(15)


#SQLData.pull_db_all_user()
#print DbTransfer.get_instance().test()
