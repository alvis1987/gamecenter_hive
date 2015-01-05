#!/usr/bin/env python
# -*- coding: utf-8 -*-
# hive util with hive server2
"""
@author:alvis
@create:2014-10-08 10:55
"""

__author__ = 'alvis'
__version__ = '0.1'

import pyhs2
import datetime
import MySQLdb
import time
import sys

def str_date(dt_str):
    dt = datetime.datetime.strptime(dt_str ,'%Y-%m-%d').date()
    return dt

def before_target_date(dt_str):
    time_delta = datetime.timedelta(days = 1)
    dt = str_date(dt_str)
    before_1 = str(dt  - time_delta)
    return before_1

class MysqlClient:
    def __init__(self):
        """
        create connection to mysql
        """
        self.conn = MySQLdb.connect(host="192.168.16.74",
                                  port=3306,
                                  user="panweike",
                                  passwd="test",
                                  db="test"
                                  )
        self.cursor = self.conn.cursor()

    def query(self, sql):
        """
        query
        """
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result

    def initial_tables():
        pass

    def close(self):
        """
        close connection
        """
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

class HiveClient:
    def __init__(self, db_host, user, password, database, port=10000, authMechanism="PLAIN"):
        """
        create connection to hive server2
        """
        self.conn = pyhs2.connect(host=db_host,
                                  port=port,
                                  authMechanism=authMechanism,
                                  user=user,
                                  password=password,
                                  database=database,
                                  )

    def query(self, sql):
        """
        query
        """
        print "task starting ...........\n"
        start = time.time()

        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            """
            print the query cost time in actual
            """
            end = time.time()
            print "task finished! task cost time %s"%(end - start)
            return cursor.fetch()



    def close(self):
        """
        close connection
        """
        self.conn.close()

def theme_active(target_date , before_1):
    hive_client = HiveClient(db_host='192.168.2.183', port=10000, user='root', password='mypass',
                             database='default', authMechanism='PLAIN')
    add_gc_partition_sql = """
                            alter table gamecenter add if not exists partition (dt = '%s')
                            location '/gamecenter/2014/07/28'
                           """%target_date
    filter_gc_sql = """
                    insert overwrite table log.filter_gc
                    select action , type , ip , lag , req_date, pstr
                    from log.gamecenter
                    where type != "HEADER" and action != "None"
                    and action != "detectGamesUpdate" and dt='%s'
                    """%target_date

    update_sql = """
                from(
                from
                    log.filter_gc
                select
                    pstr["imei"] as identity  ,
                    pstr["model"] as model ,
                    "1" as check,
                    min(req_date) as first_login,
                    max(req_date) as  last_login,
                    concat("&" , count(*)) as login_trace,
                    concat("&" , max(pstr["appVersion"])) as version_trace
                where
                    pstr["imei"] != "null" and  pstr["model"] != "null" and  pstr["imei"] != "" and pstr["model"] != ""
                group by
                    pstr["imei"], pstr["model"]
                union all
                select
                    identity,
                    model,
                    "2" as check,
                    first_login,
                    last_login,
                    login_trace,
                    version_trace
                from
                    theme.active
                where
                    dt="%s" and  bussiness="gamecenter"
                )t1
                insert
                    overwrite table theme.active partition (dt="%s" , bussiness = "gamecenter")
                select
                    identity,
                    model,
                    min(first_login) as first_login,
                    max(last_login) as last_login,
                    if(count(*)==2 ,
                        concat(max(login_trace) , min(login_trace)),
                        if (max(check)=="2",
                            concat(max(login_trace) , "&miss"),
                            split(max(login_trace) , "&")[1])
                            ) as login_trace,
                    if(count(*)==2 ,
                        concat(max(version_trace) ,  min(version_trace)),
                        if (max(check)=="2",
                            concat(max(version_trace) , "&miss"),
                            split(max(version_trace),"&")[1])
                            ) as version_trace
                where
                    login_trace != "null" and version_trace != ""
                group by
                    identity , model
                """%(before_1 , target_date)

    hive_client.query("use log")
    hive_client.query(add_gc_partition_sql)
    hive_client.query(filter_gc_sql)
    hive_client.query(update_sql)
    hive_client.close()



def main(target_date):
    """
    main process
    @rtype:
    @return:
    @note:

    """
    before_1 = before_target_date(target_date)
    theme_active(target_date , before_1)


if __name__ == '__main__':
    target_date = sys.argv[1]
    main(target_date)
