#!/usr/bin/env python
# -*- coding: utf-8 -*-
# hive util with hive server2
"""
@author:alvis
@create:2014-10-17 15:14
"""

__author__ = 'alvis'
__version__ = '0.1'

import sys
import datetime
sys.path.append("/home/panweike/Script/gamecenter")
from method_define import *
from hql_define import *


def filter_available(target_date):
    Hql = HQL()
    hive_client = HiveClient(db_host='192.168.2.183', port=10000, user='root', password='mypass',
                             database='default', authMechanism='PLAIN')
    """
    @1.add partition to external table gamecenter
    @2.filter gamecenter data
    """
    year , month , day = date_info(target_date).split(",")
    previous = previous_day(target_date)
    hive_client.query(Hql.uselog)
    hive_client.query(Hql.addgcpart%(target_date , year , month ,day))
    hive_client.query(Hql.insertfilter%(target_date ,target_date))
    hive_client.close()

def theme_update(target_date):
    """
    @1. update the active information
    """
    Hql = HQL()
    hive_client = HiveClient(db_host='192.168.2.183', port=10000, user='root', password='mypass',
                             database='default', authMechanism='PLAIN')
    previous = previous_day(target_date)
    hive_client.query(Hql.uselog)
    hive_client.query(Hql.updateactive%(target_date , previous , target_date))
    hive_client.close()

def insert_active(active_list , target_date):
    """
    @.1insert active data
    @.2update retain data
    """
    mysql_client = MysqlClient()
    #sql define
    init_sql   = "delete from daily_active where dt = '%s'"
    insert_sql = """
                 insert into  daily_active(
                 dt , user_h , start_t , dau , start_n , dnu , dau_old , a_percent)
                 values(%s , %s , %s , %s , %s , %s , %s , %s)"""
    # n -> new user , a -> active user
    update_n_sql = "update daily_active set retain_n_%s = %s  where dt = %s;"
    update_a_sql = "update daily_active set retain_a_%s = %s  where dt = %s;"
    """
    @.1 today_info   当日活跃信息 +  衍生的 去新活跃 和 活跃用户除以总用户
    @.2 n_info       每日更新新用户留存所需数据
    @.3 a_info       每日更新活跃用户留存所需数据
    """
    today_info = active_list[0:6] + [active_list[3] - active_list[5] , float(active_list[3]) / active_list[1]]

    n_info = [[1 , active_list[6]  , previous_n(target_date , 1) ],[ 3 ,  active_list[7] ,previous_n(target_date ,3)],
              [7 , active_list[8]  , previous_n(target_date , 7) ],[ 30,  active_list[9],previous_n(target_date ,30)]]

    a_info = [[1 , active_list[10]  , previous_n(target_date , 1) ],[ 3 ,  active_list[11] ,previous_n(target_date ,3)],
              [7 , active_list[12]  , previous_n(target_date , 7) ],[ 30 ,  active_list[13],previous_n(target_date ,30)]]
    #make sure no record at target_date
    mysql_client.cursor.execute( init_sql%target_date )
    #insert and update
    mysql_client.query(insert_sql , today_info)
    mysql_client.cursor.executemany(update_n_sql , n_info)
    mysql_client.cursor.executemany(update_a_sql , a_info)
    mysql_client.close()

def active_caculate(target_date):
    date_tuple =  times_word(target_date , 15)
    Hql = HQL()
    hive_client = HiveClient(db_host='192.168.2.183', port=10000, user='root', password='mypass',
                             database='default', authMechanism='PLAIN')
    hive_client.query(Hql.dreverse)
    active_list =  hive_client.query(Hql.active%date_tuple)[0]
    insert_active(active_list , target_date)
    hive_client.close()


def insert_dl(dl_list ,target_date):
    mysql_client = MysqlClient()
    # sql_define
    init_sql   = "delete from download where dt = '%s' "
    insert_sql = """insert into download (
                    dt , d , d_id , d_imei , df , df_id , df_imei , du , du_id , du_imei , cl ,
                    cl_id , cl_imei , ins ,ins_id , ins_imei , d_old , d_imei_old , df_id_old , df_imei_old)
                    values(%s , %s , %s , %s , %s , %s , %s , %s , %s , %s , %s , %s , %s ,
                    %s , %s , %s , %s , %s , %s , %s )
    """
    #给下载信息加上日期
    dl_info = [target_date] + dl_list
    mysql_client.cursor.execute(init_sql%target_date)
    mysql_client.query(insert_sql , dl_info)
    mysql_client.close()


def dl_cacalate(target_date):
    date_tuple = times_word(target_date , 3)
    Hql = HQL()
    hive_client = HiveClient(db_host='192.168.2.183', port=10000, user='root', password='mypass',
                             database='default', authMechanism='PLAIN')
    dl_list = hive_client.query(Hql.download%date_tuple)[0]
    insert_dl(dl_list , target_date)
    hive_client.close()

def insert_distribue(spread_list ,target_date):
    mysql_client = MysqlClient()
    # sql_define
    init_sql = "delete from spread where dt = '%s' "
    insert_spread_sql = " insert into spread(dt , hour , start , download) values( %s , %s , %s , %s)"
    mysql_client.cursor.execute(init_sql%target_date)
    mysql_client.querymany(insert_spread_sql , spread_list)
    mysql_client.close()

def time_distribute(target_date):
    date_tuple = times_word(target_date , 2)
    Hql = HQL()
    hive_client = HiveClient(db_host='192.168.2.183', port=10000, user='root', password='mypass',
                             database='default', authMechanism='PLAIN')
    spread_list =  hive_client.query(Hql.spread% date_tuple)
    insert_distribue(spread_list , target_date)
    hive_client.close()

def insert_v_retain(today_info  ,target_date):
    mysql_client = MysqlClient()
    # sql_define
    init_sql = "delete from version_retain  where dt = '%s' "
    insert_sql = "insert into version_retain(dt ,version ,  dau) values (%s , %s , %s)"
    mysql_client.cursor.execute(init_sql%target_date)
    mysql_client.querymany(insert_sql , today_info)
    mysql_client.close()

def update_v_retain( update_info , target_date , datedif):
    mysql_client = MysqlClient()
    # sql_define
    update_sql_temple = "update version_retain set retain_%s = %%s where version = %%s and dt = '%s'"
    update_sql = update_sql_temple%(datedif , previous_n(target_date , datedif))
    mysql_client.cursor.executemany(update_sql , update_info)
    mysql_client.close()


def version_retain(target_date):
    date_tuple=times_word(target_date , 2)
    Hql = HQL()
    hive_client = HiveClient(db_host='192.168.2.183', port=10000, user='root', password='mypass',
                             database='default', authMechanism='PLAIN')

    result = hive_client.query(Hql.version_retain_i%date_tuple)
    # 晒除结果中用户数小于20的版本
    today_info =  filter_list(result , 2 , 20)
    #transform_version 将版本序号修改为真实的版本
    insert_v_retain(transform_version(today_info , 1) , target_date)

    param = custom_tuple(1 , target_date)
    update_info =  transform_version(hive_client.query(Hql.version_retain_u%param) , 1)
    update_v_retain(update_info , target_date , 1)

    param = custom_tuple(3 , target_date)
    update_info =  transform_version(hive_client.query(Hql.version_retain_u%param) , 3)
    update_v_retain(update_info , target_date , 3)

    param = custom_tuple(7 , target_date)
    update_info =  transform_version(hive_client.query(Hql.version_retain_u%param) , 7)
    update_v_retain(update_info , target_date , 7)

    param = custom_tuple(30 , target_date)
    update_info =  transform_version(hive_client.query(Hql.version_retain_u%param) , 30)
    update_v_retain(update_info , target_date , 30)
    hive_client.close()

def insert_zoneretain(result , target_date):
    mysql_client = MysqlClient()
    # sql_define
    init_sql = "delete from zoneretain  where dt = '%s' "
    insert_sql = """
                insert into zoneretain(dt , c_week , p_week ,week_percent , c_month , p_month ,month_percent) values
                (%s , %s ,%s ,%s , %s ,%s ,%s)
    """
    if result[1] == 0:
        format_week_result =  result[0:2] + [0.0]
    else:
        format_week_result =  result[0:2] + [float(result[0])/result[1]]
    if result[3] == 0:
        format_month_result =  result[2:4] + [0.0]
    else:
        format_month_result =  result[2:4] + [float(result[2])/result[3]]
    format_result = [target_date] + format_week_result + format_month_result

    mysql_client.cursor.execute(init_sql%target_date)
    mysql_client.query(insert_sql , format_result)
    mysql_client.close()

def zoneretain(target_date):
    Hql = HQL()
    hive_client = HiveClient(db_host='192.168.2.183', port=10000, user='root', password='mypass',
                             database='default', authMechanism='PLAIN')
    hive_client.query(Hql.dstrcount)
    result =  hive_client.query(Hql.zoneretain%target_date)[0]
    insert_zoneretain(result , target_date)
    hive_client.close()

def insert_month_info(result , target_date):
    mysql_client = MysqlClient()
    init_sql = "delete from month_info  where dt = '%s' "
    insert_sql = """
                insert into month_info(dt , mau , download , du , d_id , df_imei)
                values(%s , %s , %s , %s , %s , %s)
    """
    mysql_client.cursor.execute(init_sql%target_date)
    mysql_client.query(insert_sql , [target_date] + result)
    mysql_client.close()

def month_info(target_date):
    Hql = HQL()
    hive_client = HiveClient(db_host='192.168.2.183', port=10000, user='root', password='mypass',
                             database='default', authMechanism='PLAIN')
    result =  hive_client.query(Hql.month_info%(target_date, previous_n(target_date ,30)))[0]
    insert_month_info(result , target_date)
    hive_client.close()

def insert_login_spread(result , target_date):
    mysql_client = MysqlClient()
    init_sql = "delete from login_spread  where dt = '%s' "
    insert_sql = """
                insert into login_spread(dt , p_1 , p_2 , p_3 , p_4 , P_5 ,  p_6 , p_7 , p_8 , p_9 , P_10 ,
                p_11 , p_12 , p_13 , p_14 , P_15 , p_16  , p_17 , p_18 , p_19 , p_20 , P_21 , p_22,  p_23 ,
                p_24 , p_25 , p_26 , P_27 , p_28 ,  p_29 , p_30)
                values(%s , %s , %s , %s , %s , %s, %s , %s , %s , %s , %s, %s , %s , %s , %s , %s, %s ,
                %s , %s , %s , %s, %s , %s , %s , %s , %s, %s , %s , %s , %s , %s)
                """
    mysql_client.cursor.execute(init_sql%target_date)
    mysql_client.query(insert_sql , [target_date] + result)
    mysql_client.close()

def login_spread(target_date):
    Hql = HQL()
    hive_client = HiveClient(db_host='192.168.2.183', port=10000, user='root', password='mypass',
                             database='default', authMechanism='PLAIN')
    result =  hive_client.query(Hql.login_spread%target_date)[0]
    insert_login_spread(result , target_date)
    hive_client.close()

def main(target_date):
    """
    main process
    """
    logging.info("""
        good morning ! current time  is %s ~
        It is time to work....so job start !
    """%str(datetime.datetime.today()))

    safe_execfunc(filter_available , target_date)
    safe_execfunc(theme_update , target_date)
    safe_execfunc(active_caculate , target_date)
    safe_execfunc(dl_cacalate , target_date)
    safe_execfunc(time_distribute , target_date)
    safe_execfunc(version_retain , target_date)
    safe_execfunc(zoneretain , target_date)
    safe_execfunc(month_info , target_date)
    safe_execfunc(login_spread , target_date)

if __name__ == '__main__':
    target_date = sys.argv[1]
    safe_execfunc(main , target_date)
