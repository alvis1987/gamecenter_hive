import pyhs2
import datetime
import MySQLdb
import time
import sys
import logging
#for test 
logger = logging.getLogger('mylogger')
logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                filename='/home/panweike/gamecenter.log'
                )

def str_date(dt_str):
    dt = datetime.datetime.strptime(dt_str ,'%Y-%m-%d').date()
    return dt

def date_info(dt_str):
    dt = str_date(dt_str)
    return str(dt.year) + "," + str(dt.month) + "," + str(dt.day)

def previous_day(dt_str):
    time_delta = datetime.timedelta(days = 1)
    dt = str_date(dt_str)
    previous = str(dt  - time_delta)
    return previous

def previous_n(dt_str  , n):
    time_delta = datetime.timedelta(days = n)
    dt = str_date(dt_str)
    previous = str(dt  - time_delta)
    return previous

def custom_tuple(n , target_date):
    L = [n , n , n , target_date , n , n]
    return tuple(L)

def times_word(word , times):
    return tuple(((word + "*")*times).split("*")[0:times])

def alias_version(name):
    version_dic = {"1":"1.0" , "2":"1.01" ,"3":"1.02" ,"6":"1.1" , "10":"1.2" , "15":"1.3" , "16":"1.3.1"}
    if name in version_dic:
        return version_dic[name]
    else:
        return name

def filter_list(L , indexnum , threshold):
    result = []
    for item in L:
        if item[indexnum] > threshold:
            result.append(item)
    return result

def transform_version(testlist , indexnum):
    L = testlist
    for item in L:
        item[indexnum] = alias_version(item[indexnum])
    return L


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

    def query(self, sql , data):
        """
        query
        """
        self.cursor.execute(sql , data)

    def querymany(self, sql , data):
        """
        query
        """
        self.cursor.executemany(sql , data)

    def close(self):
        """
        close connection
        """
        self.conn.commit()
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
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetch()

    def close(self):
        """
        close connection
        """
        self.conn.close()

def safe_execfunc(func_name , target_date):
    start  = time.time()
    try:
        func_name(target_date)
    except Exception,e:
        logging.error("task %s failed , caused by : %s "%(func_name.__name__ , e))
    logging.info("task %s sucessful , time cost : %d" % (func_name.__name__ , time.time() - start))
