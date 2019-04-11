import timeit
import pandas as pd
import numpy as np
df = pd.DataFrame(columns=['name','1','2','3','4','5','6','7','8','9','10'])

lastID = 0
existingId = 0
###############################################################################
import pymongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["Info"]
mycol = mydb["Info"]
    
def mongoCreate():
    global mycol
    global lastID
    lastID = lastID + 1
    values = { "id": lastID, "value": "Test"}
    mycol.insert_one(values)

def mongoRead():
    global mycol
    global existingId
    existingId = existingId + 1
    query = {'id': existingId}
    mycol.find_one(query)

def mongoUpdate():
    global mycol
    global existingId
    existingId = existingId + 1
    query = {'id': existingId}
    values = { "$set": { "value": "Test" } }
    mycol.update_one(query, values)

def mongoDelete():
    global mycol
    global lastID
    lastID = lastID + 1
    query = {'id': lastID}
    mycol.delete_one(query)

###############################################################################
import redis
redisConn = redis.StrictRedis(
    host='localhost',
    port=6379)

def redisCreate():
    global redisConn
    global lastID
    lastID = lastID + 1
    redisConn.set(lastID, "Test")
    
def redisRead():
    global redisConn
    global existingId
    existingId = existingId + 1
    redisConn.get(int(existingId))

def redisUpdate():
    global redisConn
    global existingId
    existingId = existingId + 1
    redisConn.set(int(existingId), "Test")
     
def redisDelete():
    global redisConn
    global lastID
    lastID = lastID + 1
    redisConn.delete(lastID)

###############################################################################
from cassandra.cluster import Cluster
cluster = Cluster(
    ['127.0.0.1'],
    port=9042)
session = cluster.connect('test')

def cassandraCreate():
    global session
    global lastID
    lastID = lastID + 1
    statement = 'insert into info(id, value) values (%s, %s)'
    data = (lastID, "Test")
    session.execute(statement, data)

def cassandraRead():
    global session
    global existingId
    existingId = existingId + 1
    statement = 'SELECT * FROM info where id= %s'
    data = (int(existingId),)
    session.execute(statement,data)

def cassandraUpdate():
    global session
    global existingId
    existingId = existingId + 1
    statement = 'update info set value = %s where id = %s'
    data = ("Test", int(existingId))
    session.execute(statement, data)

def cassandraDelete():
    global session
    global lastID
    lastID = lastID + 1
    statement = 'delete from info where id = %s'
    data = (lastID,)
    session.execute(statement, data)

###############################################################################
import pyodbc 
mssqlConn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DESKTOP-4VNJ5M0;'
                      'Database=test;'
                      'Trusted_Connection=yes;')

def mssqlCreate():
    global mssqlConn
    global lastID
    lastID = lastID + 1
    cursor = mssqlConn.cursor()
    statement = 'insert into Info(id, value) values (?, ?)'
    data = (lastID, "Test")
    cursor.execute(statement, data)
    
    mssqlConn.commit()
    cursor.close()

def mssqlRead():
    global mssqlConn
    global existingId
    existingId = existingId + 1
    cursor = mssqlConn.cursor()
    statement = 'SELECT * FROM Info where id = ?'
    data = (existingId,)
    cursor.execute(statement,data)
    
    cursor.fetchall()
    cursor.close()
    
def mssqlUpdate():
    global mssqlConn
    global existingId
    existingId = existingId + 1
    cursor = mssqlConn.cursor()
    statement = 'update Info set value = ? where id = ?'
    data = ("Test", existingId)
    cursor.execute(statement, data)
    
    mssqlConn.commit()
    cursor.close()
    
def mssqlDelete():
    global mssqlConn
    global lastID
    lastID = lastID + 1
    cursor = mssqlConn.cursor()
    statement = 'delete from Info where id = ?'
    data = (lastID,)
    cursor.execute(statement, data)
    
    mssqlConn.commit()
    cursor.close()

###############################################################################
import mysql.connector
mysqlConn = mysql.connector.connect(user='bubri', password='12345',
                              host='127.0.0.1', database='test',
                              auth_plugin='mysql_native_password') 
def mysqlCreate():
    global mysqlConn
    global lastID
    lastID = lastID + 1
    cursor = mysqlConn.cursor()
    statement = 'insert into Info(id, value) values (%s, %s)'
    data = (lastID, "Test")
    cursor.execute(statement, data)
    
    mysqlConn.commit()
    cursor.close()
    
def mysqlRead():
    global mysqlConn
    global existingId
    existingId = existingId + 1
    cursor = mysqlConn.cursor()
    statement = 'SELECT * FROM Info where id = %s'
    data = (existingId,)
    cursor.execute(statement,data)
    
    cursor.fetchall()
    cursor.close()

def mysqlUpdate():
    global mysqlConn
    global existingId
    existingId = existingId + 1
    cursor = mysqlConn.cursor()
    statement = 'update Info set value = %s where id = %s'
    data = ("Test", existingId)
    cursor.execute(statement, data)
    
    mysqlConn.commit()
    cursor.close()

def mysqlDelete():
    global mysqlConn
    global lastID
    lastID = lastID + 1
    cursor = mysqlConn.cursor()
    statement = 'delete from Info where id = %s'
    data = (lastID,)
    cursor.execute(statement, data)
    
    mysqlConn.commit()
    cursor.close()

###############################################################################
#import cx_Oracle
#conn = cx_Oracle.connect('bubri/12345@localhost/orcl.ucn.moc')

def oracleCreate():
    global conn
    global lastID
    lastID = lastID + 1
    cursor = conn.cursor()
    statement = 'insert into bubri.Info(id, value) values (:2, :3)'
    data = (lastID, "Test")
    cursor.execute(statement, data)
    
    conn.commit()
    cursor.close()

def oracleRead():
    global conn
    global existingId
    existingId = existingId + 1
    cursor = conn.cursor()
    statement = 'SELECT * FROM bubri.Info where id = :1'
    data = (existingId,)
    cursor.execute(statement,data)
    
    cursor.fetchall()
    cursor.close()
    
def oracleUpdate():
    global conn
    global existingId
    existingId = existingId + 1
    cursor = conn.cursor()
    statement = 'update bubri.Info set value = :1 where id = :2'
    data = ("Test", existingId)
    cursor.execute(statement, data)
    
    conn.commit()
    cursor.close()

def oracleDelete():
    global conn
    global lastID
    lastID = lastID + 1
    cursor = conn.cursor()
    statement = 'delete from bubri.Info where id = :id'
    cursor.execute(statement, {'id':lastID})
    
    conn.commit()
    cursor.close()
    
###############################################################################

def reset():
    global lastID
    lastID = 99
    global existingId
    existingId = 1
    
def run(name,number,repeat):
    global df
    
    reset()
    t1 = timeit.Timer(name+"Create()", "from __main__ import "+name+"Create")
    t = t1.repeat(number=number,repeat=repeat)
    t.append(name+" Create")
    df = df.append(pd.Series(t, index=['1','2','3','4','5','6','7','8','9','10','name']), ignore_index=True)
    
    reset()
    t1 = timeit.Timer(name+"Read()", "from __main__ import "+name+"Read")
    t = t1.repeat(number=number,repeat=repeat)
    t.append(name+" Read")
    df = df.append(pd.Series(t, index=['1','2','3','4','5','6','7','8','9','10','name']), ignore_index=True)
    
    reset()
    t1 = timeit.Timer(name+"Update()", "from __main__ import "+name+"Update")
    t = t1.repeat(number=number,repeat=repeat)
    t.append(name+" Update")
    df = df.append(pd.Series(t, index=['1','2','3','4','5','6','7','8','9','10','name']), ignore_index=True)
    
    reset()
    t1 = timeit.Timer(name+"Delete()", "from __main__ import "+name+"Delete")
    t = t1.repeat(number=number,repeat=repeat)
    t.append(name+" Delete")
    df = df.append(pd.Series(t, index=['1','2','3','4','5','6','7','8','9','10','name']), ignore_index=True)
    
if __name__ == '__main__':
    number = 1
    repeat = 10
    print('mongo')
    run('mongo',number,repeat)
    print('redis')
    run('redis',number,repeat)
    print('cassandra')
    run('cassandra',number,repeat)
    print('mssql')
    run('mssql',number,repeat)
    print('mysql')
    run('mysql',number,repeat)
    #print('oracle')
    #run('oracle',number,repeat)
    print("done!")
    df.to_csv('result.csv', index=False)