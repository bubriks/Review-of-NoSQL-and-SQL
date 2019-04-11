import numpy as np
import pandas as pd

def fillOracle():
    import cx_Oracle
    conn = cx_Oracle.connect('bubri/12345@localhost/orcl.ucn.moc')
    cursor = conn.cursor()
    cursor.execute('Delete from bubri.Info')
    print('deleted')
    #rows = cursor.execute("select count(*) from bubri.info")
    #for user_row in rows:
    #    print(user_row)
    conn.commit()
    cursor.close()
    conn.close()
    
    from sqlalchemy import create_engine
    
    #oracle+cx_oracle://System:12345@localhost:1521/?service_name=orcl.ucn.moc
    engine = create_engine("oracle://bubri:12345@localhost:1521/?service_name=orcl.ucn.moc")
    con = engine.connect()
    print('connected')
    df.to_sql(name='info',con=con,if_exists='replace')
    #statement = 'insert into sys.Info(id, value) values (:2, :3)'
    #for index, row in df.iterrows():
     #   data = (str(index), str(row['value']))
    #    print(data)
     #   cursor.execute(statement, data)

def fillMySql():
    import mysql.connector
    conn = mysql.connector.connect(user='bubri', password='12345',
                                  host='127.0.0.1', database='test',
                                  auth_plugin='mysql_native_password')
    cursor = conn.cursor()
    cursor.execute('Delete from Info')
    conn.commit()
    cursor.close()
    conn.close()
    
    from sqlalchemy import create_engine
    engine = create_engine("mysql://bubri:12345@localhost/test")
    con = engine.connect()
    df.to_sql(name='info',con=con,if_exists='replace', index=False)
    
    #df.to_sql('Info', conn, if_exists='replace')
    #statement = 'insert into Info(id, value) values (%s, %s)'
    #for index, row in df.iterrows():
     #   data = (row['id'], row['value'])
    #    cursor.execute(statement, data)
    #    print(str(index));

def fillMSSql():
    import pyodbc 
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=DESKTOP-4VNJ5M0;'
                          'Database=test;'
                          'Trusted_Connection=yes;')
    cursor = conn.cursor()
    cursor.execute('Delete from Info')
    conn.commit()
    cursor.close()
    conn.close()
    
    from sqlalchemy import create_engine
    engine = create_engine('mssql+pyodbc://sa:12345@DESKTOP-4VNJ5M0/test?Driver={SQL Server}')
    con = engine.connect()
    df.to_sql(name='Info',con=con,if_exists='replace', index=False)
    
    #statement = 'insert into Info(id, value) values (?, ?)'
    #for index, row in df.iterrows():
    #    data = (row['id'], row['value'])
    #    cursor.execute(statement, data)
    #    print(str(index));

def fillCassandra():
    from cassandra.cluster import Cluster
    cluster = Cluster(
        ['127.0.0.1'],
        port=9042)
    session = cluster.connect('test')
    
    session.execute('truncate info')
    
    #query = "insert into info(id, value) values (?, ?)"
    #prepared = session.prepare(query)
    #for index, row in df.iterrows():
    #    session.execute(prepared, (index, row['value']))
    
    #statement = 'insert into info(id, value) values (%s, %s)'
    #for index, row in df.iterrows():
    #    data = (row['id'], row['value'])
    #   session.execute(statement, data)
    #    print(str(index));
    
    #rows = session.execute("select * from info")
    #for user_row in rows:
    #    print(user_row)
    
def fillRedis():
    import redis
    
    r = redis.Redis(
        host='localhost',
        port=6379)
    
    r.flushall()
    
    for index, row in df.iterrows():
        r.set(index, row['value'])
        
    #for key in r.scan_iter():
    #   print(key)

def fillMongo():
    import pymongo
    
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["Info"]
    mycol = mydb["Info"]
    
    mycol.delete_many({})
    
    mycol.insert_many(df.to_dict('records'))
    
    #cursor = mycol.find({})
    #for document in cursor:
    #      print(document)
    
    myclient.close()

df = pd.DataFrame("Test value", index = np.arange(100000000), columns=['value'])
df['id'] = df.index

if __name__ == '__main__':
    #print('Oracle')
    #fillOracle()
    print('MySql')
    fillMySql()
    print('MSSql')
    fillMSSql()
    print('Cassandra')
    fillCassandra()
    print('Redis')
    fillRedis()
    print('Mongo')
    fillMongo()
    print('Done!')
    
    df['id'] = df['id'].astype(str)
    df.to_csv('test.csv', index=False)