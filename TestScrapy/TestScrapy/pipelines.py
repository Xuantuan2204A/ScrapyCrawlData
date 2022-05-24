# -*- coding: utf-8 -*-
import pymysql   #Third party library files used to connect to the database
from scrapy.conf import settings
from datetime import datetime

class MySQLPipeline(object):
    def process_item(self,item,spider):
        db= settings['MYSQL_DB_NAME']  #Call the corresponding data information in settings.py
        host = settings['MYSQL_HOST']
        port = settings['MYSQL_PORT']
        user = settings['MYSQL_USER']
        passwd =settings['MYSQL_PASSWORD']
        db_conn = pymysql.connect(host = host,port = port, db= db,user=user,passwd=passwd,charset ='utf8')
        db_cur = db_conn.cursor()  #Create cursor object to execute SQL statement
        print("Database connection successful")
        content_full = ""
        for content in item['Content']: 
            content_full = content_full + " " + content
        values =(   #This is the value we want to pass into the database
            item['Url'],
            item['Title'],
            item['Intro'],
            content_full,
            item['Createdate'],
        )
        try:
            sql = 'INSERT INTO new VALUES (%s,%s,%s,%s,%s)'  #SQL statement
            db_cur.execute(sql,values)   #Execute with execute
            print("Data inserted successfully")  
        except Exception as e:
            print('Insert error:', e)
            db_conn.rollback()
        else:
            db_conn.commit()  #Every time you insert it, you commit it, and the data is saved
        db_cur.close()  
        return item