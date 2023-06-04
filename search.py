import os
from unittest import result
import csv
import datetime
import time
import mysql.connector
import threading
from config import mysql_conn, mysql_connv
#from mysql.consnector import Error

import smtplib,ssl
from os.path import basename
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

#from datetime import date, timedelta
import time
print("\n\n\n\n")
print("you're about to see the status of your webservices")
#get current time
t = time.localtime()
current_time = time.strftime("%H:%M:%S", t)
print(current_time)

#get cureent date
CurrentDate=datetime.date.today()  
days = datetime.timedelta(30)

new_date = CurrentDate - days
final_date= new_date.strftime('%Y-%m-%d')
#%d is for date  
#%m is for month  
#Y is for Year  
print(final_date) 
CurrentDate1=datetime.date.today()  
days1 = datetime.timedelta(0)

new_date1 = CurrentDate1 - days1
final_date1= new_date1.strftime('%Y-%m-%d')
print(final_date1)


path1='C:/python_work/Transactions_search/session_ids.csv'
with open(path1, 'r') as file_object:
    lines=file_object.read()
        #print(lines)
    session_ids=lines.split()
    #print(f"' + {banks} + '", sep="','" )
    print(session_ids)
    fieldnames=['BVN','first_name','Middle_name','Surname','DOB','Account','Bank']
    p=str([str(x) for x in session_ids]).strip("[]")
    print(p)
    

# Define your queries
postgres_query = "SELECT * FROM mytable"
mysql_query2 = "SELECT distinct(terminal_id) FROM housing_data.transactions_2;"
mysql_query = f"SELECT * FROM housing_data.icad where bank in ({p})"

# # Function to execute PostgreSQL query in a thread
# def run_postgres_query():
#     postgres_db = psycopg2.connect(**postgres_conn)
#     postgres_cursor = postgres_db.cursor()
#     postgres_cursor.execute(postgres_query)
#     results = postgres_cursor.fetchall()
#     print("PostgreSQL results:", results)
#     postgres_cursor.close()
#     postgres_db.close()
# Function to execute MySQL query in a thread
def run_mysql_query(): # type: ignore
    mysql_db = mysql.connector.connect(**mysql_conn)
    mysql_cursor = mysql_db.cursor()
    mysql_cursor.execute(mysql_query)
    results = mysql_cursor.fetchall()
    print("MySQL results:", results)
    data=str(f"-----{mysql_conn['database']}-----").strip(",")
    
    
    with open(f"C:/python_work/Transactions_search/result.csv", 'w', newline = '') as csvfile:
        
        my_writer = csv.writer(csvfile, delimiter = '')
        my_writer.writerow(data)
        my_writer.writerow(fieldnames)
        
            
        
        
    # loop through the rows
        for row in results:
            print(row)
            #print("\n")
            my_writer.writerow(row)
    mysql_cursor.close()
    mysql_db.close()
    
# Function to execute MySQL query in a thread
def run_mysql_query2():
    mysql_db = mysql.connector.connect(**mysql_connv)
    mysql_cursor = mysql_db.cursor()
    mysql_cursor.execute(mysql_query2)
    results = mysql_cursor.fetchall()
    print("MySQL results:", results)
    data=str(f"-----{mysql_connv['database']}-----")
    with open(f"C:/python_work/Transactions_search/result.csv", 'a', newline = '') as csvfile:
        
        my_writer = csv.writer(csvfile, delimiter = ',')
        my_writer.writerow(data)
        
        
            
        
        
    # loop through the rows
        for row in results:
            print(row)
            #print("\n")
            my_writer.writerow(row)
    mysql_cursor.close()
    mysql_db.close()

# Function to execute MySQL query in a thread
def run_mysql_query():
    mysql_db = mysql.connector.connect(**mysql_conn)
    mysql_cursor = mysql_db.cursor()
    mysql_cursor.execute(mysql_query)
    results = mysql_cursor.fetchall()
    print("MySQL results:", results)
    data=str(f"-----{mysql_conn['database']}-----")
    
    
    with open(f"C:/python_work/Transactions_search/result.csv", 'a', newline = '') as csvfile:
        
        my_writer = csv.writer(csvfile, delimiter = ',')
        my_writer.writerow(data)
        my_writer.writerow(fieldnames)
        
            
        
        
    # loop through the rows
        for row in results:
            print(row)
            #print("\n")
            my_writer.writerow(row)
    mysql_cursor.close()
    mysql_db.close()
    


# Create threads for each database
#postgres_thread = threading.Thread(target=run_postgres_query)
mysql_thread = threading.Thread(target=run_mysql_query)
mysql_thread2 = threading.Thread(target=run_mysql_query2)

# Start the threads
#postgres_thread.start()
mysql_thread2.start()
mysql_thread.start()


# Wait for the threads to finish
#postgres_thread.join()
mysql_thread.join()
mysql_thread2.join()
   