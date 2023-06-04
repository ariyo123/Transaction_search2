import os
from unittest import result
import csv
import datetime
import time
import mysql.connector
import threading
from config import mysql_conn1,mysql_conn2,mysql_conn3,mysql_conn4
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


path1='/Transactions_search/session_ids.csv'
with open(path1, 'r') as file_object:
    lines=file_object.read()
        #print(lines)
    session_ids=lines.split()
    #print(f"' + {banks} + '", sep="','" )
    print(session_ids)
    fieldnames=['session_id','transaction_date','response_code','amount']
    fieldnames4=['session_id','request_time','response_code','amount']
    p=str([str(x) for x in session_ids]).strip("[]")
    print(p)
    

# Define your queries
postgres_query = "SELECT * FROM mytable"
mysql_query1 = f"SELECT session_id, transaction_date, response_code, amount FROM nip_report51.`transaction` where session_id in ({p});"
mysql_query2 = f"SELECT session_id, transaction_date, response_code, amount FROM nip_report50.`transaction` where session_id in ({p});"
mysql_query3 = f"SELECT session_id, transaction_date, response_code, amount FROM nip_report47.`transaction` where session_id in ({p});"
mysql_query4 = f"SELECT session_id, request_time, response_code, amount FROM nip_settlement.`tbl_fund_transfer_credits` where session_id in ({p});"

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
def run_mysql_query1(): # type: ignore
    mysql_db = mysql.connector.connect(**mysql_conn1)
    mysql_cursor = mysql_db.cursor()
    mysql_cursor.execute(mysql_query1)
    results = mysql_cursor.fetchall()
    print("MySQL results:", results)
    data=str(f"-----{mysql_conn1['database']}-----")
    
    
    with open(f"/Transactions_search/Result/result_{mysql_conn1['database']}.csv", 'w', newline = '') as csvfile:
        
        my_writer = csv.writer(csvfile, delimiter = ' ')
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
    mysql_db = mysql.connector.connect(**mysql_conn2)
    mysql_cursor = mysql_db.cursor()
    mysql_cursor.execute(mysql_query2)
    results = mysql_cursor.fetchall()
    print("MySQL results:", results)
    data=str(f"-----{mysql_conn2['database']}-----")
    with open(f"/Transactions_search/Result/result_{mysql_conn2['database']}.csv", 'w', newline = '') as csvfile:
        
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
def run_mysql_query3():
    mysql_db = mysql.connector.connect(**mysql_conn3)
    mysql_cursor = mysql_db.cursor()
    mysql_cursor.execute(mysql_query3)
    results = mysql_cursor.fetchall()
    print("MySQL results:", results)
    data=str(f"-----{mysql_conn3['database']}-----")
    
    
    with open(f"/Transactions_search/Result/result_{mysql_conn3['database']}.csv", 'w', newline = '') as csvfile:
        
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

# Function to execute MySQL query in a thread
def run_mysql_query4():
    mysql_db = mysql.connector.connect(**mysql_conn4)
    mysql_cursor = mysql_db.cursor()
    mysql_cursor.execute(mysql_query4)
    results = mysql_cursor.fetchall()
    print("MySQL results:", results)
    data=str(f"-----{mysql_conn4['database']}-----")
    
    
    with open(f"/Transactions_search/Result/result_{mysql_conn4['database']}.csv", 'w', newline = '') as csvfile:
        
        my_writer = csv.writer(csvfile, delimiter = ' ')
        my_writer.writerow(data)
        my_writer.writerow(fieldnames4)
        
            
        
        
    # loop through the rows'
        for row in results:
            print(row)
            #print("\n")
            my_writer.writerow(row)
    mysql_cursor.close()
    mysql_db.close()

# Create threads for each database
#postgres_thread = threading.Thread(target=run_postgres_query)
mysql_thread1 = threading.Thread(target=run_mysql_query1)
mysql_thread2 = threading.Thread(target=run_mysql_query2)
mysql_thread3 = threading.Thread(target=run_mysql_query3)
mysql_thread4 = threading.Thread(target=run_mysql_query4)

# Start the threads
#postgres_thread.start()
mysql_thread1.start()
mysql_thread2.start()
mysql_thread3.start()
mysql_thread4.start()


# Wait for the threads to finish
#postgres_thread.join()
mysql_thread1.join()
mysql_thread2.join()
mysql_thread3.join()
mysql_thread4.join()
   