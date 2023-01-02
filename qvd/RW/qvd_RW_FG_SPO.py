import cx_Oracle
import csv
import os
from pathlib import Path
import requests
from datetime import datetime
import threading
import time


oracle_client = "C:\instantclient_19_5"
os.environ["ORACLE_HOME"]=oracle_client
os.environ["PATH"]=oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"]="AMERICAN_AMERICA.TH8TISASCII"



########################################### RW FG k' NOK 

class CLS_rw_onhand_fg(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        rw_onhand_fg()
        
def rw_onhand_fg():
  my_dsn = cx_Oracle.makedsn("172.16.6.71",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="rw", password="rw", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  args = ['RW','SPORW00001','rw_onhand_fg.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print("sart  rw_onhand_fg")
  cursor.execute("""
              select * from rw_onhand_fg
  """)
  
  _csv = r"C:\QVD_DATA\COM_GARMENT\RW\rw_onhand_fg.csv"

  with open(_csv, "w", newline='', encoding='utf-8-sig') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  args = ['RW','SPORW00001',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()
  print("rw_onhand_fgDone")  
########################################### RW FG k' NOK 

class CLS_rw_received_fg(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        rw_received_fg()
        
def rw_received_fg():
  my_dsn = cx_Oracle.makedsn("172.16.6.71",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="rw", password="rw", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  args = ['RW','SPORW00002','rw_received_fg.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print("sart  rw_received_fg")
  cursor.execute("""
              select * from rw_received_fg
              where
              receive_date >=to_date(sysdate,'dd/mm/yyyy')-2190
      """)
  
  _csv = r"C:\QVD_DATA\COM_GARMENT\RW\rw_received_fg.csv"

  with open(_csv, "w", newline='', encoding='utf-8-sig') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  args = ['RW','SPORW00002',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()
  print("rw_received_fg Done")  

  ########################################### RW FG k' NOK 

class CLS_rw_issue_fg(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        rw_issue_fg()
        
def rw_issue_fg():
  my_dsn = cx_Oracle.makedsn("172.16.6.71",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="rw", password="rw", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  args = ['RW','SPORW00003','rw_issue_fg.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print("sart  rw_issue_fg")
  cursor.execute("""
            select * 
            from rw_issue_fg
            where
            iss_date >=to_date(sysdate,'dd/mm/yyyy')-2190
  """)
  
  _csv = r"C:\QVD_DATA\COM_GARMENT\RW\rw_issue_fg.csv"

  with open(_csv, "w", newline='', encoding='utf-8-sig') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  args = ['RW','SPORW00003',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()
  print("rw_issue_fg Done")  

  ########################################### RW FG k' NOK 

class CLS_RW_PO_R12(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        RW_PO_R12()
        
def RW_PO_R12():
  my_dsn = cx_Oracle.makedsn("172.16.9.54",port=1521,sid="prod")
  conn = cx_Oracle.connect(user="RAPPS", password="RAPPS", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  ##args = ['RW','SPORW00003','rw_issue_fg.csv']
  ##result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print("sart  RW_PO_R12")
  cursor.execute("""
                  select *   
                  FROM RW_PO_R12_V
                  where
                  po_date >=to_date(sysdate,'dd/mm/yyyy')-2190
  """)
  
  _csv = r"C:\QVD_DATA\COM_GARMENT\RW\RW_PO_R12.csv"

  with open(_csv, "w", newline='', encoding='utf-8-sig') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  ##args = ['RW','SPORW00003',cursor.rowcount]
  ##result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()
  print("RW_PO_R12 Done")  
    ########################################### RW FG k' NOK 

class CLS_RW_SO_R12(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        RW_SO_R12()
        
def RW_SO_R12():
  my_dsn = cx_Oracle.makedsn("172.16.9.54",port=1521,sid="prod")
  conn = cx_Oracle.connect(user="RAPPS", password="RAPPS", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  ##args = ['RW','SPORW00003','rw_issue_fg.csv']
  ##result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print("sart  RW_SO_R12")
  cursor.execute("""
          SELECT  *
          FROM RW_SO_R12_V
          where
          so_date >=to_date(sysdate,'dd/mm/yyyy')-2190
  """)
  
  _csv = r"C:\QVD_DATA\COM_GARMENT\RW\RW_SO_R12.csv"

  with open(_csv, "w", newline='', encoding='utf-8-sig') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  ##args = ['RW','SPORW00003',cursor.rowcount]
  ##result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()
  print("RW_SO_R12 Done")  

#############################################
threads = []

thread1 = CLS_rw_onhand_fg();thread1.start();threads.append(thread1)
thread2 = CLS_rw_received_fg();thread2.start();threads.append(thread2)
thread3 = CLS_rw_issue_fg();thread3.start();threads.append(thread3)
thread4 = CLS_RW_PO_R12();thread4.start();threads.append(thread4)
thread5 = CLS_RW_SO_R12();thread5.start();threads.append(thread5)




for t in threads:
    t.join()
print ("RW FG COMPLETE")

