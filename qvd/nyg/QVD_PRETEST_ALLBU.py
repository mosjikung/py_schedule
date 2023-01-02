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



########################################### 

class CLS_PRETEST_NYG_QVD(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        PRETEST_NYG_QVD()
        
def PRETEST_NYG_QVD():
  my_dsn = cx_Oracle.makedsn("172.16.9.54",port=1521,sid="prod")
  conn = cx_Oracle.connect(user="RAPPS", password="RAPPS", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  args = ['NYG','FIANYG0001','PRETEST_NYG_QVD.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print("sart  PRETEST_NYG_QVD")
  cursor.execute("""
          SELECT *
          FROM PRETEST_NYG_QVD_V
            """)
  
  _csv = r"C:\QVDatacenter\ACC\MASTER\PRETEST\PRETEST_NYG_QVD.csv"

  with open(_csv, "w", newline='', encoding='utf-8-sig') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  args = ['NYG','FIANYG0001',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()
  print("PRETEST_NYG_QVD Done")  
#############################################
class CLS_PRETEST_GW_QVD(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        PRETEST_GW_QVD()
        
def PRETEST_GW_QVD():
  my_dsn = cx_Oracle.makedsn("172.16.9.54",port=1521,sid="prod")
  conn = cx_Oracle.connect(user="RAPPS", password="RAPPS", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  args = ['GW','FIANYG0001','PRETEST_GW_QVD.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print("sart  PRETEST_GW_QVD")
  cursor.execute("""
          SELECT *
          FROM PRETEST_GRW_QVD_V
            """)
  
  _csv = r"C:\QVDatacenter\ACC\MASTER\PRETEST\PRETEST_GW_QVD.csv"

  with open(_csv, "w", newline='', encoding='utf-8-sig') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  args = ['GW','FIANYG0001',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()
  print("PRETEST_GW_QVD Done")  
#############################################
class CLS_PRETEST_NYV_QVD(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        PRETEST_NYV_QVD()
        
def PRETEST_NYV_QVD():
  my_dsn = cx_Oracle.makedsn("192.168.101.36",port=1521,sid="prod")
  conn = cx_Oracle.connect(user="R12GATE", password="R12GATE", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  args = ['NYV','FIANYG0001','PRETEST_NYV_QVD.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print("sart  PRETEST_NYV_QVD")
  cursor.execute("""
          SELECT *
          FROM PRETEST_NYV_QVD_V
            """)
  
  _csv = r"C:\QVDatacenter\ACC\MASTER\PRETEST\PRETEST_NYV_QVD.csv"

  with open(_csv, "w", newline='', encoding='utf-8-sig') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  args = ['NYV','FIANYG0001',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()
  print("PRETEST_NYV_QVD Done")  
#############################################
class CLS_PRETEST_TRM_QVD(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        PRETEST_TRM_QVD()
        
def PRETEST_TRM_QVD():
  my_dsn = cx_Oracle.makedsn("192.168.110.6",port=1521,sid="ORCL")
  conn = cx_Oracle.connect(user="trm", password="trm", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  args = ['TRM','FIANYG0001','PRETEST_TRM_QVD.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print("sart  PRETEST_TRM_QVD")
  cursor.execute("""
              SELECT *
              FROM tri.PRETEST_TRM_QVD_V@TRM_OLD.WORLD
            """)
  
  _csv = r"C:\QVDatacenter\ACC\MASTER\PRETEST\PRETEST_TRM_QVD.csv"

  with open(_csv, "w", newline='', encoding='utf-8-sig') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  args = ['TRM','FIANYG0001',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()
  print("PRETEST_TRM_QVD Done")  
#############################################
threads = []

thread1 = CLS_PRETEST_NYG_QVD();thread1.start();threads.append(thread1)
thread2 = CLS_PRETEST_GW_QVD();thread2.start();threads.append(thread2)
thread3 = CLS_PRETEST_NYV_QVD();thread3.start();threads.append(thread3)
thread4 = CLS_PRETEST_TRM_QVD();thread4.start();threads.append(thread4)



for t in threads:
    t.join()
print ("COMPLETE")

