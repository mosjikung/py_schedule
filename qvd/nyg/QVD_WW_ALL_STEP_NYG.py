import cx_Oracle
import csv
import os
from pathlib import Path
import requests
from datetime import datetime
import threading
import time
import pandas as pd


oracle_client = "C:\instantclient_19_5"
os.environ["ORACLE_HOME"]=oracle_client
os.environ["PATH"]=oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"]="AMERICAN_AMERICA.TH8TISASCII"



###########################################  k'fon scm

class CLS_WW_ALL_STEP_NYG_QVD_ALLBU(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        WW_ALL_STEP_NYG_QVD_ALLBU()
        
def WW_ALL_STEP_NYG_QVD_ALLBU():
  my_dsn = cx_Oracle.makedsn("172.16.6.82",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  args = ['G1','WCSSCM0001','WCS_ALL_STEP_NYG_QVD.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print("sart  WW_ALL_STEP_NYG_QVD")
  sql=("""
        select *
        from QVD_WW_ALL_STEP_NYG
  """)  

  df = pd.read_sql_query(sql, conn)
  _filename = r"\\172.16.0.49\Machine_Soft_System\Data_Management_NYG\Data_Center\WCS_ALL_STEP_NYG_QVD.csv"
  df.to_csv(_filename,  index=False,encoding='utf-8-sig')
  _filename = r"C:\QVDatacenter\SCM\GARMENT\NYG\WCS_ALL_STEP_NYG_QVD.csv"
  df.to_csv(_filename, index=False,encoding='utf-8-sig')    
  args = ['G1','WCSSCM0001',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()
  print("WW_ALL_STEP_NYG_QVD Done")

###########################################  k'fon scm bu GW

class CLS_WW_ALL_STEP_GW_QVD_ALLBU(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        WW_ALL_STEP_GW_QVD_ALLBU()
        
def WW_ALL_STEP_GW_QVD_ALLBU():
  my_dsn = cx_Oracle.makedsn("172.16.6.87",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  args = ['G7','WCSSCM0001','WCS_ALL_STEP_GW_QVD.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print("sart  WW_ALL_STEP_GW_QVD")
  sql=("""
        select *
        from QVD_WW_ALL_STEP_NYG
  """)  

  df = pd.read_sql_query(sql, conn)
  _filename = r"\\172.16.0.49\Machine_Soft_System\Data_Management_NYG\Data_Center\WCS_ALL_STEP_GW_QVD.csv"
  df.to_csv(_filename,  index=False,encoding='utf-8-sig')
  _filename = r"C:\QVDatacenter\SCM\GARMENT\GRW\WCS_ALL_STEP_GW_QVD.csv"
  df.to_csv(_filename, index=False,encoding='utf-8-sig')    
  args = ['G7','WCSSCM0001',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()
  print("WW_ALL_STEP_GW_QVD Done")  
###########################################  k'fon trm

class CLS_WW_ALL_STEP_TRM_QVD_ALLBU(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        WW_ALL_STEP_TRM_QVD_ALLBU()
        
def WW_ALL_STEP_TRM_QVD_ALLBU():
  my_dsn = cx_Oracle.makedsn("192.168.110.6",port=1521,sid="ORCL")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  args = ['G5','WCSSCM0001','WCS_ALL_STEP_TRM_QVD.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print("sart  WW_ALL_STEP_TRM_QVD")
  sql=("""
        select *
        from QVD_WW_ALL_STEP_NYG
  """)  

  df = pd.read_sql_query(sql, conn)
  _filename = r"\\172.16.0.49\Machine_Soft_System\Data_Management_NYG\Data_Center\WCS_ALL_STEP_TRM_QVD.csv"
  df.to_csv(_filename,  index=False,encoding='utf-8-sig')
  _filename = r"C:\QVDatacenter\SCM\GARMENT\TRM\WCS_ALL_STEP_TRM_QVD.csv"
  df.to_csv(_filename, index=False,encoding='utf-8-sig')    
  args = ['G5','WCSSCM0001',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()
  print("WW_ALL_STEP_TRM_QVD Done")  

  ###########################################  k'fon scm bu: NYV

class CLS_WW_ALL_STEP_NYV_QVD_ALLBU(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        WW_ALL_STEP_NYV_QVD_ALLBU()
        
def WW_ALL_STEP_NYV_QVD_ALLBU():
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  args = ['G6','WCSSCM0001','WCS_ALL_STEP_NYV_QVD.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print("sart  WW_ALL_STEP_NYV_QVD")
  sql=("""
        select *
        from QVD_WW_ALL_STEP_NYG
  """)  

  df = pd.read_sql_query(sql, conn)
  _filename = r"\\172.16.0.49\Machine_Soft_System\Data_Management_NYG\Data_Center\WCS_ALL_STEP_NYV_QVD.csv"
  df.to_csv(_filename,  index=False,encoding='utf-8-sig')
  _filename = r"C:\QVDatacenter\SCM\GARMENT\VN\WCS_ALL_STEP_NYV_QVD.csv"
  df.to_csv(_filename, index=False,encoding='utf-8-sig')    
    
  args = ['G6','WCSSCM0001',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()
  print("WW_ALL_STEP_NYV_QVD Done")  



#############################################
threads = []

thread1 = CLS_WW_ALL_STEP_NYG_QVD_ALLBU();thread1.start();threads.append(thread1)
thread2 = CLS_WW_ALL_STEP_GW_QVD_ALLBU();thread2.start();threads.append(thread2)
thread3 = CLS_WW_ALL_STEP_TRM_QVD_ALLBU();thread3.start();threads.append(thread3)
thread4 = CLS_WW_ALL_STEP_NYV_QVD_ALLBU();thread4.start();threads.append(thread4)



for t in threads:
    t.join()
print ("COMPLETE NOW")

