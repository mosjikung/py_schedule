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


def sendLine(txt):
  url = 'https://notify-api.line.me/api/notify'
  token = 'ZE6d4wFQO2qQiSMAMqecrPbj6R3nhj0y1STqOJ6xQ1s'
  headers = {'content-type':'application/x-www-form-urlencoded','Authorization':'Bearer ' + token}
  msg = txt
  requests.post(url,headers=headers,data = {'message':msg})



###########################################
########
# Create 12/04/2022
# Request By Chonlada Suksamer
# SQL By 
# Create By Khanisara.P
########

class CLS_SNAP_BILL_OF_Materials(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    SNAP_BILL_OF_Materials()


def SNAP_BILL_OF_Materials():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_SNAP_BILL_OF_Materials")
  sql =""" select *
           from SNAP_BILL_OF_Materials """

  _filename = r"C:\QVD_DATA\PRO_NYK\SNAP_BILL_OF_Materials.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE SNAP_BILL_OF_Materials.xlsx")
  sendLine("COMPLETE SNAP_BILL_OF_Materials.xlsx")

#############################################

class CLS_SNAP_YARN_NYK(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    SNAP_YARN_NYK()


def SNAP_YARN_NYK():
  my_dsn = cx_Oracle.makedsn("172.16.6.75", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_SNAP_YARN_NYK")
  sql =""" select *
           from SNAP_YARN_RECEIVED """

  _filename = r"C:\QVD_DATA\PRO_NYK\SNAP_YARN_NYK.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE SNAP_YARN_NYK.xlsx")
  sendLine("COMPLETE SNAP_YARN_NYK.xlsx")

#############################################


#############################################

class CLS_SNAP_YARN_NYT(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    SNAP_YARN_NYT()


def SNAP_YARN_NYT():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_SNAP_YARN_NYT")
  sql =""" select *
           from SNAP_YARN_NYT """

  _filename = r"C:\QVD_DATA\PRO_NYK\SNAP_YARN_NYT.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE SNAP_YARN_NYT.xlsx")
  sendLine("COMPLETE SNAP_YARN_NYT.xlsx")

#############################################


threads = []

thread1 = CLS_SNAP_BILL_OF_Materials(); thread1.start(); threads.append(thread1)
thread2 = CLS_SNAP_YARN_NYK(); thread2.start(); threads.append(thread2)
thread3 = CLS_SNAP_YARN_NYT(); thread3.start(); threads.append(thread3)


for t in threads:
    t.join()
print ("COMPLETE")

