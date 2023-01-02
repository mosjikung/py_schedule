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
class CLS_DAILY_ANDON_ALLBU(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DAILY_ANDON_ALLBU()

def DAILY_ANDON_ALLBU():
  my_dsn = cx_Oracle.makedsn("172.16.6.82", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  args = ['G1','G1AUDSPO0001','DAILY_ANDON_ALLBU.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print("start  DAILY_ANDON_ALLBU.csv")
  ##sendLine("START CLS_DATA_BATCH_PRODUCTION_COST")

  sql =""" 
  select *
from AD_QVD_DAILY_V_ALL

  """

  df = pd.read_sql_query(sql, conn)

  # _filename = r"C:\QVD_DATA\COST_SPO\DATA_BATCH_PRODUCTION_COST.xlsx" 
  # df.to_excel(_filename, index=False)

  _filename = r"C:\QVD_DATA\COM_GARMENT\NYG\ANDON\DAILY_ANDON_ALLBU.csv"
  df.to_csv(_filename,  index=False,encoding='utf-8-sig')
  _filename = r"\\172.16.0.49\Machine_Soft_System\Andon\DAILY_ANDON_ALLBU.csv"
  df.to_csv(_filename, index=False,encoding='utf-8-sig')
  args = ['G1','G1AUDSPO0001',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)

  conn.close()

  print("DAILY_ANDON_ALLBU.csv Done")
  ##sendLine("COMPLETE CLS_DATA_BATCH_PRODUCTION_COST")


#############################################


threads = []

thread1 = CLS_DAILY_ANDON_ALLBU();thread1.start();threads.append(thread1)


for t in threads:
    t.join()
print ("COMPLETE")

