import cx_Oracle
import csv
import os
from pathlib import Path
import requests
from datetime import datetime, timedelta
import threading
import time
import numpy as np
import pandas as pd
import asyncio


oracle_client = "C:\instantclient_19_5"
os.environ["ORACLE_HOME"] = oracle_client
os.environ["PATH"] = oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.TH8TISASCII"

time_start = datetime.now()


def printttime(txt):
  dateTimeObj = datetime.now()
  timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
  now = datetime.now()
  duration = now - time_start
  print(timestampStr + ' ' + str(duration.total_seconds()) + ' ' + txt)

###########################################


class CLS_DAILY_FG_transaction(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    DAILY_FG_transaction()


def DAILY_FG_transaction():
  my_dsn = cx_Oracle.makedsn("172.16.6.82", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  args = ['G1','G1WCSSPO0003','DAILY_FG_ALLBU.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  printttime('DAILY_FG_transaction START')

  sql = """
  select *
  from V_QVD_DAILY_FG_ALLBU

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\QVD_DATA\COM_GARMENT\NYG\DAILY_FG_ALLBU.csv', index=False,encoding='utf-8-sig')

  args = ['G1','G1WCSSPO0003',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('DAILY_FG_transaction Complete')

  ##################################################################################

threads = []

thread1 = CLS_DAILY_FG_transaction() ;thread1.start() ;threads.append(thread1)



for t in threads:
    t.join()



