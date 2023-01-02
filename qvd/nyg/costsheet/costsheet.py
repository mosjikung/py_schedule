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


class CLS_SO_ORDER_COSTSHEET(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    SO_ORDER_COSTSHEET()


def SO_ORDER_COSTSHEET():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  printttime('SO_ORDER_COSTSHEET Start')

  sql = """SELECT SO_NO,SO_YEAR,OU_CODE,SO_NO_DOC,COST_SHEETID FROM OE_SO """

  df = pd.read_sql_query(sql, conn)

  df.to_excel(
      r'C:\QVD_DATA\COM_GARMENT\NYG\SO_ORDER_COSTSHEET.xlsx', index=False)


  df.to_excel(
      r'C:\QVDatacenter\SCM\GARMENT\NYG\SO_ORDER_COSTSHEET.xlsx', index=False)

  conn.close()

  printttime('SO_ORDER_COSTSHEET Complete')


threads = []

thread1 = CLS_SO_ORDER_COSTSHEET()
thread1.start()
threads.append(thread1)



for t in threads:
    t.join()



