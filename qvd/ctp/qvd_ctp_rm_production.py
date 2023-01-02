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

class CLS_qvd_ctp_rm_production(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    qvd_ctp_rm_production()


def qvd_ctp_rm_production():
  my_dsn = cx_Oracle.makedsn("172.16.6.78", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="ctp", password="misctp",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  printttime('qvd_ctp_rm_production Start')

  sql = """

 SELECT *
 from  ctp_mps_plan_v
 where
  FG_YEAR>=to_char(sysdate,'YYYY')-1

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\QVD_DATA\COM_GARMENT\CTP\qvd_ctp_rm_production.csv', index=False,encoding='utf-8-sig')


  conn.close()

  printttime('qvd_ctp_rm_production Complete')


threads = []

thread1 = CLS_qvd_ctp_rm_production() ;thread1.start() ;threads.append(thread1)
##thread2 = CLS_FC_PO() ;thread2.start() ;threads.append(thread2)


for t in threads:
    t.join()



