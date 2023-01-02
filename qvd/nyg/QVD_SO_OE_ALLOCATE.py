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
class CLS_SO_OE_ALLOCATE(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    SO_OE_ALLOCATE()

def SO_OE_ALLOCATE():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  args = ['NYG','SCMNYG0002','SO_OE_ALLOCATE.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print("sart  SO_OE_ALLOCATE.csv")
  ##sendLine("START CLS_DATA_BATCH_PRODUCTION_COST")

  sql =""" 
select OU_CODE,SO_YEAR, SO_NO, OU_CUT OU_CUTTING,OU_OWNER OU_OWNER, OU_SEW OU_SEWING,  OE_REMARK
from NYGW.SO_OE_ALLOCATE_NEW a
where
ou_code='N03'
--and so_no='4388'
and PLAN_QTY=(select max(PLAN_QTY)from NYGW.SO_OE_ALLOCATE_NEW where ou_code=a.ou_code and so_year=a.so_year and so_no=a.so_no )
and so_year>=to_char(get_sysdate,'YY')-5
order by ou_code,so_year,so_no

  """

  df = pd.read_sql_query(sql, conn)

  # _filename = r"C:\QVD_DATA\COST_SPO\DATA_BATCH_PRODUCTION_COST.xlsx" 
  # df.to_excel(_filename, index=False)

  _filename = r"C:\QVDatacenter\SCM\GARMENT\NYG\SO_OE_ALLOCATE.csv"
  df.to_csv(_filename,  index=False,encoding='utf-8-sig')
  args = ['NYG','SCMNYG0002',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)

  conn.close()

  print("SO_OE_ALLOCATE Done")
  ##sendLine("COMPLETE CLS_DATA_BATCH_PRODUCTION_COST")

#############################################


threads = []

thread1 = CLS_SO_OE_ALLOCATE();thread1.start();threads.append(thread1)

for t in threads:
    t.join()
print ("COMPLETE")

