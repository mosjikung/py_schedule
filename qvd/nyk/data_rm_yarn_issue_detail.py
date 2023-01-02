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
# 11/04/2022
# Request By Chonlada Suksamer
# SQL By SRISUDA.C
# Create By Krisada.R
########
class CLS_DATA_RM_YARN_ISSUE_DETAIL(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_RM_YARN_ISSUE_DETAIL()


def DATA_RM_YARN_ISSUE_DETAIL():
  my_dsn = cx_Oracle.makedsn("172.16.6.75", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START DATA_YarnCost_QNwithPO")

  sql ="""SELECT REQUESTED_TYPE, GR_TYPE, ISSUED_DATE, RM_JOB_NO, YARN_ITEM, YARN_LOT, BU_ASSET, BU_ISSUED, YARN_TYPE,
                YARN_ROLL, YARN_KGS, YARN_VALUES, LOCATION_CODE, ISSUE_NO, KP_NO, VENDOR_NAME, TARE_QTY,
                GROSS_QTY, REQUEST_REMARK, REQUEST_NO, REQUEST_DATE, REQUEST_WEIGHT 
          FROM DATA_RM_YARN_ISSUE_DETAIL
             """

  df = pd.read_sql_query(sql, conn)

#   _filename = r"C:\QVD_DATA\PRO_NYK\DATA_RM_Yarn_Issue_Detail.csv"
#   df.to_csv(_filename, index=False,encoding='utf-8-sig')

  _filename = r"C:\QVD_DATA\PRO_NYK\DATA_RM_Yarn_Issue_Detail.xlsx"
  df.to_excel(_filename, index=False)

  conn.close()
  print("COMPLETE DATA_RM_Yarn_Issue_Detail")
  sendLine("COMPLETE DATA_RM_Yarn_Issue_Detail")

#############################################




threads = []

thread1 = CLS_DATA_RM_YARN_ISSUE_DETAIL();thread1.start();threads.append(thread1)


for t in threads:
    t.join()
print ("COMPLETE")

