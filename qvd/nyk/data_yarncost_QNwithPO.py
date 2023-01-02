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
# 10/02/2022
# Request By Sirada Sekekul
# SQL By SRISUDA.C
# Create By Krisada.R
########
class CLS_DATA_YarnCost_QNwithPO(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_YarnCost_QNwithPO()


def DATA_YarnCost_QNwithPO():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  print("START DATA_YarnCost_QNwithPO")
  sendLine("START DATA_YarnCost_QNwithPO")

  sql ="""SELECT QN_NO,RS_NO, RS_ORDERED_DATE,SO_NO,YARN_ITEM, COLOR_CODE,NVL(QN_YARN_PRICE,QN_YARN_PRICE_ITEM) QN_YARN_PRICE, 
                 QUANTITY,PO_NO, PO_LINE,PO_YARN_PRICE, rs_send_date
          FROM (
          SELECT  H.QN_NO, H.ORDER_NUMBER RS_NO,H.ORDERED_DATE RS_ORDERED_DATE,H.ORA_ORDER_NUMBER SO_NO,
                  R.ITEM_CODE YARN_ITEM, R.COLOR_CODE, R.QUANTITY, R.PO_NO, R.PO_LINE,
                  (SELECT DISTINCT NVL(YARN_PRICE_KG,YARN_PRICE)
                  FROM DFIT_YARN_BOM Y WHERE ORDER_NO=SUBSTR(H.QN_NO,2)
                  AND Y.YARN_ITEM=R.ITEM_CODE
                  AND NVL(Y.TOPDYED_COLOR,'NO-COLOR')=R.COLOR_CODE AND ROWNUM=1) QN_YARN_PRICE,
                  (SELECT DISTINCT NVL(YARN_PRICE_KG,YARN_PRICE)
                  FROM DFIT_YARN_BOM Y WHERE ORDER_NO=SUBSTR(H.QN_NO,2)
                  AND Y.YARN_ITEM=R.ITEM_CODE AND ROWNUM=1) QN_YARN_PRICE_ITEM,
                  (SELECT ITEM_PRICE_K FROM FMIT_PO_DETAIL  PD
                  WHERE PD.PO_NO=R.PO_NO  AND PD.LINE_ID=      R.PO_LINE) PO_YARN_PRICE,
                  (SELECT max(dh.START_STEPS) FROM sf5.DUMMY_CONFIRM_ORDER dh WHERE dh.STEPS_ID = 2 and dh.ORDER_NUMBER=H.ORDER_NUMBER) rs_send_date
          FROM DUMMY_SO_HEADERS H, RSPR_SUMMARY R
          WHERE LTRIM(TO_CHAR(H.ORDER_NUMBER))=R.REQ_NUMBER
          AND QN_NO IS NOT NULL
          )
          ORDER BY 1
             """

  df = pd.read_sql_query(sql, conn)

  # _filename = r"C:\QVD_DATA\COST_SPO\DATA_YarnCost_QNwithPO.xlsx" 
  # df.to_excel(_filename, index=False)

  _filename = r"C:\QVD_DATA\COST_SPO\DATA_YarnCost_QNwithPO.csv"
  df.to_csv(_filename, index=False,encoding='utf-8-sig')

  _filename = r"C:\QVD_DATA\COST\DATA_YarnCost_QNwithPO.csv"
  df.to_csv(_filename, index=False,encoding='utf-8-sig')

  conn.close()
  print("COMPLETE DATA_YarnCost_QNwithPO")
  sendLine("COMPLETE DATA_YarnCost_QNwithPO")

#############################################




threads = []

thread1 = CLS_DATA_YarnCost_QNwithPO();thread1.start();threads.append(thread1)


for t in threads:
    t.join()
print ("COMPLETE")

