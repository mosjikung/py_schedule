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

class CLS_qvd_soft_rm_po_bom_NYV(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    qvd_soft_rm_po_bom_NYV()
def qvd_soft_rm_po_bom_NYV():
  my_dsn = cx_Oracle.makedsn("192.168.101.36", port=1521, sid="prod")
  conn = cx_Oracle.connect(user="R12GATE", password="R12GATE",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  args = ['NYV','PURNYG0002','SUMMARY_RM_PO_BOM_NYV.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  printttime('RM PO BOM NYV Start')

  sql = """
   select BU_CODE "BU",
RM_TYPE "RM Type",
PO_YEAR "PO Year",
PO_NO "PO No",
PO_NO_DOC "PO No Doc",
PO_DATE_SS "PO Date (SS)",
PO_DATE_R12 "PO Date (R12)",
PO_STATUS "PO Status (SS)",
VENDOR_CODE "Vendor Code",
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(VENDOR_NAME, CHR(10)||CHR(13),' '), CHR(9), ' '), CHR(10), ' '), CHR(13), ' '),'"',' '),',','') "Vendor Name",
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CUSTOMER, CHR(10)||CHR(13),' '), CHR(9), ' '), CHR(10), ' '), CHR(13), ' '),'"',' '),',','')  "Customer",
PO_LINE "PO Line",
ITEM_CODE "Item Code",
ITEM_COLOR,
ITEM_SIZE,
UOM_CODE "Primary UOM Code",
PO_BOM_QTY "BOM (Qty)",
PO_BOM_AMOUNT "BOM (Amount)",
PO_QTY_PRICE "PO Price (Qty)",
PO_AMOUNT_PRICE "PO Price (Amount)",
PO_QTY_FREE "PO Free (Qty)",
PO_AMOUNT_FREE "PO Free (Amount)",
PO_REC_QTY_PRICE "Receive PO Price (Qty)",
PO_REC_AMOUNT_PRICE "Receive PO Price (Amount)",
PO_REC_QTY_FREE "Receive PO Free (Qty)",
PO_REC_AMOUNT_FREE "Receive PO Free (Amount)",
TOT_PO_REC_QTY "Total Receive PO (Qty)",
TOT_PO_REC_AMOUNT "Total Receive PO (Amount)",
NET_PROD_QTY "Net Production (Qty)",
NET_PROD_AMOUNT "Net Production (Amount)",
NET_BALANCE_QTY "Bal(TOTRec-NetPro(Qty)",---max30digit
NET_BALANCE_AMOUNT "Bal(TOTRec-NetPro)(AMT)",---max30digit
DECODE(NVL(UNIT_PRICE,0), 0, 0, NVL(NET_PROD_QTY,0)) "NetPro(Qty)POLinePrice",---max30digit
DECODE(NVL(UNIT_PRICE,0), 0, 0, NVL(NET_PROD_AMOUNT,0)) "NetPro(Amount)POLinePrice",---max30digit
DECODE(NVL(UNIT_PRICE,0), 0, NVL(NET_PROD_QTY,0), 0) "NetPro(Qty)POLineFree",---max30digit
DECODE(NVL(UNIT_PRICE,0), 0, NVL(NET_PROD_AMOUNT,0), 0) "NetPro(Amount)POLineFree" , ---max30digit
ISS_SAMPLE_QTY "Issue sample (Qty)", --notsupport thai
ISS_SAMPLE_AMOUNT "Issue sample (Amount)", --notsupport thai
ISS_SALE_QTY "Issue sale (Qty)",--notsupport thai
ISS_SALE_AMOUNT "Issue sale (Amount)",--notsupport thai
ISS_OTH_QTY "Issue Others (Qty)",
ISS_OTH_AMOUNT "Issue Others (Amount)",
BALANCE_QTY "Balance (Qty)",
BALANCE_AMOUNT "Balance (Amount)",
SO_NO_DOC "SO No Doc",
SO_STATUS "SO Status",
PO_TYPE ,
MOQ,
CREATED_BY,
PO_LINE_SS_STATUS,
PO_CREATED_BY,
PO_LINE_TYPE,
REVISE_SO
   from NY_PO_NYG_RM

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\Qlikview_Report\INVENTORY\NYV\SUMMARY_RM_PO_BOM_NYV.csv', index=False,encoding='utf-8-sig')


  args = ['NYV','PURNYG0002',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('RM PO BOM NYV Complete')
###########################################

threads = []

thread1 = CLS_qvd_soft_rm_po_bom_NYV() ;thread1.start() ;threads.append(thread1)



for t in threads:
    t.join()



