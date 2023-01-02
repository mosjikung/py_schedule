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

class CLS_po_bom_GW(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    qvd_po_bom_GW()


def qvd_po_bom_GW():
  my_dsn = cx_Oracle.makedsn("172.16.6.87", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  printttime('po_bom_GW Start')

  sql = """
SELECT TRUNC(PO_DATE) PO_DATE,PO_NO_DOC,SO_NO_DOC,SUM(QTYBOM)QTYBOMDUMMY,UOMBOM,SUM(POQTY) POQTY,UOMPO,SUM(AMOUNT) AMOUNT,EXC_RATE,CURRENCY_CODE,VEND_ID,VENDOR_NAME,STYLE_CODE,STYLE_REF, GROUP_CODE,GROUP_DESC,ITEM_CODE,ITEMNAME,I_COL
FROM (
SELECT  C.PO_DATE,C.PO_NO_DOC,A.SO_NO_DOC,B.QTY QTYBOM,B.UOM_CODE UOMBOM,D.PRICE,(D.PO_RATIO*B.QTY) POQTY,D.SUOM_CODE UOMPO,((D.PO_RATIO*B.QTY)*D.PRICE) AMOUNT,C.EXC_RATE, C.CURRENCY_CODE,C.VEND_ID,RM.VENDOR_DESC(C.VEND_ID) VENDOR_NAME,A.STYLE_CODE,A.STYLE_REF, B.GROUP_CODE,RM.GROUP_DESC(B.OU_CODE,B.GROUP_CODE) GROUP_DESC, RM.GET_ITEM_CODE(B.I_SEQ) ITEM_CODE, GET_ITEM_NAME(A.OU_CODE,D.ITEM_SEQ) ITEMNAME,B.I_COL
FROM OE_SO A, PO_CS_PART_L B, PO_HEAD C, PO_DETAIL D
WHERE
 B.OU_CODE = C.OU_CODE
AND B.PO_YEAR = C.PO_YEAR
AND B.PO_NO = C.PO_NO
AND C.OU_CODE = D.OU_CODE
AND C.PO_YEAR = D.PO_YEAR
AND C.PO_NO = D.PO_NO
AND B.I_SEQ = D.ITEM_SEQ
AND B.I_COL = D.ITEM_COLOR
AND B.I_SIZE = D.ITEM_SIZE
AND  B.OU_CODE=A.OU_CODE
AND B.SO_YEAR=A.SO_YEAR
AND B.SO_NO=A.SO_NO
AND A.OU_CODE=  'N03'
AND A.SO_STATUS <> 'C'
AND C.PO_STATUS <> 'C'
AND C.PO_RM_GROUP IN  ('DF01','FA01') 
 AND C.PO_RM_GROUP  <>'KS01'
 and c.po_year >=to_char(sysdate,'YY')-5
 AND C.PO_DATE >= TO_DATE('01/01/2012','DD/MM/YYYY')
AND D.DETAIL_STATUS <> 'C'
)
GROUP BY PO_DATE,PO_NO_DOC,SO_NO_DOC,UOMBOM,POQTY,UOMPO,EXC_RATE,CURRENCY_CODE,VEND_ID,VENDOR_NAME,STYLE_CODE,STYLE_REF, GROUP_CODE,GROUP_DESC,ITEM_CODE,ITEMNAME,I_COL


  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\QVD_DATA\COM_GARMENT\GRW\PO_BOM.csv', index=False,encoding='utf-8-sig')


  conn.close()

  printttime('po_bom_GW Complete')
###########################################


threads = []

thread1 = CLS_po_bom_GW() ;thread1.start() ;threads.append(thread1)

for t in threads:
    t.join()



