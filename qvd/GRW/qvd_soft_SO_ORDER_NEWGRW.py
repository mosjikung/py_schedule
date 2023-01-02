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


class CLS_SO_ORDER_NEWGRW(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    SO_ORDER_NEWGRW()


def SO_ORDER_NEWGRW():
  my_dsn = cx_Oracle.makedsn("172.16.6.87", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  args = ['GW','SPONYG0001','SO_ORDER_NEWGRW.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  printttime('SO_ORDER_NEWGRW START')

  sql = """
SELECT GET_CUSTOMER_NAME(H.CUST_CODE) CUSTOMER_NAME,H.CUST_CATE,GMT_TYPE,GET_BRAND_NAME(CUST_CODE,BRAND_CODE,H.OU_CODE) BRAND_NAME
,H.SO_NO_DOC,H.CRE_DATE SO_NO_DATE
,H.ORDER_TYPE,SUB.SUB_NO,
(select DISTINCT order_name  from oe_order_type T where T.active = 'Y'
AND T.order_type=H.order_type) ORDER_NAME,SUB.CUS_PO_NO,
SUB.SHIP_DATE SHIP_DATE_BY_PO,GMT.COL_FC,GMT.SIZE_FC,GMT.PO_ITEM,GMT.QTY,H.SAM,H.SCORE,GMT.PRICE,0  COST
,GMT.QTY*GMT.PRICE SALES_AMOUNT,H.SHIPMENT_DATE SHIP_DATE_BY_ORDER,SUB.SHIP_BY_DETAIL
,(SELECT DEPT_NAME  FROM  DB_DEPT  WHERE NVL(ACTIVE,'N')='Y' AND OU_CODE='N03'
AND DEPT_CODE=H.DEPT_CODE) SALES_TEAM,
H.HVA,sub.EX_RATE,H.STYLE_CODE,H.SEA_CODE SEASON
,(SELECT BILLTO_COUNTRY FROM RT_CUST_BILLTO WHERE CUST_CODE=H.cust_code AND BILLTO_ID=sub.BILLTO_ID) BILL_COUNTRY
,(SELECT DISTINCT SHIPTO_COUNTRY FROM RT_CUST_SHIPTO WHERE CUST_CODE=H.cust_code AND SHIPTO_ID=sub.SHIPTO_ID) SHIP_COUNTRY
,sub.CURRENCY_CODE, CUS_SEASON, (SELECT FOB_FOR_PCS FROM GM_CS_HEADER_GRW_V1 V WHERE COST_SHEET_NO
=H.COST_SHEETID)   EXC_RATE_COSTSHEET,
DECODE(H.SO_STATUS,'C','CANCEL','ACTIVE') OE_STATUS
,(SELECT SAM FROM GM_CS_HEADER_GRW_V1 V WHERE COST_SHEET_NO
=H.COST_SHEETID)   SAM_ORDER_EFF, h.SCORE SAM_ALL_TOTAL , h.SAM  SAM_BUDGET
FROM OE_SO H,OE_SO_SUB SUB,OE_SUB_GMT GMT,OE_COLOR_SO COL,OE_SIZE_SO SIZ
WHERE H.OU_CODE=SUB.OU_CODE
AND H.SO_NO=SUB.SO_NO
AND H.SO_YEAR=SUB.SO_YEAR
AND SUB.SO_NO=GMT.SO_NO
AND SUB.SO_YEAR=GMT.SO_YEAR
AND SUB.OU_CODE=GMT.OU_CODE
AND SUB.SUB_NO=GMT.SUB_NO
AND    GMT.OU_CODE     = COL.OU_CODE
AND    GMT.SO_YEAR     = COL.SO_YEAR
AND    GMT.SO_NO    = COL.SO_NO
AND    GMT.COL_FC =COL.COL_FC
AND    GMT.OU_CODE     = SIZ.OU_CODE
AND    GMT.SO_YEAR     = SIZ.SO_YEAR
AND    GMT.SO_NO    = SIZ.SO_NO
AND    GMT.SIZE_FC =SIZ.SIZE_FC
AND H.OU_CODE='N03'
  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\IT_ONLY\GRW\SO_ORDER_NEWGRW.csv', index=False,encoding='utf-8-sig')

  args = ['GW','SPONYG0001',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('SO_ORDER_NEWGRW Complete')

  ##################################################################################

threads = []

thread1 = CLS_SO_ORDER_NEWGRW() ;thread1.start() ;threads.append(thread1)



for t in threads:
    t.join()



