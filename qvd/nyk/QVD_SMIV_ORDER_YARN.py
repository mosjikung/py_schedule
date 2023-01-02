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

class CLS_SMIV_ORDER_YARN(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    qvd_SMIV_ORDER_YARN()
def qvd_SMIV_ORDER_YARN():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521, sid="nytg")
  conn = cx_Oracle.connect(user="sf5", password="omsf5",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  args = ['NYK','SF5YAN001','QVD_MIV_ORDER_YARN.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  printttime('SMIV_ORDER_YARN Start')

  sql = """
   select *
   from SMIV_ORDER_YARN 

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\Qlikview_Report\NYK\QVD_MIV_ORDER_YARN.csv', index=False,encoding='utf-8-sig')


  args = ['NYK','SF5YAN001',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('SMIV_ORDER_YARN  Complete')
###########################################

###########################################

class CLS_RSYR(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    qvd_RSYR()
def qvd_RSYR():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521, sid="nytg")
  conn = cx_Oracle.connect(user="sf5", password="omsf5",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  args = ['NYK','SF5YAN002','QVD_RSYR.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  printttime('RSYR Start')

  sql = """
   select ora_order_number so_no, ORDER_NUMBER rs_no, YP_ORDER YR_NO,r.po_no,r.po_line,r.item_code,r.color_code, r.UNIT_OF_MEASURE UOM, sum(r.QUANTITY) QUANTITY
   from dummy_so_headers s,
        RSPR_SUMMARY r
   where s.YP_ORDER = r.REQ_NUMBER
     and s.YP_ORDER is not null
     and s.FLOW_STATUS_CODE <> 'CANCEL'
     and nvl(s.CLOSE_FLAG,'N') <> 'C'
   group by ora_order_number, ORDER_NUMBER, YP_ORDER,r.po_no,r.po_line,r.item_code,r.color_code, r.UNIT_OF_MEASURE

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\Qlikview_Report\NYK\QVD_RSYR.csv', index=False,encoding='utf-8-sig')


  args = ['NYK','SF5YAN002',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('RSYR  Complete')
###########################################

###########################################

class CLS_SO_WIPStep(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    qvd_SO_WIPStep()
def qvd_SO_WIPStep():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521, sid="nytg")
  conn = cx_Oracle.connect(user="sf5", password="omsf5",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  args = ['NYK','SF5YAN003','QVD_SO_WIPStep.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  printttime('SO_WIPStep Start')

  sql = """
   SELECT a.so_no,a.LINE_ID,a.ITEM_CODE, 
          GET_MPS_WIP_STEP (A.SCHEDULE_ID,
                         DECODE(NVL(A.SCH_CLOSED,'N'),'Y','Y', NVL(A.GREY_ACTIVE,'N')),
                         DECODE(NVL(A.SCH_CLOSED,'N'),'Y', A.SCH_CLOSED_DATE, A.FABRIC_RECEIVE),
                         DECODE(NVL(A.SCH_CLOSED,'N'),'Y', A.SCH_CLOSED_DATE, NVL(B.NYK_REC_DATE, NVL(B.DYE_START_DATE, NVL(B.DYE_END_DATE,B.NYK_FG_DATE)))),
                         DECODE(NVL(A.SCH_CLOSED,'N'),'Y', 1, DECODE(NVL(B.DYE_START_DATE, NVL(B.DYE_END_DATE, B.NYK_FG_DATE)),NULL,B.NO_BATCH,1)),
                         DECODE(NVL(A.SCH_CLOSED,'N'),'Y', A.SCH_CLOSED_DATE, DECODE(SUBSTR(A.SCHEDULE_ID,1,1),'8',SYSDATE,NVL(B.DYE_START_DATE, NVL(B.DYE_END_DATE, B.NYK_FG_DATE)))),
                         A.SCH_CLOSED) WIP_STEP,
                               SUM(DECODE(GET_MPS_WIP_STEP (A.SCHEDULE_ID,
                               DECODE(NVL(A.SCH_CLOSED,'N'),'Y','Y', NVL(A.GREY_ACTIVE,'N')),
                               DECODE(NVL(A.SCH_CLOSED,'N'),'Y', A.SCH_CLOSED_DATE, A.FABRIC_RECEIVE),
                               DECODE(NVL(A.SCH_CLOSED,'N'),'Y', A.SCH_CLOSED_DATE, NVL(B.NYK_REC_DATE, NVL(B.DYE_START_DATE, NVL(B.DYE_END_DATE,B.NYK_FG_DATE)))),
                               DECODE(NVL(A.SCH_CLOSED,'N'),'Y', 1, DECODE(NVL(B.DYE_START_DATE, NVL(B.DYE_END_DATE, B.NYK_FG_DATE)),NULL,B.NO_BATCH,1)),
                               DECODE(NVL(A.SCH_CLOSED,'N'),'Y', A.SCH_CLOSED_DATE, DECODE(SUBSTR(A.SCHEDULE_ID,1,1),'8',SYSDATE,NVL(B.DYE_START_DATE, NVL(B.DYE_END_DATE, B.NYK_FG_DATE)))),
                               A.SCH_CLOSED) , '1. No Fab', NVL(A.FABRIC_QUANTITY,0), '2. W.Pull' ,NVL(A.FABRIC_QUANTITY,0), '3. W.Fab', NVL(A.FABRIC_QUANTITY,0),
                               '4. W.Batch', NVL(B.NYK_REC_WEIGHT,0), '5. DPS' , NVL(B.NYK_REC_WEIGHT,0),
                               '6. DH WIP', NVL(A.FABRIC_QUANTITY,0) - NVL(B.NYK_FG_WEIGHT,0), NVL(A.FABRIC_QUANTITY,0))) FABRIC_QUANTITY
                            FROM SF5.DFIT_MC_SCHEDULE A ,
                                        SF5.DFIT_MC_SCH_OMNOI B,
                                        SF5.SMIT_SO_HEADER C,
                                        SF5.DFORA_SALE D
                            WHERE  C.CUSTOMER_YEAR >= 2021                                                                                                                               
                            AND A.SCHEDULE_ID = B.SCHEDULE_ID (+)
                            AND C.SO_NO = A.SO_NO
                            AND LENGTH(C.SO_NO)<>11
                            AND C.SO_STATUS NOT LIKE '00%'
                            AND NVL(A.SCH_CLOSED,'N') NOT IN ('C')
                            AND A.NYK_CANCLE_SCH IS NULL
                            AND A.FABRIC_QUANTITY>0
                            AND D.SALE_ID (+) = C.SALE_ID
                            GROUP BY a.so_no,a.LINE_ID, a.item_Code,
                                                   GET_MPS_WIP_STEP (A.SCHEDULE_ID,
                                                                     DECODE(NVL(A.SCH_CLOSED,'N'),'Y','Y', NVL(A.GREY_ACTIVE,'N')),
                                                                     DECODE(NVL(A.SCH_CLOSED,'N'),'Y', A.SCH_CLOSED_DATE, A.FABRIC_RECEIVE),
                                                                     DECODE(NVL(A.SCH_CLOSED,'N'),'Y', A.SCH_CLOSED_DATE, NVL(B.NYK_REC_DATE, NVL(B.DYE_START_DATE, NVL(B.DYE_END_DATE,B.NYK_FG_DATE)))),
                                                                     DECODE(NVL(A.SCH_CLOSED,'N'),'Y', 1, DECODE(NVL(B.DYE_START_DATE, NVL(B.DYE_END_DATE, B.NYK_FG_DATE)),NULL,B.NO_BATCH,1)),
                                                                     DECODE(NVL(A.SCH_CLOSED,'N'),'Y', A.SCH_CLOSED_DATE, DECODE(SUBSTR(A.SCHEDULE_ID,1,1),'8',SYSDATE,NVL(B.DYE_START_DATE, NVL(B.DYE_END_DATE, B.NYK_FG_DATE)))),
                                                                     A.SCH_CLOSED)

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\Qlikview_Report\NYK\QVD_SO_WIPStep.csv', index=False,encoding='utf-8-sig')


  args = ['NYK','SF5YAN003',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('SO_WIPStep  Complete')
###########################################


threads = []

thread1 = CLS_SMIV_ORDER_YARN() ;thread1.start() ;threads.append(thread1)
thread2 = CLS_RSYR() ;thread2.start() ;threads.append(thread2)
thread3 = CLS_SO_WIPStep() ;thread3.start() ;threads.append(thread3)

for t in threads:
    t.join()



