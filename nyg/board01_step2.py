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


def sendLineControlNYG(txt):
  url = 'https://notify-api.line.me/api/notify'
  token = 'xNt29CUwvuIQWzQOgmfpYgXY0dF7wcG46cSnQ7H2atF'
  headers = {
          'content-type':
          'application/x-www-form-urlencoded',
          'Authorization':'Bearer '+token
          }
  r = requests.post(url, headers=headers, data={'message': txt})

def printttime(txt):
  dateTimeObj = datetime.now()
  timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
  now = datetime.now()
  duration = now - time_start
  print(timestampStr + ' ' + str(duration.total_seconds()) + ' ' + txt)


class CLS_PPR_NYG(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    PPR_NYG()


def PPR_NYG():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  printttime('PPR NYG Start')

  cursor.execute("""INSERT INTO CONTROL_WIP_READINESS_ALL_TMP
                  SELECT BU, GMT_TYPE, STYLE_REF, SO_NO, SO_YEAR, SUB_NO, SO_NO_DOC, SHIPMENT_DATE, CUST_GROUP, CUST_NAME
                  , BRAND_NAME, EDD_W, ORDER_ID, COLOR_FC, ORDER_QTY, SO_RELEASE, SO_RELEASE_DATE, SAMPLE_RELEASE
                  , SAMPLE_RELEASE_DATE, PATTERN_RELEASE, PATTERN_RELEASE_DATE, FC_RELEASE, FC_RELEASE_DATE
                  , SEW_ACC_COMPLETE, SEW_ACC_COMPLETE_DATE, PACK_ACC_COMPLETE, PACK_ACC_COMPLETE_DATE, FABRIC_COMPLETE
                  , FABRIC_COMPLETE_DATE, SCM_ALLOCATE, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FIRST_CUT_DATE, LOADED_QTY
                  , TO_CHAR(GET_FR_MRDDATE_BY_SO(BU, SO_YEAR,SO_NO,SHIPMENT_DATE),'IYYYIW') MRD_W, SAM, SCORE, 0
                  , TO_NUMBER(GET_READINESS_FIND_TARGET(RDD_W,CASE WHEN ORDER_TYPE = '02' THEN 4 ELSE 5 END)) TARGET_BY_RDD
                  FROM OE_SUB_READINESS_STATUS_NYG M
                  WHERE SHIPMENT_DATE >= TO_DATE('01/01/2020','DD/MM/RRRR') 
                  """)

  conn.commit()
  conn.close()

  printttime('PPR NYG Complete')


class CLS_PPR_TRM(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    PPR_TRM()


def PPR_TRM():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  printttime('PPR TRM Start')

  cursor.execute("""INSERT INTO CONTROL_WIP_READINESS_ALL_TMP
                  SELECT BU, GMT_TYPE, STYLE_REF, SO_NO, SO_YEAR, SUB_NO, SO_NO_DOC, SHIPMENT_DATE, CUST_GROUP, CUST_NAME
                  , BRAND_NAME, EDD_W, ORDER_ID, COLOR_FC, ORDER_QTY, SO_RELEASE, SO_RELEASE_DATE, SAMPLE_RELEASE
                  , SAMPLE_RELEASE_DATE, PATTERN_RELEASE, PATTERN_RELEASE_DATE, FC_RELEASE, FC_RELEASE_DATE
                  , SEW_ACC_COMPLETE, SEW_ACC_COMPLETE_DATE, PACK_ACC_COMPLETE, PACK_ACC_COMPLETE_DATE, FABRIC_COMPLETE
                  , FABRIC_COMPLETE_DATE, SCM_ALLOCATE, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FIRST_CUT_DATE, LOADED_QTY
                  , TO_CHAR(GET_FR_MRDDATE_BY_SO(BU, SO_YEAR,SO_NO,SHIPMENT_DATE),'IYYYIW') MRD_W,  SAM,   SCORE, 0
                  , TO_NUMBER(GET_READINESS_FIND_TARGET(RDD_W,CASE WHEN ORDER_TYPE = '02' THEN 4 ELSE 5 END)) TARGET_BY_RDD
                  FROM OE_SUB_READINESS_STATUS_TRM M
                  WHERE SHIPMENT_DATE >= TO_DATE('01/01/2020','DD/MM/RRRR') 
                   """)

  conn.commit()
  conn.close()

  printttime('PPR TRM Complete')


class CLS_PPR_GRW(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    PPR_GRW()


def PPR_GRW():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  printttime('PPR GRW Start')

  cursor.execute("""INSERT INTO CONTROL_WIP_READINESS_ALL_TMP
                  SELECT BU, GMT_TYPE, STYLE_REF, SO_NO, SO_YEAR, SUB_NO, SO_NO_DOC, SHIPMENT_DATE, CUST_GROUP, CUST_NAME
                  , BRAND_NAME, EDD_W, ORDER_ID, COLOR_FC, ORDER_QTY, SO_RELEASE, SO_RELEASE_DATE, SAMPLE_RELEASE
                  , SAMPLE_RELEASE_DATE, PATTERN_RELEASE, PATTERN_RELEASE_DATE, FC_RELEASE, FC_RELEASE_DATE
                  , SEW_ACC_COMPLETE, SEW_ACC_COMPLETE_DATE, PACK_ACC_COMPLETE, PACK_ACC_COMPLETE_DATE, FABRIC_COMPLETE
                  , FABRIC_COMPLETE_DATE, SCM_ALLOCATE, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FIRST_CUT_DATE, LOADED_QTY
                  , TO_CHAR(GET_FR_MRDDATE_BY_SO(BU, SO_YEAR,SO_NO,SHIPMENT_DATE),'IYYYIW') MRD_W,  SAM,  SCORE, 0
                  , TO_NUMBER(GET_READINESS_FIND_TARGET(RDD_W,CASE WHEN ORDER_TYPE = '02' THEN 4 ELSE 5 END)) TARGET_BY_RDD
                  FROM OE_SUB_READINESS_STATUS_GW M
                  WHERE SHIPMENT_DATE >= TO_DATE('01/01/2020','DD/MM/RRRR')  
                   """)

  conn.commit()
  conn.close()

  printttime('PPR GRW Complete')


class CLS_PPR_VN(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    PPR_VN()


def PPR_VN():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  printttime('PPR VN Start')

  cursor.execute("""INSERT INTO CONTROL_WIP_READINESS_ALL_TMP
                  SELECT BU, GMT_TYPE, STYLE_REF, SO_NO, SO_YEAR, SUB_NO, SO_NO_DOC, SHIPMENT_DATE, CUST_GROUP, CUST_NAME
                  , BRAND_NAME, EDD_W, ORDER_ID, COLOR_FC, ORDER_QTY, SO_RELEASE, SO_RELEASE_DATE, SAMPLE_RELEASE
                  , SAMPLE_RELEASE_DATE, PATTERN_RELEASE, PATTERN_RELEASE_DATE, FC_RELEASE, FC_RELEASE_DATE
                  , SEW_ACC_COMPLETE, SEW_ACC_COMPLETE_DATE, PACK_ACC_COMPLETE, PACK_ACC_COMPLETE_DATE, FABRIC_COMPLETE
                  , FABRIC_COMPLETE_DATE, SCM_ALLOCATE, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FIRST_CUT_DATE, LOADED_QTY
                  , TO_CHAR(GET_FR_MRDDATE_BY_SO(BU, SO_YEAR,SO_NO,SHIPMENT_DATE),'IYYYIW') MRD_W,  SAM,  SCORE, 0
                  , TO_NUMBER(GET_READINESS_FIND_TARGET(RDD_W,CASE WHEN ORDER_TYPE = '02' THEN 4 ELSE 5 END)) TARGET_BY_RDD
                  FROM OE_SUB_READINESS_STATUS_NYV M
                  WHERE SHIPMENT_DATE >= TO_DATE('01/01/2020','DD/MM/RRRR') 
                 """)

  conn.commit()
  conn.close()

  printttime('PPR VN Complete')



# sendLineControlNYG('Start Insert CONTROL_WIP_READINESS_ALL_TMP')

threads = []

# thread1 = CLS_PPR_NYG()
# thread1.start()
# threads.append(thread1)

# thread2 = CLS_PPR_TRM()
# thread2.start()
# threads.append(thread2)

# thread3 = CLS_PPR_GRW()
# thread3.start()
# threads.append(thread3)

# thread4 = CLS_PPR_VN()
# thread4.start()
# threads.append(thread4)

for t in threads:
    t.join()

# sendLineControlNYG('End Insert CONTROL_WIP_READINESS_ALL_TMP')
