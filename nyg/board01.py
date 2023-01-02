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


def FR_QVD_MAINQUERY_HIS():
  my_dsn = cx_Oracle.makedsn("192.168.101.34", port=1521, sid="VN")
  conn = cx_Oracle.connect(user="VN", password="VN",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  cursor.execute("""DELETE FROM FR_QVD_MAINQUERY_HIS""")

  conn.commit()

  cursor.execute("""INSERT INTO FR_QVD_MAINQUERY_HIS
                    SELECT M.*
                    FROM NYGM.FR_QVD_MAINQUERY_HIS@APPR.WORLD M
                    WHERE FACTORY IN ('NYV')""")
  conn.commit()

  printttime('FR_QVD_MAINQUERY_HIS VN Complete')


async def CLEAR_DATA_TMP():
  await asyncio.sleep(0)
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  cursor.execute("""DELETE FROM CONTROL_WIP_READINESS_ALL_TMP""")

  conn.commit()

  printttime('Clear Data')


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
                  FROM OE_SUB_READINESS_STATUS_NYG M
                  WHERE SO_YEAR > 19 """)

  conn.commit()

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
                  , TO_CHAR(GET_FR_MRDDATE_BY_SO(BU, SO_YEAR,SO_NO,SHIPMENT_DATE),'IYYYIW') MRD_W, NULL SAM,  NULL SCORE, 0
                  FROM OE_SUB_READINESS_STATUS_TRM M
                  WHERE SO_YEAR > 19 """)

  conn.commit()

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
                  , TO_CHAR(GET_FR_MRDDATE_BY_SO(BU, SO_YEAR,SO_NO,SHIPMENT_DATE),'IYYYIW') MRD_W, NULL SAM, NULL SCORE, 0
                  FROM OE_SUB_READINESS_STATUS_GW M
                  WHERE SO_YEAR > 19 """)

  conn.commit()

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
                  , TO_CHAR(GET_FR_MRDDATE_BY_SO(BU, SO_YEAR,SO_NO,SHIPMENT_DATE),'IYYYIW') MRD_W, NULL SAM, NULL SCORE, 0
                  FROM OE_SUB_READINESS_STATUS_NYV M
                  WHERE SO_YEAR > 19 """)

  conn.commit()

  printttime('PPR VN Complete')
  

###################################################################
def NEW_PPR_NYG():
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
                  FROM OE_SUB_READINESS_STATUS_NYG M
                  WHERE SO_YEAR > 19 
                  AND NOT EXISTS (SELECT * FROM CONTROL_WIP_READINESS_CLOSED C WHERE C.BU= M.BU AND C.SO_NO_DOC = M.SO_NO_DOC) """)

  conn.commit()

  printttime('PPR NYG Complete')

  # cursor.execute("""INSERT INTO CONTROL_WIP_READINESS_ALL_TMP
  #                 SELECT BU, GMT_TYPE, STYLE_REF, SO_NO, SO_YEAR, SUB_NO, SO_NO_DOC, SHIPMENT_DATE, CUST_GROUP, CUST_NAME
  #                 , BRAND_NAME, EDD_W, ORDER_ID, COLOR_FC, ORDER_QTY, SO_RELEASE, SO_RELEASE_DATE, SAMPLE_RELEASE
  #                 , SAMPLE_RELEASE_DATE, PATTERN_RELEASE, PATTERN_RELEASE_DATE, FC_RELEASE, FC_RELEASE_DATE
  #                 , SEW_ACC_COMPLETE, SEW_ACC_COMPLETE_DATE, PACK_ACC_COMPLETE, PACK_ACC_COMPLETE_DATE, FABRIC_COMPLETE
  #                 , FABRIC_COMPLETE_DATE, SCM_ALLOCATE, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FIRST_CUT_DATE, LOADED_QTY
  #                 , TO_CHAR(GET_FR_MRDDATE_BY_SO(BU, SO_YEAR,SO_NO,SHIPMENT_DATE),'IYYYIW') MRD_W, NULL SAM,  NULL SCORE, 0
  #                 FROM OE_SUB_READINESS_STATUS_TRM M
  #                 WHERE SO_YEAR > 19 
  #                 AND NOT EXISTS (SELECT * FROM CONTROL_WIP_READINESS_CLOSED C WHERE C.BU= M.BU AND C.SO_NO_DOC = M.SO_NO_DOC) """)

  # conn.commit()

  # printttime('PPR TRM Complete')

  # cursor.execute("""INSERT INTO CONTROL_WIP_READINESS_ALL_TMP
  #                 SELECT BU, GMT_TYPE, STYLE_REF, SO_NO, SO_YEAR, SUB_NO, SO_NO_DOC, SHIPMENT_DATE, CUST_GROUP, CUST_NAME
  #                 , BRAND_NAME, EDD_W, ORDER_ID, COLOR_FC, ORDER_QTY, SO_RELEASE, SO_RELEASE_DATE, SAMPLE_RELEASE
  #                 , SAMPLE_RELEASE_DATE, PATTERN_RELEASE, PATTERN_RELEASE_DATE, FC_RELEASE, FC_RELEASE_DATE
  #                 , SEW_ACC_COMPLETE, SEW_ACC_COMPLETE_DATE, PACK_ACC_COMPLETE, PACK_ACC_COMPLETE_DATE, FABRIC_COMPLETE
  #                 , FABRIC_COMPLETE_DATE, SCM_ALLOCATE, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FIRST_CUT_DATE, LOADED_QTY
  #                 , TO_CHAR(GET_FR_MRDDATE_BY_SO(BU, SO_YEAR,SO_NO,SHIPMENT_DATE),'IYYYIW') MRD_W, NULL SAM, NULL SCORE, 0
  #                 FROM OE_SUB_READINESS_STATUS_GW M
  #                 WHERE SO_YEAR > 19 
  #                 AND NOT EXISTS (SELECT * FROM CONTROL_WIP_READINESS_CLOSED C WHERE C.BU= M.BU AND C.SO_NO_DOC = M.SO_NO_DOC) """)

  # conn.commit()

  # printttime('PPR GW Complete')

  # cursor.execute("""INSERT INTO CONTROL_WIP_READINESS_ALL_TMP
  #                 SELECT BU, GMT_TYPE, STYLE_REF, SO_NO, SO_YEAR, SUB_NO, SO_NO_DOC, SHIPMENT_DATE, CUST_GROUP, CUST_NAME
  #                 , BRAND_NAME, EDD_W, ORDER_ID, COLOR_FC, ORDER_QTY, SO_RELEASE, SO_RELEASE_DATE, SAMPLE_RELEASE
  #                 , SAMPLE_RELEASE_DATE, PATTERN_RELEASE, PATTERN_RELEASE_DATE, FC_RELEASE, FC_RELEASE_DATE
  #                 , SEW_ACC_COMPLETE, SEW_ACC_COMPLETE_DATE, PACK_ACC_COMPLETE, PACK_ACC_COMPLETE_DATE, FABRIC_COMPLETE
  #                 , FABRIC_COMPLETE_DATE, SCM_ALLOCATE, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FIRST_CUT_DATE, LOADED_QTY
  #                 , TO_CHAR(GET_FR_MRDDATE_BY_SO(BU, SO_YEAR,SO_NO,SHIPMENT_DATE),'IYYYIW') MRD_W, NULL SAM, NULL SCORE, 0
  #                 FROM OE_SUB_READINESS_STATUS_NYV M
  #                 WHERE SO_YEAR > 19 
  #                 AND NOT EXISTS (SELECT * FROM CONTROL_WIP_READINESS_CLOSED C WHERE C.BU= M.BU AND C.SO_NO_DOC = M.SO_NO_DOC)""")

  # conn.commit()

  # printttime('PPR VN Complete')

  # cursor.execute("""DELETE FROM CONTROL_WIP_READINESS_ALL""")

  # conn.commit()

  # try:
  #   cursor.execute("""INSERT INTO CONTROL_WIP_READINESS_ALL
  # SELECT BU, GMT_TYPE, STYLE_REF, SO_NO, SO_YEAR, SUB_NO, SO_NO_DOC, SHIPMENT_DATE, CUST_GROUP, CUST_NAME
  # , BRAND_NAME, EDD_W, ORDER_ID, COLOR_FC, NVL(ORDER_QTY,0) ORDER_QTY, SO_RELEASE, SO_RELEASE_DATE, SAMPLE_RELEASE
  # , SAMPLE_RELEASE_DATE, PATTERN_RELEASE, PATTERN_RELEASE_DATE, FC_RELEASE, FC_RELEASE_DATE
  # , SEW_ACC_COMPLETE, SEW_ACC_COMPLETE_DATE, PACK_ACC_COMPLETE, PACK_ACC_COMPLETE_DATE
  # , FABRIC_COMPLETE, FABRIC_COMPLETE_DATE, SCM_ALLOCATE, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FIRST_CUT_DATE
  # , CASE WHEN M.SO_RELEASE = 'Y' AND M.SAMPLE_RELEASE = 'Y' AND M.FABRIC_COMPLETE = 'Y' 
  # AND M.FC_RELEASE = 'Y' AND M.SEW_ACC_COMPLETE = 'Y' AND M.PACK_ACC_COMPLETE = 'Y'
  # AND M.FC_RELEASE = 'Y' AND M.PATTERN_RELEASE = 'Y' THEN 'Y' ELSE 'N' END SO_READY,
  # GET_READINESS_FIND_TARGET (EDD_W,4) EDD_W_NEW, GET_READINESS_FIND_TARGET (RDD_W,4) RDD_W_NEW,
  # MRD_W,
  # ACT_CUT,
  # LOADED_QTY,
  # SYSDATE, SAM, SCORE
  # FROM (
  #   select M.*
  # from CONTROL_WIP_READINESS_ALL_TMP M
  # WHERE EDD_W IS NOT NULL
  # ) M
  # WHERE NVL(ORDER_QTY,0) > 0
  # AND NOT EXISTS (SELECT * FROM CONTROL_WIP_READINESS_CLOSED C WHERE C.BU= M.BU AND C.SO_NO_DOC = M.SO_NO_DOC) """)

  #   conn.commit()
  # except:
  #   print('Error')

  printttime('PPR ALL Complete')


# FR_QVD_MAINQUERY_HIS()
# NEW_PPR_NYG()

async def main2():
  await asyncio.sleep(10)
  threads = []

  thread1 = CLS_PPR_NYG()
  thread1.start()
  threads.append(thread1)

  thread2 = CLS_PPR_TRM()
  thread2.start()
  threads.append(thread2)

  thread3 = CLS_PPR_GRW()
  thread3.start()
  threads.append(thread3)

  # thread4 = CLS_PPR_VN()
  # thread4.start()
  # threads.append(thread4)


async def main():
  # print('CLEAR_DATA_TMP')
  # CLEAR_DATA_TMP()
  # await asyncio.sleep(5)

  task1 = asyncio.create_task(CLEAR_DATA_TMP())
  task2 = asyncio.create_task(main2())

  # task2 = asyncio.create_task(PPR_NYG())
  # task3 = asyncio.create_task(PPR_TRM())
  # task4 = asyncio.create_task(PPR_GRW())
  # task5 = asyncio.create_task(PPR_VN())

  # await task1
  # await task2
  # await task3
  # await task4
  # await task5

  await asyncio.gather(task1, task2)

  print(f"finished at {time.strftime('%X')}")
  


asyncio.run(main())
