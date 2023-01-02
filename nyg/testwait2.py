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

t1 = time.time()
time_start = datetime.now()


def printttime(txt):
  dateTimeObj = datetime.now()
  timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
  now = datetime.now()
  duration = now - time_start
  print(timestampStr + ' ' + str(duration.total_seconds()) + ' ' + txt)


async def fetch_posts(arg):
  await asyncio.sleep(arg)
  print("Fetched all posts")


async def fetch_users(arg):
  await asyncio.sleep(arg)
  print("Fetched all Users")


async def CLEAR_DATA_TMP():
  await asyncio.sleep(0)
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  cursor.execute("""DELETE FROM CONTROL_WIP_READINESS_ALL_TMP""")

  conn.commit()

  printttime('Clear Data')


async def PPR_NYG():
  await asyncio.sleep(10)
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
                  AND NOT EXISTS (SELECT * FROM CTROOM_BOARD_07_CLOSED C WHERE C.BU= M.BU AND C.SO_NO_DOC = M.SO_NO_DOC)
                  """)

  conn.commit()

  printttime('PPR NYG Complete')


async def PPR_TRM():
  await asyncio.sleep(10)
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


async def main():
  await asyncio.gather(fetch_posts(2), fetch_users(1), CLEAR_DATA_TMP(), PPR_NYG(), PPR_TRM())
  print("Finished in", time.time() - t1, "secs")

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
