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

def sendLineControlNYG(txt):
  url = 'https://notify-api.line.me/api/notify'
  token = 'xNt29CUwvuIQWzQOgmfpYgXY0dF7wcG46cSnQ7H2atF'
  headers = {
          'content-type':
          'application/x-www-form-urlencoded',
          'Authorization':'Bearer '+token
          }
  r = requests.post(url, headers=headers, data={'message': txt})

def STEP3():

  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  printttime('PPR STEP3 Start')


  cursor.execute("""DELETE FROM CONTROL_WIP_READINESS_ALL""")

  conn.commit()

  try:
    cursor.execute("""INSERT INTO CONTROL_WIP_READINESS_ALL
  SELECT BU, GMT_TYPE, STYLE_REF, SO_NO, SO_YEAR, SUB_NO, SO_NO_DOC, SHIPMENT_DATE, CUST_GROUP, CUST_NAME
  , BRAND_NAME, EDD_W, ORDER_ID, COLOR_FC, NVL(ORDER_QTY,0) ORDER_QTY, SO_RELEASE, SO_RELEASE_DATE, SAMPLE_RELEASE
  , SAMPLE_RELEASE_DATE, PATTERN_RELEASE, PATTERN_RELEASE_DATE, FC_RELEASE, FC_RELEASE_DATE
  , SEW_ACC_COMPLETE, SEW_ACC_COMPLETE_DATE, PACK_ACC_COMPLETE, PACK_ACC_COMPLETE_DATE
  , FABRIC_COMPLETE, FABRIC_COMPLETE_DATE, SCM_ALLOCATE, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FIRST_CUT_DATE
  , CASE WHEN M.SO_RELEASE = 'Y' AND M.SAMPLE_RELEASE = 'Y' AND M.FABRIC_COMPLETE = 'Y' 
  AND M.FC_RELEASE = 'Y' AND M.SEW_ACC_COMPLETE = 'Y' AND M.PACK_ACC_COMPLETE = 'Y'
  AND M.FC_RELEASE = 'Y' AND M.PATTERN_RELEASE = 'Y' THEN 'Y' ELSE 'N' END SO_READY,
  GET_READINESS_FIND_TARGET (EDD_W,4) EDD_W_NEW, GET_READINESS_FIND_TARGET (RDD_W,4) RDD_W_NEW,
  MRD_W,
  --CTR_GET_CUT_QTY(BU, SO_NO, SO_YEAR, NULL, NULL, NULL) ACT_CUT,
  ACT_CUT,
  LOADED_QTY,
  SYSDATE, SAM, SCORE
  , GET_READINESS_FIND_TARGET (RDD_W,4)
  , GET_READINESS_FIND_TARGET (RDD_W,5)
  , TARGET_BY_RDD
  FROM (
    select M.*
  from CONTROL_WIP_READINESS_ALL_TMP M
  WHERE RDD_W IS NOT NULL
  AND NOT EXISTS (SELECT * FROM CTROOM_BOARD_07_CLOSED C WHERE C.BU= M.BU AND C.SO_NO_DOC = M.SO_NO_DOC) 
  ) M
  WHERE NVL(ORDER_QTY,0) > 0 """)

    conn.commit()
  except:
    print('Error')



  cursor.execute("""DELETE FROM ppr_so_by_otp""")

  conn.commit()


  cursor.execute("""insert into ppr_so_by_otp
  select BU, NVL(TRIM(SCM_ALLOCATE),'NA') LOC,  SO_NO_DOC, MIN(TARGET_BY_RDD) WK, sysdate
  from CONTROL_WIP_READINESS_ALL_TMP M
  group by BU, NVL(TRIM(SCM_ALLOCATE),'NA'),  SO_NO_DOC""")

  conn.commit()

  cursor.execute("""DELETE FROM ppr_so_by_mrd""")

  conn.commit()

  cursor.execute("""insert into  ppr_so_by_mrd
  select BU, NVL(TRIM(SCM_ALLOCATE),'NA') LOC,  SO_NO_DOC, MIN(MRD_W) WK, sysdate
  from CONTROL_WIP_READINESS_ALL_TMP M
  group by BU, NVL(TRIM(SCM_ALLOCATE),'NA'),  SO_NO_DOC""")

  conn.commit()

  cursor.execute("""DELETE FROM ppr_so_by_rdd""")

  conn.commit()


  cursor.execute("""insert into   ppr_so_by_rdd
  select BU, NVL(TRIM(SCM_ALLOCATE),'NA') LOC,  SO_NO_DOC, MIN(RDD_W) WK, sysdate
  from CONTROL_WIP_READINESS_ALL_TMP M
  group by BU, NVL(TRIM(SCM_ALLOCATE),'NA'),  SO_NO_DOC""")

  conn.commit()


  cursor.execute("""DELETE FROM ppr_so_by_rdd_4""")

  conn.commit()

  cursor.execute("""insert into   ppr_so_by_rdd_4
  select BU,  LOC,  SO_NO_DOC, GET_READINESS_FIND_TARGET (WK,4) WK, sysdate
  from ppr_so_by_rdd M""")

  conn.commit()

  cursor.execute("""DELETE FROM ppr_so_by_rdd_5""")

  conn.commit()


  cursor.execute("""insert into   ppr_so_by_rdd_5
  select BU,  LOC,  SO_NO_DOC, GET_READINESS_FIND_TARGET (WK,5) WK, sysdate
  from ppr_so_by_rdd M""")

  conn.commit()





  conn.close()

  printttime('PPR STEP3 Complete')


# sendLineControlNYG('CONTROL_WIP_READINESS_ALL Start')

# STEP3()


# sendLineControlNYG('CONTROL_WIP_READINESS_ALL End')
# sendLineControlNYG('ppr_so_by_otp End')
# sendLineControlNYG('ppr_so_by_mrd End')
# sendLineControlNYG('ppr_so_by_rdd End')
# sendLineControlNYG('ppr_so_by_rdd_4 End')
# sendLineControlNYG('ppr_so_by_rdd_5 End')
