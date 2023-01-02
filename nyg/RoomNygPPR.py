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


oracle_client = "C:\instantclient_19_5"
os.environ["ORACLE_HOME"]=oracle_client
os.environ["PATH"]=oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"]="AMERICAN_AMERICA.TH8TISASCII"


time_start = datetime.now()
allTxt = ''
allHtml = ''
py_file = 'RoomNygPPR.py '


def printttime(txt):
  global allTxt
  global allHtml

  dateTimeObj = datetime.now()
  timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
  now = datetime.now()
  duration = now - time_start
  if allTxt == '':
    # sendLine(py_file + 'Start', '')
    allTxt = '\n'
    allHtml = '<br>'
  print(py_file + timestampStr + ' ' +
        str(duration.total_seconds()) + ' ' + txt)
  txtSend = py_file + timestampStr + ' ' + \
      str(duration.total_seconds()) + ' ' + txt
  allTxt = allTxt + txtSend + '\n'
  allHtml = allHtml + txtSend + '<br>'


def CLOSEDSO():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  cursor.execute("""DELETE FROM CONTROL_WIP_READINESS_CLOSED""")

  conn.commit()

  cursor.execute("""INSERT INTO CONTROL_WIP_READINESS_CLOSED
                    SELECT BU, FACTORY, SO_YEAR, SO_NO, CLOSE_ORDER, SO_NO_DOC 
                    FROM OE_SO_READINESS_PROD_CLOSE_V M   """)

  conn.commit()

  class CLS_PPR(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)

      def run(self):
        PPR()


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



def NEW_PPR_NYG():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  cursor.execute("""DELETE FROM CONTROL_WIP_READINESS_ALL_TMP""")

  conn.commit()

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

  cursor.execute("""INSERT INTO CONTROL_WIP_READINESS_ALL_TMP
                  SELECT BU, GMT_TYPE, STYLE_REF, SO_NO, SO_YEAR, SUB_NO, SO_NO_DOC, SHIPMENT_DATE, CUST_GROUP, CUST_NAME
                  , BRAND_NAME, EDD_W, ORDER_ID, COLOR_FC, ORDER_QTY, SO_RELEASE, SO_RELEASE_DATE, SAMPLE_RELEASE
                  , SAMPLE_RELEASE_DATE, PATTERN_RELEASE, PATTERN_RELEASE_DATE, FC_RELEASE, FC_RELEASE_DATE
                  , SEW_ACC_COMPLETE, SEW_ACC_COMPLETE_DATE, PACK_ACC_COMPLETE, PACK_ACC_COMPLETE_DATE, FABRIC_COMPLETE
                  , FABRIC_COMPLETE_DATE, SCM_ALLOCATE, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FIRST_CUT_DATE, LOADED_QTY
                  , TO_CHAR(GET_FR_MRDDATE_BY_SO(BU, SO_YEAR,SO_NO,SHIPMENT_DATE),'IYYYIW') MRD_W, NULL SAM,  NULL SCORE, 0
                  FROM OE_SUB_READINESS_STATUS_TRM M
                  WHERE SO_YEAR > 19 
                  AND NOT EXISTS (SELECT * FROM CONTROL_WIP_READINESS_CLOSED C WHERE C.BU= M.BU AND C.SO_NO_DOC = M.SO_NO_DOC) """)

  conn.commit()

  printttime('PPR TRM Complete')

  cursor.execute("""INSERT INTO CONTROL_WIP_READINESS_ALL_TMP
                  SELECT BU, GMT_TYPE, STYLE_REF, SO_NO, SO_YEAR, SUB_NO, SO_NO_DOC, SHIPMENT_DATE, CUST_GROUP, CUST_NAME
                  , BRAND_NAME, EDD_W, ORDER_ID, COLOR_FC, ORDER_QTY, SO_RELEASE, SO_RELEASE_DATE, SAMPLE_RELEASE
                  , SAMPLE_RELEASE_DATE, PATTERN_RELEASE, PATTERN_RELEASE_DATE, FC_RELEASE, FC_RELEASE_DATE
                  , SEW_ACC_COMPLETE, SEW_ACC_COMPLETE_DATE, PACK_ACC_COMPLETE, PACK_ACC_COMPLETE_DATE, FABRIC_COMPLETE
                  , FABRIC_COMPLETE_DATE, SCM_ALLOCATE, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FIRST_CUT_DATE, LOADED_QTY
                  , TO_CHAR(GET_FR_MRDDATE_BY_SO(BU, SO_YEAR,SO_NO,SHIPMENT_DATE),'IYYYIW') MRD_W, NULL SAM, NULL SCORE, 0
                  FROM OE_SUB_READINESS_STATUS_GW M
                  WHERE SO_YEAR > 19 
                  AND NOT EXISTS (SELECT * FROM CONTROL_WIP_READINESS_CLOSED C WHERE C.BU= M.BU AND C.SO_NO_DOC = M.SO_NO_DOC) """)

  conn.commit()




  printttime('PPR GW Complete')

  cursor.execute("""INSERT INTO CONTROL_WIP_READINESS_ALL_TMP
                  SELECT BU, GMT_TYPE, STYLE_REF, SO_NO, SO_YEAR, SUB_NO, SO_NO_DOC, SHIPMENT_DATE, CUST_GROUP, CUST_NAME
                  , BRAND_NAME, EDD_W, ORDER_ID, COLOR_FC, ORDER_QTY, SO_RELEASE, SO_RELEASE_DATE, SAMPLE_RELEASE
                  , SAMPLE_RELEASE_DATE, PATTERN_RELEASE, PATTERN_RELEASE_DATE, FC_RELEASE, FC_RELEASE_DATE
                  , SEW_ACC_COMPLETE, SEW_ACC_COMPLETE_DATE, PACK_ACC_COMPLETE, PACK_ACC_COMPLETE_DATE, FABRIC_COMPLETE
                  , FABRIC_COMPLETE_DATE, SCM_ALLOCATE, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FIRST_CUT_DATE, LOADED_QTY
                  , TO_CHAR(GET_FR_MRDDATE_BY_SO(BU, SO_YEAR,SO_NO,SHIPMENT_DATE),'IYYYIW') MRD_W, NULL SAM, NULL SCORE, 0
                  FROM OE_SUB_READINESS_STATUS_NYV M
                  WHERE SO_YEAR > 19 
                  AND NOT EXISTS (SELECT * FROM CONTROL_WIP_READINESS_CLOSED C WHERE C.BU= M.BU AND C.SO_NO_DOC = M.SO_NO_DOC)""")

  conn.commit()

  printttime('PPR VN Complete')

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
  FROM (
    select M.*
  from CONTROL_WIP_READINESS_ALL_TMP M
  WHERE EDD_W IS NOT NULL
  ) M
  WHERE NVL(ORDER_QTY,0) > 0
  AND NOT EXISTS (SELECT * FROM CONTROL_WIP_READINESS_CLOSED C WHERE C.BU= M.BU AND C.SO_NO_DOC = M.SO_NO_DOC) """)

    conn.commit()
  except:
    print('Error')

  printttime('PPR ALL Complete')


class CLS_PPR(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        PPR()
        
def PPR():
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()

  cursor.execute("""DELETE FROM CONTROL_WIP_READINESS_ALL""")

  conn.commit()

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
--GET_READINESS_FIND_TARGET (LEAST(EDD_W,RDD_W ),6) MRD_W,
CTR_GET_CUT_QTY(BU, SO_NO, SO_YEAR, NULL, NULL, NULL) ACT_CUT,
LOADED_QTY,
SYSDATE
FROM (
 
 select BU, GMT_TYPE, STYLE_REF, SO_NO, SO_YEAR, SUB_NO, SO_NO_DOC, SHIPMENT_DATE, CUST_GROUP, CUST_NAME, BRAND_NAME, EDD_W, ORDER_ID, COLOR_FC, ORDER_QTY, SO_RELEASE, SO_RELEASE_DATE, SAMPLE_RELEASE, SAMPLE_RELEASE_DATE, PATTERN_RELEASE, PATTERN_RELEASE_DATE, FC_RELEASE, FC_RELEASE_DATE, SEW_ACC_COMPLETE, SEW_ACC_COMPLETE_DATE, PACK_ACC_COMPLETE, PACK_ACC_COMPLETE_DATE, FABRIC_COMPLETE, FABRIC_COMPLETE_DATE, SCM_ALLOCATE, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FIRST_CUT_DATE, LOADED_QTY, MRD_W
from OE_SUB_READINESS_STATUS_NYG
WHERE SO_YEAR > 19 
union all
select BU, GMT_TYPE, STYLE_REF, SO_NO, SO_YEAR, SUB_NO, SO_NO_DOC, SHIPMENT_DATE, CUST_GROUP, CUST_NAME, BRAND_NAME, EDD_W, ORDER_ID, COLOR_FC, ORDER_QTY, SO_RELEASE, SO_RELEASE_DATE, SAMPLE_RELEASE, SAMPLE_RELEASE_DATE, PATTERN_RELEASE, PATTERN_RELEASE_DATE, FC_RELEASE, FC_RELEASE_DATE, SEW_ACC_COMPLETE, SEW_ACC_COMPLETE_DATE, PACK_ACC_COMPLETE, PACK_ACC_COMPLETE_DATE, FABRIC_COMPLETE, FABRIC_COMPLETE_DATE, SCM_ALLOCATE, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FIRST_CUT_DATE, LOADED_QTY, MRD_W
from OE_SUB_READINESS_STATUS_TRM
WHERE SO_YEAR > 19 
union all
select BU, GMT_TYPE, STYLE_REF, SO_NO, SO_YEAR, SUB_NO, SO_NO_DOC, SHIPMENT_DATE, CUST_GROUP, CUST_NAME, BRAND_NAME, EDD_W, ORDER_ID, COLOR_FC, ORDER_QTY, SO_RELEASE, SO_RELEASE_DATE, SAMPLE_RELEASE, SAMPLE_RELEASE_DATE, PATTERN_RELEASE, PATTERN_RELEASE_DATE, FC_RELEASE, FC_RELEASE_DATE, SEW_ACC_COMPLETE, SEW_ACC_COMPLETE_DATE, PACK_ACC_COMPLETE, PACK_ACC_COMPLETE_DATE, FABRIC_COMPLETE, FABRIC_COMPLETE_DATE, SCM_ALLOCATE, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FIRST_CUT_DATE, LOADED_QTY, MRD_W
from OE_SUB_READINESS_STATUS_GW
WHERE SO_YEAR > 19 

) M
WHERE NVL(ORDER_QTY,0) > 0
AND NOT EXISTS (SELECT * FROM CONTROL_WIP_READINESS_CLOSED C WHERE C.BU= M.BU AND C.SO_NO_DOC = M.SO_NO_DOC) """)


#   cursor.execute("""INSERT INTO CONTROL_WIP_READINESS_ALL 
# SELECT m.BU, m.GMT_TYPE, m.STYLE_REF, m.SO_NO, m.SO_YEAR, m.SUB_NO, m.SO_NO_DOC, m.SHIPMENT_DATE, m.CUST_GROUP, m.CUST_NAME
# , m.BRAND_NAME, NVL(r.edd_w, m.EDD_W) EDD_W, m.ORDER_ID, m.COLOR_FC, NVL(m.ORDER_QTY, 0) ORDER_QTY, m.SO_RELEASE, m.SO_RELEASE_DATE, m.SAMPLE_RELEASE
# , m.SAMPLE_RELEASE_DATE, m.PATTERN_RELEASE, m.PATTERN_RELEASE_DATE, m.FC_RELEASE, m.FC_RELEASE_DATE
# , m.SEW_ACC_COMPLETE, m.SEW_ACC_COMPLETE_DATE, m.PACK_ACC_COMPLETE, m.PACK_ACC_COMPLETE_DATE
# , m.FABRIC_COMPLETE, m.FABRIC_COMPLETE_DATE, m.SCM_ALLOCATE, m.CREATE_DATE, m.ORDER_TYPE, m.ORDER_TYPE_DESC, nvl(r.RDD_W, m.RDD_W) RDD_W, m.FIRST_CUT_DATE
# , CASE WHEN M.SO_RELEASE = 'Y' AND M.SAMPLE_RELEASE = 'Y' AND M.FABRIC_COMPLETE = 'Y'
# AND M.FC_RELEASE = 'Y' AND M.SEW_ACC_COMPLETE = 'Y' AND M.PACK_ACC_COMPLETE = 'Y'
# AND M.FC_RELEASE = 'Y' AND M.PATTERN_RELEASE = 'Y' THEN 'Y' ELSE 'N' END SO_READY,
# GET_READINESS_FIND_TARGET(NVL(r.edd_w, m.EDD_W), 4) EDD_W_NEW, GET_READINESS_FIND_TARGET(nvl(r.RDD_W, m.RDD_W), 4) RDD_W_NEW,
# GET_READINESS_FIND_TARGET(LEAST(NVL(r.edd_w, m.EDD_W), nvl(r.RDD_W, m.RDD_W)), 6) MRD_W
# FROM OE_SUB_READINESS_STATUS_V M, control_wip_readiness r
# WHERE NVL(M.ORDER_QTY, 0) > 0
# AND M.SO_NO_DOC = R.SO_NO_DOC(+)
# AND M.BU = R.BU(+)
# AND M.SO_YEAR > 19
# AND NOT EXISTS (SELECT * FROM CONTROL_WIP_READINESS_CLOSED C WHERE C.BU= M.BU AND C.SO_NO_DOC = M.SO_NO_DOC) """)


  conn.commit()
  
  conn.close()
  # print('Complete PPR()')
  printttime('Complete PPR()')


def PPR2():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  cursor.execute("""DELETE FROM CONTROL_WIP_READINESS_ALL WHERE BU = 'NYV' """)

  conn.commit()

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
--GET_READINESS_FIND_TARGET (LEAST(EDD_W,RDD_W ),6) MRD_W,
CTR_GET_CUT_QTY(BU, SO_NO, SO_YEAR, NULL, NULL, NULL) ACT_CUT,
LOADED_QTY,
SYSDATE
FROM (
 
 select BU, GMT_TYPE, STYLE_REF, SO_NO, SO_YEAR, SUB_NO, SO_NO_DOC, SHIPMENT_DATE, CUST_GROUP, CUST_NAME, BRAND_NAME, EDD_W, ORDER_ID, COLOR_FC, ORDER_QTY, SO_RELEASE, SO_RELEASE_DATE, SAMPLE_RELEASE, SAMPLE_RELEASE_DATE, PATTERN_RELEASE, PATTERN_RELEASE_DATE, FC_RELEASE, FC_RELEASE_DATE, SEW_ACC_COMPLETE, SEW_ACC_COMPLETE_DATE, PACK_ACC_COMPLETE, PACK_ACC_COMPLETE_DATE, FABRIC_COMPLETE, FABRIC_COMPLETE_DATE, SCM_ALLOCATE, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FIRST_CUT_DATE, LOADED_QTY, MRD_W
from OE_SUB_READINESS_STATUS_NYG
WHERE SO_YEAR > 19 
union all
select BU, GMT_TYPE, STYLE_REF, SO_NO, SO_YEAR, SUB_NO, SO_NO_DOC, SHIPMENT_DATE, CUST_GROUP, CUST_NAME, BRAND_NAME, EDD_W, ORDER_ID, COLOR_FC, ORDER_QTY, SO_RELEASE, SO_RELEASE_DATE, SAMPLE_RELEASE, SAMPLE_RELEASE_DATE, PATTERN_RELEASE, PATTERN_RELEASE_DATE, FC_RELEASE, FC_RELEASE_DATE, SEW_ACC_COMPLETE, SEW_ACC_COMPLETE_DATE, PACK_ACC_COMPLETE, PACK_ACC_COMPLETE_DATE, FABRIC_COMPLETE, FABRIC_COMPLETE_DATE, SCM_ALLOCATE, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FIRST_CUT_DATE, LOADED_QTY, MRD_W
from OE_SUB_READINESS_STATUS_TRM
WHERE SO_YEAR > 19 
union all
select BU, GMT_TYPE, STYLE_REF, SO_NO, SO_YEAR, SUB_NO, SO_NO_DOC, SHIPMENT_DATE, CUST_GROUP, CUST_NAME, BRAND_NAME, EDD_W, ORDER_ID, COLOR_FC, ORDER_QTY, SO_RELEASE, SO_RELEASE_DATE, SAMPLE_RELEASE, SAMPLE_RELEASE_DATE, PATTERN_RELEASE, PATTERN_RELEASE_DATE, FC_RELEASE, FC_RELEASE_DATE, SEW_ACC_COMPLETE, SEW_ACC_COMPLETE_DATE, PACK_ACC_COMPLETE, PACK_ACC_COMPLETE_DATE, FABRIC_COMPLETE, FABRIC_COMPLETE_DATE, SCM_ALLOCATE, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FIRST_CUT_DATE, LOADED_QTY, MRD_W
from OE_SUB_READINESS_STATUS_GW
WHERE SO_YEAR > 19 

) M
WHERE NVL(ORDER_QTY,0) > 0
AND NOT EXISTS (SELECT * FROM CONTROL_WIP_READINESS_CLOSED C WHERE C.BU= M.BU AND C.SO_NO_DOC = M.SO_NO_DOC) """)


#   cursor.execute("""INSERT INTO CONTROL_WIP_READINESS_ALL
# SELECT m.BU, m.GMT_TYPE, m.STYLE_REF, m.SO_NO, m.SO_YEAR, m.SUB_NO, m.SO_NO_DOC, m.SHIPMENT_DATE, m.CUST_GROUP, m.CUST_NAME
# , m.BRAND_NAME, NVL(r.edd_w, m.EDD_W) EDD_W, m.ORDER_ID, m.COLOR_FC, NVL(m.ORDER_QTY, 0) ORDER_QTY, m.SO_RELEASE, m.SO_RELEASE_DATE, m.SAMPLE_RELEASE
# , m.SAMPLE_RELEASE_DATE, m.PATTERN_RELEASE, m.PATTERN_RELEASE_DATE, m.FC_RELEASE, m.FC_RELEASE_DATE
# , m.SEW_ACC_COMPLETE, m.SEW_ACC_COMPLETE_DATE, m.PACK_ACC_COMPLETE, m.PACK_ACC_COMPLETE_DATE
# , m.FABRIC_COMPLETE, m.FABRIC_COMPLETE_DATE, m.SCM_ALLOCATE, m.CREATE_DATE, m.ORDER_TYPE, m.ORDER_TYPE_DESC, nvl(r.RDD_W, m.RDD_W) RDD_W, m.FIRST_CUT_DATE
# , CASE WHEN M.SO_RELEASE = 'Y' AND M.SAMPLE_RELEASE = 'Y' AND M.FABRIC_COMPLETE = 'Y'
# AND M.FC_RELEASE = 'Y' AND M.SEW_ACC_COMPLETE = 'Y' AND M.PACK_ACC_COMPLETE = 'Y'
# AND M.FC_RELEASE = 'Y' AND M.PATTERN_RELEASE = 'Y' THEN 'Y' ELSE 'N' END SO_READY,
# GET_READINESS_FIND_TARGET(NVL(r.edd_w, m.EDD_W), 4) EDD_W_NEW, GET_READINESS_FIND_TARGET(nvl(r.RDD_W, m.RDD_W), 4) RDD_W_NEW,
# GET_READINESS_FIND_TARGET(LEAST(NVL(r.edd_w, m.EDD_W), nvl(r.RDD_W, m.RDD_W)), 6) MRD_W
# FROM OE_SUB_READINESS_STATUS_V M, control_wip_readiness r
# WHERE NVL(M.ORDER_QTY, 0) > 0
# AND M.SO_NO_DOC = R.SO_NO_DOC(+)
# AND M.BU = R.BU(+)
# AND M.SO_YEAR > 19
# AND NOT EXISTS (SELECT * FROM CONTROL_WIP_READINESS_CLOSED C WHERE C.BU= M.BU AND C.SO_NO_DOC = M.SO_NO_DOC) """)

  conn.commit()

  conn.close()
  # print('Complete PPR()')
  printttime('Complete PPR2()')


def OE_SO_READINESS_PO_V_01():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  cursor.execute("""DELETE FROM OE_SO_READINESS_PO_V_TMP3 WHERE 1 = 1 """)

  conn.commit()

  try:
    cursor.execute("""INSERT INTO OE_SO_READINESS_PO_V_TMP3 
                  SELECT BU_CODE, SO_YEAR, SO_NO, SO_NO_DOC, PO_NO, PO_YEAR, PO_NO_DOC, SUPPLIER_NAME, 
                  DEL_DATE, CONFIRM_DATE, RM_TYPE, ITEM_NAME, 
                  ITEM_COLOR, ITEM_SIZE, PO_QTY, REC_QTY, PURCHASER_NAME, DECODE(PO_STATUS, 'A', 'APPROVED', '9', 'CLOSED', 'N', 'NORMAL') PO_STATUS,
                  REC_STATUS, PO_VI, MOQ, BOMDATE, PO_DATE, DELAY_NOTE
                  FROM OE_SO_READINESS_PO
                  WHERE  REC_STATUS <> 'CLOSED' 
                  AND PO_NO_DOC  NOT LIKE 'KL%' 
                  AND SO_YEAR > 19 """)

    conn.commit()
  except:
    print('Error')
  conn.close()


def OE_SO_READINESS_PO_V_02():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
 

  try:
    cursor.execute("""INSERT INTO OE_SO_READINESS_PO_V_TMP3 
                  SELECT BU_CODE, SO_YEAR, SO_NO, SO_NO_DOC, PO_NO, PO_YEAR, PO_NO_DOC, SUPPLIER_NAME, 
                  DEL_DATE, CONFIRM_DATE, RM_TYPE, ITEM_NAME, 
                  ITEM_COLOR, ITEM_SIZE, PO_QTY, REC_QTY, PURCHASER_NAME, DECODE(PO_STATUS, 'A', 'APPROVED', '9', 'CLOSED', 'N', 'NORMAL') PO_STATUS,
                  REC_STATUS, PO_VI, MOQ,  BOMDATE, PO_DATE, DELAY_NOTE
                  FROM TRM.OE_SO_READINESS_PO@TRM.WORLD
                  WHERE  REC_STATUS <> 'CLOSED' 
                  AND PO_NO_DOC  NOT LIKE 'KL%' 
                  AND SO_YEAR > 19 """)

    conn.commit()
  except:
    print('Error')
  conn.close()


def OE_SO_READINESS_PO_V_03():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  

  try:
    cursor.execute("""INSERT INTO OE_SO_READINESS_PO_V_TMP3 
                  SELECT BU_CODE, SO_YEAR, SO_NO, SO_NO_DOC, PO_NO, PO_YEAR, PO_NO_DOC, SUPPLIER_NAME, 
                  DEL_DATE, CONFIRM_DATE, RM_TYPE, ITEM_NAME, 
                  ITEM_COLOR, ITEM_SIZE, PO_QTY, REC_QTY, PURCHASER_NAME, DECODE(PO_STATUS, 'A', 'APPROVED', '9', 'CLOSED', 'N', 'NORMAL') PO_STATUS,
                  REC_STATUS, PO_VI, MOQ,  BOMDATE, PO_DATE, DELAY_NOTE
                  FROM NYGM.OE_SO_READINESS_PO@NGWSP.WORLD
                  WHERE  REC_STATUS <> 'CLOSED' 
                  AND PO_NO_DOC  NOT LIKE 'KL%' 
                  AND SO_YEAR > 19 """)

    conn.commit()
  except:
    print('Error')
  conn.close()


def OE_SO_READINESS_PO_V_04():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  

  try:
    cursor.execute("""INSERT INTO OE_SO_READINESS_PO_V_TMP3 
                  SELECT BU_CODE, SO_YEAR, SO_NO, SO_NO_DOC, PO_NO, PO_YEAR, PO_NO_DOC, SUPPLIER_NAME, 
                  DEL_DATE, CONFIRM_DATE, RM_TYPE, ITEM_NAME, 
                  ITEM_COLOR, ITEM_SIZE, PO_QTY, REC_QTY, PURCHASER_NAME, DECODE(PO_STATUS, 'A', 'APPROVED', '9', 'CLOSED', 'N', 'NORMAL') PO_STATUS,
                  REC_STATUS, PO_VI, MOQ,  BOMDATE, PO_DATE, DELAY_NOTE
                  FROM VN.OE_SO_READINESS_PO@VNSQPROD.WORLD
                  WHERE  REC_STATUS <> 'CLOSED' 
                  AND PO_NO_DOC  NOT LIKE 'KL%' 
                  AND SO_YEAR > 19 """)

    conn.commit()
  except:
    print('Error')
  conn.close()


def OE_SO_READINESS_PO_V_05():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  cursor.execute("""DELETE FROM OE_SO_READINESS_PO_V_TMP2 WHERE 1 = 1 """)

  conn.commit()

  try:
    cursor.execute("""INSERT INTO OE_SO_READINESS_PO_V_TMP2 
                  SELECT M.*,
                  CASE WHEN PO_STATUS = 'CLOSED' THEN 'CLOSED'
                       WHEN REC_STATUS = 'CLOSED' THEN 'CLOSED'
                       WHEN REC_QTY >= PO_QTY THEN 'CLOSED'
                       ELSE 'PENDING' END
                  FROM OE_SO_READINESS_PO_V_TMP3 M
                 """)

    conn.commit()
  except:
    print('Error')
  conn.close()


def OE_SO_READINESS_PO_PR_STATUS_V_01():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  cursor.execute("""DELETE FROM CONTROL_PR_PO_TMP WHERE 1 = 1 """)

  conn.commit()

  cursor.execute("""INSERT INTO CONTROL_PR_PO_TMP
                      SELECT "BU","SO_NO","SO_YEAR","SO_NO_DOC","RM_TYPE","PO_FLAG","PR_FLAG"
                      FROM OE_SO_READINESS_PO_PR_STATUS
                      UNION ALL
                      SELECT "BU","SO_NO","SO_YEAR","SO_NO_DOC","RM_TYPE","PO_FLAG","PR_FLAG"
                      FROM NYGM.OE_SO_READINESS_PO_PR_STATUS@NGWSP.WORLD
                      UNION ALL
                      SELECT "BU","SO_NO","SO_YEAR","SO_NO_DOC","RM_TYPE","PO_FLAG","PR_FLAG"
                      FROM TRM.OE_SO_READINESS_PO_PR_STATUS@TRM.WORLD """)
# FIRST_CUT_DATE IS NULL

  conn.commit()

  conn.close()


def OE_SO_READINESS_PO_PR_STATUS_V_02():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  try:
    
    cursor.execute("""INSERT INTO CONTROL_PR_PO_TMP
                      SELECT "BU","SO_NO","SO_YEAR","SO_NO_DOC","RM_TYPE","PO_FLAG","PR_FLAG"
                      FROM VN.OE_SO_READINESS_PO_PR_STATUS@VNSQPROD.WORLD """)

    conn.commit()

  except:
    print('Error')
  conn.close()


def OE_SO_READINESS_PO_PR_STATUS_V_03():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  cursor.execute("""DELETE FROM CONTROL_PR_PO WHERE 1 = 1 """)

  conn.commit()

  try:

    cursor.execute("""INSERT INTO CONTROL_PR_PO
                      SELECT * FROM CONTROL_PR_PO_TMP """)

    conn.commit()

  except:
    print('Error')
  conn.close()


class CLS_Call_All_PO(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    Call_All_PO()

def Call_All_PO():
  OE_SO_READINESS_PO_V_01()
  OE_SO_READINESS_PO_V_02()
  OE_SO_READINESS_PO_V_03()
  OE_SO_READINESS_PO_V_04()
  printttime('Complete Temp 1')
  OE_SO_READINESS_PO_V_05()
  printttime('Complete Temp 2')

  # ใช้งาน
  OE_SO_READINESS_PO_PR_STATUS_V_01()
  OE_SO_READINESS_PO_PR_STATUS_V_02()
  printttime('Complete PR Status 1')
  OE_SO_READINESS_PO_PR_STATUS_V_03()
  printttime('Complete PR Status 2')
  printttime('Complete Call_All_PO()')


class CLS_OE_SO_READINESS_PLANSHIP_V(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    OE_SO_READINESS_PLANSHIP_V()

def OE_SO_READINESS_PLANSHIP_V():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  cursor.execute("""DELETE FROM OE_SO_READINESS_PLANSHIP_V_TMP WHERE 1 = 1 """)

  conn.commit()

  sqlWk = """ SELECT DISTINCT TO_CHAR(DTE,'IYYYIW') PERIOD
          FROM  PS_WW
          WHERE TO_CHAR(DTE,'IYYYIW') >= GET_READINESS_FIND_TARGET(TO_CHAR(SYSDATE,'IYYYIW'),20)
          --AND  TO_CHAR(DTE,'IYYYIW') <= GET_READINESS_FIND_TARGET(TO_CHAR( TRUNC(SYSDATE)+63,'IYYYIW'),1)
          ORDER BY 1 """

  dfWK = pd.read_sql_query(sqlWk, conn)
  # print(dfWK)

  for index, row in dfWK.iterrows():
    print(row['PERIOD'])
    cursor.execute("""INSERT INTO OE_SO_READINESS_PLANSHIP_V_TMP
                      SELECT M.*, RDD_W
                      FROM OE_SO_READINESS_PLANSHIP_V M
                      WHERE TO_CHAR(SHIPMENT_DATE,'IYYYIW') = '{}' """.format(row['PERIOD']))

    conn.commit()

    cursor.execute("""INSERT INTO OE_SO_READINESS_PLANSHIP_V_TMP
                      SELECT M.*, RDD_W
                      FROM trm.OE_SO_READINESS_PLANSHIP@trm.world M
                      WHERE TO_CHAR(SHIPMENT_DATE,'IYYYIW') = '{}' """.format(row['PERIOD']))

    conn.commit()

    cursor.execute("""INSERT INTO OE_SO_READINESS_PLANSHIP_V_TMP
                      SELECT M.*, RDD_W
                      FROM vn.OE_SO_READINESS_PLANSHIP@vnsqprod.world M
                      WHERE TO_CHAR(SHIPMENT_DATE,'IYYYIW') = '{}' """.format(row['PERIOD']))

    conn.commit()


# def OE_SO_READINESS_PLANSHIP_TRM():
#   my_dsn = cx_Oracle.makedsn("192.168.110.6", port=1521, sid="ORCL")
#   conn = cx_Oracle.connect(user="TRM", password="TRM",
#                            dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
#   cursor = conn.cursor()

  
#   sqlWk = """SELECT M.* FROM OE_SO_READINESS_PLANSHIP M """

#   dfWK = pd.read_sql_query(sqlWk, conn)
#   print(dfWK)

  # for index, row in dfWK.iterrows():
  #   print(row['PERIOD'])
  #   cursor.execute("""INSERT INTO OE_SO_READINESS_PLANSHIP_V_TMP
  #                     SELECT M.*, TO_NUMBER(TO_CHAR(SHIPMENT_DATE,'IYYYIW'))
  #                     FROM OE_SO_READINESS_PLANSHIP_V M
  #                     WHERE TO_CHAR(SHIPMENT_DATE,'IYYYIW') = '{}' """.format(row['PERIOD']))

  #   conn.commit()
    


def OE_SUB_VN():
  my_dsn = cx_Oracle.makedsn("192.168.101.34", port=1521, sid="VN")
  conn = cx_Oracle.connect(user="VN", password="VN",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  try:

#     cursor.execute("""DELETE FROM CT_ROOM_OESO""")

#     conn.commit()

#     cursor.execute(""" INSERT INTO CT_ROOM_OESO
# SELECT 'NYV' BU, OU_CODE, OS.GMT_TYPE, OS.STYLE_REF, OS.SO_NO, OS.SO_YEAR, OS.SO_NO_DOC,
# CASE
#     WHEN OS.DEPT_CODE IN ('T8') THEN 'NIKE'
#     ELSE 'NON-NIKE'
#     END CUST_GROUP
#     ,OE.CUST_DESC(OS.CUST_CODE) CUST_NAME, OE.BRAND_DESC(OS.OU_CODE, OS.BRAND_CODE) BRAND_NAME
#     ,'NYV' SCM_ALLOCATE,
#     OS.CRE_DATE CREATE_DATE,
#     OS.ORDER_TYPE,
#     OE.ORDER_TYPE_DESC(OS.ORDER_TYPE) ORDER_TYPE_DESC, CRE_DATE,
#     get_readiness_step_date('SO', os.ou_code, os.so_no, os.so_year, null, null) SO_RELEASE_DATE,
#     get_readiness_step_date('SAMPLE', os.ou_code, os.so_no, os.so_year, null, null) SAMPLE_RELEASE_DATE,
#     get_readiness_step_date('PATTERN', os.ou_code, os.so_no, os.so_year, null, null) PATTERN_RELEASE_DATE,
#     get_readiness_step_date('SEW', os.ou_code, os.so_no, os.so_year, null, null) SEW_ACC_COMPLETE_DATE,
#     get_readiness_step_date('PACK', os.ou_code, os.so_no, os.so_year, null, null) PACK_ACC_COMPLETE_DATE,
#     get_readiness_step_date('FAB', os.ou_code, os.so_no, os.so_year, null, null) FABRIC_COMPLETE_DATE
#  FROM  OE_SO OS
#     WHERE OS.OU_CODE     = 'NVN'
#     AND   OS.SO_STATUS      <> 'C'
#     AND OS.SO_YEAR > 19
#     AND OS.ORDER_TYPE IN ('01','02','03','05','07','09') """)

#     conn.commit()

#     cursor.execute("""DELETE FROM CT_ROOM_OESO_SUB""")

#     conn.commit()

#     cursor.execute(""" INSERT INTO CT_ROOM_OESO_SUB
# SELECT SUB_NO
# ,OU_CODE
# ,SO_NO
# ,SO_YEAR
# ,QUOTA
# ,COUNTRY_CODE
# ,CURRENCY_CODE
# ,BILLTO_ID
# ,SHIPTO_ID
# ,SHIP_DATE
# ,SHIP_BY
# ,SHIP_BY_DETAIL
# ,CUS_PO_NO
# ,CUS_PO_DATE
# ,EX_RATE
# ,LC_TYPE
# ,LC_DETAIL
# ,LC_TERM
# ,LC_TERM_DAY
# ,REQUIRE_DATE
# ,SUB_STATUS
# ,CRE_BY
# ,CRE_DATE
# ,UPD_BY
# ,UPD_DATE
# ,SUB_PRE_STATUS
# ,PRE_STATUS
# ,PDEPT_CODE
# ,SHIP_MARK
# ,JO_SEQ
# ,LC_DATE
# ,DIAMOND
# ,D_COUNTRY
# ,PLANT_CODE
# ,SHIP_TO_CODE
# ,CATEGORY
# ,F_FX
# ,F_FX_DATE
# ,APPROVE_SUB
# ,APPROVE_DATE
# ,APPROVE_BY
# ,NOTE
# ,PROMO_TYPE
# ,OGAC_DATE
# FROM OE_SO_SUB OSS
# WHERE EXISTS (SELECT * FROM CT_ROOM_OESO OS WHERE  OS.OU_CODE = OSS.OU_CODE
#     AND OS.SO_NO = OSS.SO_NO
#     AND OS.SO_YEAR = OSS.SO_YEAR )
#     AND OSS.SUB_STATUS <> 'C' """)

#     conn.commit()

#     cursor.execute("""DELETE FROM CT_ROOM_OESO_SUB_GMT""")

#     conn.commit()

#     cursor.execute(""" INSERT INTO CT_ROOM_OESO_SUB_GMT
# SELECT SUB_NO
# ,OU_CODE
# ,SO_NO
# ,SIZE_FC
# ,COL_FC
# ,SO_YEAR
# ,QTY
# ,PRICE
# ,MPS_PLAN_ID
# ,MPS_PLAN_ACTIVE
# ,MPS_PLAN_CLOSED
# ,FX_PRICE
# ,SDD_DATE
# ,OLD_PRICE
# ,F_FX
# ,FX_ADJUST_DATE
# ,W_RATE
# ,FX_RUN_DATE
# ,UPD_BY
# ,UPD_DATE
# ,CARTON_BOX
# ,PO_ITEM
# ,SCHEDULE_ID
# ,SCHDULE_MAIN
# ,LAUNCH_CODE
# ,LEAGUE_ID
# FROM OE_SUB_GMT OSG
# WHERE EXISTS (SELECT * FROM CT_ROOM_OESO_SUB OSS WHERE  OSS.OU_CODE = OSG.OU_CODE
#     AND OSS.SO_NO = OSG.SO_NO
#     AND OSS.SO_YEAR = OSG.SO_YEAR
#     AND OSS.SUB_NO = OSG.SUB_NO) """)

#     conn.commit()

    cursor.execute("""DELETE FROM CONTROL_WIP_READINESS_ALL""")

    conn.commit()

    sql = """ INSERT INTO CONTROL_WIP_READINESS_ALL (BU, GMT_TYPE, STYLE_REF, SO_NO, SO_YEAR, SUB_NO, SO_NO_DOC,
    CUST_GROUP, CUST_NAME, BRAND_NAME, EDD_W, ORDER_ID, COLOR_FC, ORDER_QTY
    ,SO_RELEASE,SO_RELEASE_DATE
,SAMPLE_RELEASE,SAMPLE_RELEASE_DATE
,PATTERN_RELEASE,PATTERN_RELEASE_DATE
,FC_RELEASE,FC_RELEASE_DATE
,SEW_ACC_COMPLETE,SEW_ACC_COMPLETE_DATE
,PACK_ACC_COMPLETE,PACK_ACC_COMPLETE_DATE
,FABRIC_COMPLETE,FABRIC_COMPLETE_DATE
,SCM_ALLOCATE, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC
,RDD_W, FIRST_CUT_DATE, LOADED_QTY, MRD_W
)
    
    SELECT BU, OS.GMT_TYPE, OS.STYLE_REF, OS.SO_NO, OS.SO_YEAR, OSS.SUB_NO, OS.SO_NO_DOC, 
    OS.CUST_GROUP, OS.CUST_NAME,  OS.BRAND_NAME
    ,NULL EDD_W
    ,OSG.SCHEDULE_ID ORDER_ID, OSG.COL_FC COLOR_FC, SUM(OSG.QTY) ORDER_QTY,
    get_readiness_step_completed('SO', os.ou_code, os.so_no, os.so_year, null, null) SO_RELEASE,
    get_readiness_step_date('SO', os.ou_code, os.so_no, os.so_year, null, null) SO_RELEASE_DATE,
    get_readiness_step_completed('SAMPLE', os.ou_code, os.so_no, os.so_year, null, null) SAMPLE_RELEASE,
    get_readiness_step_date('SAMPLE', os.ou_code, os.so_no, os.so_year, null, null) SAMPLE_RELEASE_DATE,
    get_readiness_step_completed('PATTERN', os.ou_code, os.so_no, os.so_year, null, null) PATTERN_RELEASE,
    get_readiness_step_date('PATTERN', os.ou_code, os.so_no, os.so_year, null, null) PATTERN_RELEASE_DATE,
    get_readiness_step_completed('FC', os.ou_code, os.so_no, os.so_year, null, OSG.COL_FC) FC_RELEASE,
    get_readiness_step_date('FC', os.ou_code, os.so_no, os.so_year, null, OSG.COL_FC) FC_RELEASE_DATE,
    get_readiness_step_completed('SEW', os.ou_code, os.so_no, os.so_year, null, null) SEW_ACC_COMPLETE,
    get_readiness_step_date('SEW', os.ou_code, os.so_no, os.so_year, null, null) SEW_ACC_COMPLETE_DATE,
    get_readiness_step_completed('PACK', os.ou_code, os.so_no, os.so_year, null, null) PACK_ACC_COMPLETE,
    get_readiness_step_date('PACK', os.ou_code, os.so_no, os.so_year, null, null) PACK_ACC_COMPLETE_DATE,
    get_readiness_step_completed('FAB', os.ou_code, os.so_no, os.so_year, null, null) FABRIC_COMPLETE,
    get_readiness_step_date('FAB', os.ou_code, os.so_no, os.so_year, null, null) FABRIC_COMPLETE_DATE,
    'NYV' SCM_ALLOCATE, os.CRE_DATE CREATE_DATE,     OS.ORDER_TYPE,    ORDER_TYPE_DESC,
    to_number(to_char(OSs.ship_date, 'IYYYIW')) RDD_W, 
    WCS_GET_FIRST_CUT_BY_SO(os.so_no, os.so_year) FIRST_CUT_DATE,
    GET_SCM_LOAD_CUT(os.so_no, os.so_year) LOADED_QTY,
    CASE
    WHEN TO_CHAR(GET_FR_ST_DATE_BY_SO(os.ou_code, os.so_year, os.so_no, null, null), 'IYYYIW') IS NULL THEN to_number(to_char(OSs.ship_date-35, 'IYYYIW'))
    ELSE TO_NUMBER(TO_CHAR(GET_FR_ST_DATE_BY_SO(os.ou_code, os.so_year, os.so_no, null, null), 'IYYYIW'))
    END MRD_W
    FROM  CT_ROOM_OESO OS, CT_ROOM_OESO_SUB OSS, CT_ROOM_OESO_SUB_GMT OSG
    WHERE 1 = 1
    AND OS.OU_CODE = OSS.OU_CODE
    AND OS.SO_NO = OSS.SO_NO
    AND OS.SO_YEAR = OSS.SO_YEAR
    AND OSS.OU_CODE = OSG.OU_CODE
    AND OSS.SO_NO = OSG.SO_NO
    AND OSS.SO_YEAR = OSG.SO_YEAR
    AND OSS.SUB_NO = OSG.SUB_NO
    GROUP BY BU, OS.GMT_TYPE, OS.OU_CODE, OS.STYLE_REF, OS.SO_NO, OS.SO_YEAR, OSS.SUB_NO, OS.SO_NO_DOC, 
    OS.CUST_GROUP, OS.CUST_NAME,  OS.BRAND_NAME, OSG.SCHEDULE_ID, OSG.COL_FC
    ,os.CRE_DATE, OS.ORDER_TYPE,    ORDER_TYPE_DESC
    , TRUNC(OSS.SHIP_DATE), TO_NUMBER(TO_CHAR(OSS.SHIP_DATE, 'IYYYIW'))  """

    cursor.execute(sql)

    conn.commit()

    # df = pd.read_sql_query(sql, conn)

    # print(df)

    print('OE_SUB_VN Complete')
  except:
    print('Error')
  conn.close()


def OE_SUB_VN2():
  my_nygm = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn_nygm = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_nygm, encoding="UTF-8", nencoding="UTF-8")
  cursor_nygm = conn_nygm.cursor()

  sql = """ SELECT DISTINCT SO_NO_DOC
FROM OE_SUB_READINESS_STATUS_NYV M
WHERE NOT EXISTS (SELECT * FROM CONTROL_WIP_READINESS_CLOSED C WHERE C.BU= M.BU AND C.SO_NO_DOC = M.SO_NO_DOC)   """

  df = pd.read_sql_query(sql, conn_nygm)
  # print(df)

  my_vn = cx_Oracle.makedsn("192.168.101.34", port=1521, sid="VN")
  conn_vn = cx_Oracle.connect(user="VN", password="VN",
                           dsn=my_vn, encoding="UTF-8", nencoding="UTF-8")
  cursor_vn = conn_vn.cursor()

  for index, row in df.iterrows():
      print(row['SO_NO_DOC'])
      cursor_vn.execute("""
      INSERT INTO CONTROL_WIP_READINESS_ALL
          SELECT BU, GMT_TYPE, STYLE_REF, SO_NO, SO_YEAR, SUB_NO, SO_NO_DOC, SHIPMENT_DATE, CUST_GROUP, CUST_NAME
          , BRAND_NAME, EDD_W, ORDER_ID, COLOR_FC, NVL(ORDER_QTY,0) ORDER_QTY, SO_RELEASE, SO_RELEASE_DATE, SAMPLE_RELEASE
          , SAMPLE_RELEASE_DATE, PATTERN_RELEASE, PATTERN_RELEASE_DATE, FC_RELEASE, FC_RELEASE_DATE
          , SEW_ACC_COMPLETE, SEW_ACC_COMPLETE_DATE, PACK_ACC_COMPLETE, PACK_ACC_COMPLETE_DATE
          , FABRIC_COMPLETE, FABRIC_COMPLETE_DATE, SCM_ALLOCATE, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FIRST_CUT_DATE
          , CASE WHEN M.SO_RELEASE = 'Y' AND M.SAMPLE_RELEASE = 'Y' AND M.FABRIC_COMPLETE = 'Y' 
          AND M.FC_RELEASE = 'Y' AND M.SEW_ACC_COMPLETE = 'Y' AND M.PACK_ACC_COMPLETE = 'Y'
          AND M.FC_RELEASE = 'Y' AND M.PATTERN_RELEASE = 'Y' THEN 'Y' ELSE 'N' END SO_READY,
          NULL EDD_W_NEW, NULL RDD_W_NEW,
          MRD_W,
          0 ACT_CUT,
          LOADED_QTY,
          SYSDATE
          FROM (
          SELECT M.* FROM OE_SUB_READINESS_STATUS M WHERE SO_NO_DOC = '{}'
          ) M
       """.format(row['SO_NO_DOC']))

      conn_vn.commit()



#   try:
#     sql = """
# SELECT M.* FROM OE_SUB_READINESS_STATUS M

#     """

#     df = pd.read_sql_query(sql, conn)

#     print(df)

#     print('OE_SUB_VN Complete')
#   except:
#     print('Error')

  conn_nygm.close()
  conn_vn.close()

# OE_SUB_VN2()


class CLS_PPR_NEW(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    PPR_NEW()


def PPR_NEW():
  FR_QVD_MAINQUERY_HIS()
  NEW_PPR_NYG()
  print('ggggggggggg')





def ins_week():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
 
  date_str2 = '04/01/2021'

  for i in range(0, 366, 7):
      date_add = datetime.strptime(date_str2, '%d/%m/%Y') + timedelta(days=i)
      dte = date_add.strftime("%d/%m/%Y")

      try:
        cursor.execute("""
                          INSERT INTO PS_WW
                          SELECT 
                          TO_CHAR(TO_DATE('{dte}','DD/MM/RRRR') ,'IYYY') NYEAR, 
                          TO_CHAR(TO_DATE('{dte}','DD/MM/RRRR') ,'IW') NWEEK,
                          TO_DATE('{dte}','DD/MM/RRRR') DTE 
                          FROM DUAL
                  """.format(dte=dte))

        conn.commit()
      except:
        a = 'g'

  conn.close()
  printttime('PS_WW Complete')


printttime('Start 3 process')

threads = []

# ins_week()
# OE_SO_READINESS_PLANSHIP_TRM


# thread1 = CLS_PPR_NEW();thread1.start();threads.append(thread1);
thread2 = CLS_Call_All_PO();thread2.start();threads.append(thread2);
# thread3 = CLS_OE_SO_READINESS_PLANSHIP_V();thread3.start();threads.append(thread3);


# 2020-10-30 ----------------


for t in threads:
    t.join()



