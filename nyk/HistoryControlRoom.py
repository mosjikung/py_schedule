import cx_Oracle
import csv
import os
from pathlib import Path
import requests
from datetime import datetime
import threading
import time


oracle_client = "C:\instantclient_19_5"
os.environ["ORACLE_HOME"] = oracle_client
os.environ["PATH"] = oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.TH8TISASCII"


class CLS_STPE01(threading.Thread):
     def __init__(self):
        threading.Thread.__init__(self)

      def run(self):
        STPE01()


def STPE01():
  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="demo", password="demo", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  # cursor.execute("""DELETE FROM CONTROL_WIP_READINESS_ALL""")

  # conn.commit()

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
GET_READINESS_FIND_TARGET (LEAST(EDD_W,RDD_W ),6) MRD_W
FROM OE_SUB_READINESS_STATUS_V M
WHERE NVL(ORDER_QTY,0) > 0 """)

  conn.commit()

  conn.close()
  print('Complete STPE01()')


threads = []

thread1 = CLS_STPE01();thread1.start();threads.append(thread1)

for t in threads:
    t.join()
