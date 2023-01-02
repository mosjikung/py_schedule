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

  conn.close()

  printttime('FR_QVD_MAINQUERY_HIS VN Complete')


def CLEAR_DATA_TMP():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  cursor.execute("""DELETE FROM CONTROL_WIP_READINESS_ALL_TMP""")

  conn.commit()

  conn.close()

  printttime('Clear Data')


def SO_VI():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  cursor.execute("""DELETE FROM CONTROL_SO_VI""")

  conn.commit()

  cursor.execute("""INSERT INTO CONTROL_SO_VI
                    SELECT BU_CODE, SO_YEAR, SO_NO, 'VI' AS VI
                    FROM OE_SO_READINESS_PO_V M
                    WHERE RM_TYPE = 'FABRIC'
                    AND PO_VI = 'Y'
                    GROUP BY BU_CODE, SO_YEAR, SO_NO""")

  conn.commit()

  conn.close()

  printttime('SO_VI')


def CUT_ALL():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  cursor.execute("""DELETE FROM CONTROL_BOARD1_ACT_CUT""")

  conn.commit()

  # NYG
  cursor.execute("""INSERT INTO CONTROL_BOARD1_ACT_CUT
                    SELECT 'NYG' BU, SO_YEAR, SO_NO, NVL(SUM(CUT_QTY), 0) ACT_CUT 
                    FROM NYG1.DFIT_CUT_DETAIL_ALL_BU@WCS.WORLD
                    WHERE 1 = 1
                    AND CONDITIONS = 'NORMAL'
                    AND IS_TRANSFER = 'N'
                    AND SO_YEAR > 18
                    GROUP BY SO_NO, SO_YEAR""")

  conn.commit()

  # NYV
  cursor.execute("""INSERT INTO CONTROL_BOARD1_ACT_CUT
                    SELECT 'NYV' BU, SO_YEAR, SO_NO, NVL(SUM(CUT_QTY), 0) ACT_CUT 
                    FROM NYG1.DFIT_CUT_DETAIL_ALL_BU@VNSQPROD.WORLD
                    WHERE 1 = 1
                    AND CONDITIONS = 'NORMAL'
                    AND IS_TRANSFER = 'N'
                    AND SO_YEAR > 18
                    GROUP BY SO_NO, SO_YEAR""")

  conn.commit()

  # TRM
  cursor.execute("""INSERT INTO CONTROL_BOARD1_ACT_CUT
                    SELECT 'TRM' BU, SO_YEAR, SO_NO, NVL(SUM(CUT_QTY), 0) ACT_CUT 
                    FROM NYG1.DFIT_CUT_DETAIL_ALL_BU@TRM.WORLD
                    WHERE 1 = 1
                    AND CONDITIONS = 'NORMAL'
                    AND IS_TRANSFER = 'N'
                    AND SO_YEAR > 18
                    GROUP BY SO_NO, SO_YEAR""")

  conn.commit()

  # GW
  cursor.execute("""INSERT INTO CONTROL_BOARD1_ACT_CUT
                    SELECT 'GW' BU, SO_YEAR, SO_NO, NVL(SUM(CUT_QTY), 0) ACT_CUT 
                    FROM NYG1.DFIT_CUT_DETAIL_ALL_BU@NGWSP.WORLD
                    WHERE 1 = 1
                    AND CONDITIONS = 'NORMAL'
                    AND IS_TRANSFER = 'N'
                    AND SO_YEAR > 18
                    GROUP BY SO_NO, SO_YEAR""")

  conn.commit()
  conn.close()

  printttime('CUT_ALL')


FR_QVD_MAINQUERY_HIS()
# CLEAR_DATA_TMP()
SO_VI()
# CUT_ALL()
