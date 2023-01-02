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


oracle_client = r"C:\instantclient_19_5"
os.environ["ORACLE_HOME"] = oracle_client
os.environ["PATH"] = oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.TH8TISASCII"

time_start = datetime.now()
allTxt = ''
allHtml = ''
py_file = 'board04.py '


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


def INSERT_DATA():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  cursor = conn.cursor()
  cursor.execute(""" INSERT INTO CONTROL_PAGE_SM3 (BU_CODE
                            ,FACTORY
                            ,VENDER_CODE
                            ,SEWING_TARGET
                            ,WIP_SM2_MIN
                            ,WIP_SM3_MIN
                            ,WIP_DAY
                            ,UPD_DATE)
                            SELECT DISTINCT M.BU_CODE, M.FACTORY, M.VENDER_CODE, 0, 0, 0, 0, NULL
                            FROM WIP_CTRL_SM2_SM3_ALL_BU_V M
                            WHERE NOT EXISTS (SELECT * FROM CONTROL_PAGE_SM3 D 
                            WHERE M.BU_CODE = D.BU_CODE 
                            AND M.FACTORY=D.FACTORY
                            AND M.VENDER_CODE = D.VENDER_CODE)
                            AND FACTORY IS NOT NULL """)
  conn.commit()
  conn.close()
  printttime('INSERT WIP_SM3')


class CLS_WIP_SM3_NYG(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    WIP_SM3_NYG()

def WIP_SM3_NYG():
  printttime('Start NYG')
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  cursor2 = conn.cursor()
  cursor.execute("""SELECT M.* FROM CONTROL_PAGE_SM3 M WHERE BU_CODE = 'NYG' """)

  for row in cursor:
    fac = "NULL" if row[1] is None else "'{}'".format(row[1])
    line = "NULL" if row[2] is None else "'{}'".format(row[2])

    sql = """
         UPDATE CONTROL_PAGE_SM3 SET UPD_DATE = SYSDATE WHERE FACTORY = {} AND VENDER_CODE = {}
     """.format(fac, line)
    # print(sql)
    cursor2.execute(sql)
    conn.commit()

  conn.close()
  printttime('UPDATE WIP_SM3_NYG')


class CLS_WIP_SM3_NOT_NYG(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    WIP_SM3_NOT_NYG()

def WIP_SM3_NOT_NYG():
  printttime('Start Not NYG')
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  cursor2 = conn.cursor()
  cursor.execute("""SELECT M.* FROM CONTROL_PAGE_SM3 M WHERE BU_CODE <> 'NYG' """)

  for row in cursor:
    fac = "NULL" if row[1] is None else "'{}'".format(row[1])
    line = "NULL" if row[2] is None else "'{}'".format(row[2])

    sql = """
         UPDATE CONTROL_PAGE_SM3 SET UPD_DATE = SYSDATE WHERE FACTORY = {} AND VENDER_CODE = {}
     """.format(fac, line)
    # print(sql)
    cursor2.execute(sql)
    conn.commit()

  conn.close()
  printttime('UPDATE WIP_SM3_NOT NYG')

printttime('Start')


INSERT_DATA()

threads = []

thread1 = CLS_WIP_SM3_NYG()
thread1.start()
threads.append(thread1)

thread2 = CLS_WIP_SM3_NOT_NYG()
thread2.start()
threads.append(thread2)

for t in threads:
    t.join()

