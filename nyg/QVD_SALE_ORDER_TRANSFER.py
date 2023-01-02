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

class CLS_qvd_soft_rm_po_bom(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    qvd_soft_rm_po_bom()


def qvd_soft_rm_po_bom():
  my_dsn = cx_Oracle.makedsn("172.16.6.87", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  printttime('SALE_ORDER_TRANSFER GRW Start')

  sql = """

 select *
from V_SALE_ORDER_TRANSFER

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\QVD_DATA\COM_GARMENT\NYG\GRW_SALE_ORDER_TRANSFER.csv', index=False,encoding='utf-8-sig')


  conn.close()

  printttime('SALE_ORDER_TRANSFER GRW Complete')
###########################################

class CLS_QVD_BILL_OF_MATERIALSTRM(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    QVD_BILL_OF_MATERIALSTRM()


def QVD_BILL_OF_MATERIALSTRM():
  my_dsn = cx_Oracle.makedsn("192.168.110.6", port=1521, sid="ORCL")
  conn = cx_Oracle.connect(user="trm", password="trm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  printttime('SALE_ORDER_TRANSFER TRM Start')

  sql = """

select *
from V_SALE_ORDER_TRANSFER

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\QVD_DATA\COM_GARMENT\NYG\TRM_SALE_ORDER_TRANSFER.csv', index=False,encoding='utf-8-sig')


  conn.close()

  printttime('SALE_ORDER_TRANSFER TRM Complete')

  ##################################################################################
class CLS_qvd_soft_rm_po_bom_NYV(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    qvd_soft_rm_po_bom_NYV()


def qvd_soft_rm_po_bom_NYV():
  my_dsn = cx_Oracle.makedsn("192.168.101.34", port=1521, sid="vn")
  conn = cx_Oracle.connect(user="vn", password="vn",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  printttime('SALE_ORDER_TRANSFER NYV Start')

  sql = """

 select *
from V_SALE_ORDER_TRANSFER

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\QVD_DATA\COM_GARMENT\NYG\NYV_SALE_ORDER_TRANSFER.csv', index=False,encoding='utf-8-sig')


  conn.close()

  printttime('SALE_ORDER_TRANSFER NYV Complete')
###########################################

threads = []

thread1 = CLS_qvd_soft_rm_po_bom() ;thread1.start() ;threads.append(thread1)
thread2 = CLS_QVD_BILL_OF_MATERIALSTRM() ;thread2.start() ;threads.append(thread2)
thread3 = CLS_qvd_soft_rm_po_bom_NYV() ;thread3.start() ;threads.append(thread3)

for t in threads:
    t.join()



