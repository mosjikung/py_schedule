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

class CLS_CONTROL_ROOM_PO_OTP_QVD_NYG(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    CONTROL_ROOM_PO_OTP_QVD_NYG()


def CONTROL_ROOM_PO_OTP_QVD_NYG():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  args = ['NYG','SOUNYG0001','CONTROL_ROOM_PO_OTP_QVD_NYG.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  printttime('CONTROL_ROOM_PO_OTP_QVD_NYG Start')

  sql = """

SELECT *
FROM CONTROL_ROOM_PO_OTP_QVD2

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\Qlikview_Report\CENTER_DATA_PUR\CONTROL_ROOM_PO_OTP_QVD\CONTROL_ROOM_PO_OTP_QVD_NYG.csv', index=False,encoding='utf-8-sig')

  args = ['NYG','SOUNYG0001',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('CONTROL_ROOM_PO_OTP_QVD_NYG Complete')

###########################################

class CLS_CONTROL_ROOM_PO_OTP_QVD_GW(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    CONTROL_ROOM_PO_OTP_QVD_GW()


def CONTROL_ROOM_PO_OTP_QVD_GW():
  my_dsn = cx_Oracle.makedsn("172.16.6.87", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  args = ['GW','SOUNYG0001','CONTROL_ROOM_PO_OTP_QVD_GW.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  printttime('CONTROL_ROOM_PO_OTP_QVD_GW Start')

  sql = """

SELECT *
FROM CONTROL_ROOM_PO_OTP_QVD2

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\Qlikview_Report\CENTER_DATA_PUR\CONTROL_ROOM_PO_OTP_QVD\CONTROL_ROOM_PO_OTP_QVD_GW.csv', index=False,encoding='utf-8-sig')

  args = ['GW','SOUNYG0001',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('CONTROL_ROOM_PO_OTP_QVD_GW Complete')

###########################################

class CLS_CONTROL_ROOM_PO_OTP_QVD_TRM(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    CONTROL_ROOM_PO_OTP_QVD_TRM()


def CONTROL_ROOM_PO_OTP_QVD_TRM():
  my_dsn = cx_Oracle.makedsn("192.168.110.6", port=1521, sid="ORCL")
  conn = cx_Oracle.connect(user="TRM", password="TRM",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  args = ['TRM','SOUNYG0001','CONTROL_ROOM_PO_OTP_QVD_TRM.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  printttime('CONTROL_ROOM_PO_OTP_QVD_TRM Start')

  sql = """

SELECT *
FROM CONTROL_ROOM_PO_OTP_QVD2

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\Qlikview_Report\CENTER_DATA_PUR\CONTROL_ROOM_PO_OTP_QVD\CONTROL_ROOM_PO_OTP_QVD_TRM.csv', index=False,encoding='utf-8-sig')

  args = ['TRM','SOUNYG0001',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('CONTROL_ROOM_PO_OTP_QVD_TRM Complete')

###########################################

class CLS_CONTROL_ROOM_PO_OTP_QVD_NYV(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    CONTROL_ROOM_PO_OTP_QVD_NYV()


def CONTROL_ROOM_PO_OTP_QVD_NYV():
  my_dsn = cx_Oracle.makedsn("192.168.101.34", port=1521, sid="VN")
  conn = cx_Oracle.connect(user="VN", password="VN",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  args = ['NYV','SOUNYG0001','CONTROL_ROOM_PO_OTP_QVD_NYV.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  printttime('CONTROL_ROOM_PO_OTP_QVD_NYV Start')

  sql = """

SELECT *
FROM CONTROL_ROOM_PO_OTP_QVD2

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\Qlikview_Report\CENTER_DATA_PUR\CONTROL_ROOM_PO_OTP_QVD\CONTROL_ROOM_PO_OTP_QVD_NYV.csv', index=False,encoding='utf-8-sig')

  args = ['NYV','SOUNYG0001',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('CONTROL_ROOM_PO_OTP_QVD_NYV Complete')

###########################################

class CLS_CONTROL_ROOM_PO_BOM_QVD_NYG(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    CONTROL_ROOM_PO_BOM_QVD_NYG()


def CONTROL_ROOM_PO_BOM_QVD_NYG():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  args = ['NYG','SOUNYG0003','CONTROL_ROOM_PO_BOM_QVD_NYG.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  printttime('CONTROL_ROOM_PO_BOM_QVD_NYG Start')

  sql = """

SELECT *
FROM CONTROL_ROOM_PO_BOM_QVD

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\Qlikview_Report\CONTROL_ROOM\CONTROL_ROOM_PO_BOM_QVD_NYG.csv', index=False,encoding='utf-8-sig')

  args = ['NYG','SOUNYG0003',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('CONTROL_ROOM_PO_BOM_QVD_NYG Complete')

###########################################

class CLS_CONTROL_ROOM_PO_BOM_QVD_GW(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    CONTROL_ROOM_PO_BOM_QVD_GW()


def CONTROL_ROOM_PO_BOM_QVD_GW():
  my_dsn = cx_Oracle.makedsn("172.16.6.87", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  printttime('CONTROL_ROOM_PO_BOM_QVD_GW Start')
  cursor = conn.cursor()
  args = ['GW','SOUNYG0003','CONTROL_ROOM_PO_BOM_QVD_GW.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  sql = """

SELECT *
FROM CONTROL_ROOM_PO_BOM_QVD

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\Qlikview_Report\CENTER_DATA_PUR\CONTROL_ROOM_PO_BOM_QVD_GW.csv', index=False,encoding='utf-8-sig')

  args = ['GW','SOUNYG0003',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('CONTROL_ROOM_PO_BOM_QVD_GW Complete')

###########################################

class CLS_CONTROL_ROOM_PO_BOM_QVD_TRM(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    CONTROL_ROOM_PO_BOM_QVD_TRM()


def CONTROL_ROOM_PO_BOM_QVD_TRM():
  my_dsn = cx_Oracle.makedsn("192.168.110.6", port=1521, sid="ORCL")
  conn = cx_Oracle.connect(user="trm", password="trm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  args = ['TRM','SOUNYG0003','CONTROL_ROOM_PO_BOM_QVD_TRM.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  printttime('CONTROL_ROOM_PO_BOM_QVD_TRM Start')

  sql = """

SELECT *
FROM CONTROL_ROOM_PO_BOM_QVD

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\Qlikview_Report\CONTROL_ROOM\CONTROL_ROOM_PO_BOM_QVD_TRM.csv', index=False,encoding='utf-8-sig')

  args = ['TRM','SOUNYG0003',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('CONTROL_ROOM_PO_BOM_QVD_TRM Complete')


###########################################

class CLS_CONTROL_ROOM_PO_BOM_QVD_NYV(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    CONTROL_ROOM_PO_BOM_QVD_NYV()


def CONTROL_ROOM_PO_BOM_QVD_NYV():
  my_dsn = cx_Oracle.makedsn("192.168.101.34", port=1521, sid="vn")
  conn = cx_Oracle.connect(user="vn", password="vn",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  printttime('CONTROL_ROOM_PO_BOM_QVD_NYV Start')
  cursor = conn.cursor()
  args = ['NYV','SOUNYG0003','CONTROL_ROOM_PO_BOM_QVD_NYV.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  sql = """

SELECT *
FROM CONTROL_ROOM_PO_BOM_QVD

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\Qlikview_Report\CONTROL_ROOM\CONTROL_ROOM_PO_BOM_QVD_NYV.csv', index=False,encoding='utf-8-sig')

  args = ['NYV','SOUNYG0003',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('CONTROL_ROOM_PO_BOM_QVD_NYV Complete')

  ###########################################

class CLS_CREADINESS_OTIF_FREEZE(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    READINESS_OTIF_FREEZE()


def READINESS_OTIF_FREEZE():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  printttime('OE_SO_READINESS_OTIF_FREEZE Start')  
  cursor = conn.cursor()
  args = ['NYG','SOUNYG0002','OE_SO_READINESS_OTIF_FREEZE.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)

  sql = """

SELECT *
FROM OE_SO_READINESS_OTIF_FREEZE_V

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\QVDatacenter\SCM\GARMENT\NYG\OE_SO_READINESS_OTIF_FREEZE.csv', index=False,encoding='utf-8-sig')

  args = ['NYG','SOUNYG0002',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('OE_SO_READINESS_OTIF_FREEZE Complete')

###########################################

class CLS_SCM_CONFIRM_MRD_MASTER_ALL(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    SCM_CONFIRM_MRD_MASTER_ALL()


def SCM_CONFIRM_MRD_MASTER_ALL():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  printttime('SCM_CONFIRM_MRD_MASTER_ALL Start')
  cursor = conn.cursor()
  args = ['NYG','SOUNYG0004','SCM_CONFIRM_MRD_MASTER_ALL.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)


  sql = """

select *
from SCM_CONFIRM_MRD_MASTER_ALL_QVD

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\QVDatacenter\SCM\GARMENT\NYG\SCM_CONFIRM_MRD_MASTER_ALL.csv', index=False,encoding='utf-8-sig')

  args = ['NYG','SOUNYG0004',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('SCM_CONFIRM_MRD_MASTER_ALL Complete')


###########################################

class CLS_Purchase_order_Export_NYG(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    Purchase_order_Export_NYG()


def Purchase_order_Export_NYG():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  printttime('V_QVD_Purchase_order_Export_NYG Start')
  cursor = conn.cursor()
  args = ['NYG','SOUNYG0005','Purchase_order_Export_NYG.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  sql = """

SELECT *
FROM V_QVD_PURCHASE_ORDER_EXPORT

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\Qlikview_Report\SPENDING GARMENT\Purchase_order_Export_NYG.csv', index=False,encoding='utf-8-sig')

  args = ['NYG','SOUNYG0005',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('V_QVD_Purchase_order_Export_NYG Complete')

  ###########################################

class CLS_Purchase_order_Export_GW(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    Purchase_order_Export_GW()


def Purchase_order_Export_GW():
  my_dsn = cx_Oracle.makedsn("172.16.6.87", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  printttime('V_QVD_Purchase_order_Export GRW Start')
  cursor = conn.cursor()
  args = ['GW','SOUNYG0005','Purchase_order_Export_GW.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  sql = """

SELECT *
FROM V_QVD_PURCHASE_ORDER_EXPORT

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\Qlikview_Report\SPENDING GARMENT\Purchase_order_Export_GW.csv', index=False,encoding='utf-8-sig')

  args = ['GW','SOUNYG0005',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('V_QVD_Purchase_order_Export_GW Complete')

###########################################

class CLS_Purchase_order_Export_TRM(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    Purchase_order_Export_TRM()


def Purchase_order_Export_TRM():
  my_dsn = cx_Oracle.makedsn("192.168.110.6", port=1521, sid="ORCL")
  conn = cx_Oracle.connect(user="trm", password="trm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  printttime('V_QVD_Purchase_order_Export TRM Start')
  cursor = conn.cursor()
  args = ['TRM','SOUNYG0005','Purchase_order_Export_TRM.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  sql = """

SELECT *
FROM V_QVD_PURCHASE_ORDER_EXPORT

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\Qlikview_Report\SPENDING GARMENT\Purchase_order_Export_TRM.csv', index=False,encoding='utf-8-sig')

  args = ['TRM','SOUNYG0005',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('V_QVD_Purchase_order_Export_TRM Complete')

###########################################

class CLS_Purchase_order_Export_NYV(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    Purchase_order_Export_NYV()


def Purchase_order_Export_NYV():
  my_dsn = cx_Oracle.makedsn("192.168.101.34", port=1521, sid="vn")
  conn = cx_Oracle.connect(user="vn", password="vn",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  printttime('V_QVD_Purchase_order_Export_NYV Start')
  cursor = conn.cursor()
  args = ['NYV','SOUNYG0005','Purchase_order_Export_NYV.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)

  sql = """

SELECT *
FROM V_QVD_PURCHASE_ORDER_EXPORT

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\Qlikview_Report\SPENDING GARMENT\Purchase_order_Export_NYV.csv', index=False,encoding='utf-8-sig')

  args = ['NYV','SOUNYG0005',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('V_QVD_Purchase_order_Export_NYV Complete')

###########################################

class CLS_NYG_FG_ONHAND_ACTIVE_R12(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    NYG_FG_ONHAND_ACTIVE_R12()


def NYG_FG_ONHAND_ACTIVE_R12():
  my_dsn = cx_Oracle.makedsn("172.16.9.54", port=1521, sid="prod")
  conn = cx_Oracle.connect(user="rapps", password="rapps",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  printttime('NYG_FG_ONHAND_ACTIVE R12 Start')
  cursor = conn.cursor()
  args = ['NYG','SOUNYG0006','FG_ONHAND_ACTIVE_R12.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)

  sql = """

SELECT *
FROM NYG_FG_ONHAND_ACTIVE_V

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\Qlikview_Report\FG_ONHAND_ACTIVE\FG_ONHAND_ACTIVE_R12.csv', index=False,encoding='utf-8-sig')

  args = ['NYG','SOUNYG0006',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('NYG_FG_ONHAND_ACTIVE R12 Complete')

  ###########################################

class CLS_NYG_FG_ONHAND_ACTIVE_WCS(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    NYG_FG_ONHAND_ACTIVE_WCS()


def NYG_FG_ONHAND_ACTIVE_WCS():
  my_dsn = cx_Oracle.makedsn("172.16.6.82", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nyg3", password="nyg3",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  args = ['G3','SOUNYG0007','NYG_FG_ONHAND_ACTIVE_WCS.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  printttime('CLS_NYG_FG_ONHAND_ACTIVE_WCS Start')

  sql = """

SELECT *
FROM SCRAP_INACTIVE

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\Qlikview_Report\FG_ONHAND_ACTIVE\NYG_FG_ONHAND_ACTIVE_WCS.csv', index=False,encoding='utf-8-sig')

  args = ['G3','SOUNYG0007',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('NYG_FG_ONHAND_ACTIVE_WCS Complete')


  
  ##################################################################################

threads = []

thread1 = CLS_CONTROL_ROOM_PO_OTP_QVD_NYG() ;thread1.start() ;threads.append(thread1)
thread14 = CLS_CONTROL_ROOM_PO_OTP_QVD_GW() ;thread14.start() ;threads.append(thread14)
thread15 = CLS_CONTROL_ROOM_PO_OTP_QVD_TRM() ;thread15.start() ;threads.append(thread15)
thread16 = CLS_CONTROL_ROOM_PO_OTP_QVD_NYV() ;thread16.start() ;threads.append(thread16)
thread2 = CLS_CREADINESS_OTIF_FREEZE() ;thread2.start() ;threads.append(thread2)
thread3 = CLS_CONTROL_ROOM_PO_BOM_QVD_NYG() ;thread3.start() ;threads.append(thread3)
thread4 = CLS_CONTROL_ROOM_PO_BOM_QVD_GW() ;thread4.start() ;threads.append(thread4)
thread5 = CLS_CONTROL_ROOM_PO_BOM_QVD_TRM() ;thread5.start() ;threads.append(thread5)
thread6 = CLS_CONTROL_ROOM_PO_BOM_QVD_NYV() ;thread6.start() ;threads.append(thread6)
thread7 = CLS_Purchase_order_Export_NYG() ;thread7.start() ;threads.append(thread7)
thread8 = CLS_Purchase_order_Export_GW() ;thread8.start() ;threads.append(thread8)
thread9 = CLS_Purchase_order_Export_TRM() ;thread9.start() ;threads.append(thread9)
thread10 = CLS_Purchase_order_Export_NYV() ;thread10.start() ;threads.append(thread10)
thread11 = CLS_SCM_CONFIRM_MRD_MASTER_ALL() ;thread11.start() ;threads.append(thread11)
thread12 = CLS_NYG_FG_ONHAND_ACTIVE_R12() ;thread12.start() ;threads.append(thread12)
thread13 = CLS_NYG_FG_ONHAND_ACTIVE_WCS() ;thread13.start() ;threads.append(thread13)

for t in threads:
    t.join()



