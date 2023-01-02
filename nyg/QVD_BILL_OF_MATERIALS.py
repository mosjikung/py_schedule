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

class CLS_QVD_BILL_OF_MATERIALS_NYG(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    qvd_soft_rm_po_bom()


def qvd_soft_rm_po_bom():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  printttime('QVD_BILL_OF_MATERIALS NYG Start')
  args = ['NYG','SCMNYG0001','NYG_BILL_OF_MATERIALS_EXPORT_CHECK.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)

  sql = """

select BU,
OU_CODE,
CRE_DATE,
ORDER_TYPE,
SO_YEAR,
SO_NO,
SO_NO_DOC,
CUST_NAME,
GROUP_CODE,
CPART_NO,
PART_DESC,
SPART_NO,
ITEM_NO,
ITEM_CODE,
ITEM_NAME,
BOM_DATE,
UPD_DATE,
UPDATEBOMUSERNAME,
ORDERQTY,
CONSUMPTION,
CONSUMPTION_GMT,
ALLOWANCE_VALUE,
ALLOWANCE_TYPE,
BOMQTY,
BOM_UOM,
SHIPMENT_DATE,
POQTY_BOMDUMMY,
PO_NO_DOC,
PO_DATE,
DEL_DATE,
DEL_DATE_TO_FAC,
to_date(to_char(UPD_INHOUSE,'DD-MON-YY')) UPD_INHOUSE,
FR_SUGGEST,
DELAY_NOTE,
VEND_NAME,
PR_QTY,
PR_NO,
PO_QTY,
FIRST_REC,
LAST_REC,
RECEIVE_QTY,
OUTSTANDING_QTY,
PO_UOM,
PO_STATUS,
STATUS,
POUSERID,
POUSERNAME,
SOUSERID,
SOUSERNAME,
BOMCONFIRMPHURCHASE,
INVOICE_SUPP,
FIRST_QUALITY,
STYLE,
SEASON,
SEASON_BUY,
ACTUAL_REC_DATE,
MFG_LT,
OU,
GMT_TYPE,
PO_NO,
PO_YEAR,
ITEM_SEQ,
DIVISION_CODE,
IMPORT_LOCAL,
MASTER_LT,
RMDS_LT,
CUST_PO,
MRD_DATE,
DELAY_REASON,
MRD_NEED_DATE,
CRE_MRD_NEED_DATE,
PRF_NO,
INSERT_BY,
INSERT_DATE
from QVD_BILL_OF_MATERIALS

  """

  df = pd.read_sql_query(sql, conn)
  
  _filename = r"C:\QVDatacenter\SCM\GARMENT\NYG\NYG_BILL_OF_MATERIALS_EXPORT_CHECK.csv"
  df.to_csv(_filename,  index=False,encoding='utf-8-sig')
  _filename = r"C:\Qlikview_Report\BILL_OF_MATERIALS\NYG_BILL_OF_MATERIALS_EXPORT_CHECK.csv"
  df.to_csv(_filename, index=False,encoding='utf-8-sig')
  args = ['NYG','SCMNYG0001',1]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)

  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  ##df.to_csv(
  ##    r'C:\QVDatacenter\SCM\GARMENT\NYG\NYG_BILL_OF_MATERIALS_EXPORT_CHECK.csv', index=False,encoding='utf-8-sig')


  conn.close()

  printttime('QVD_BILL_OF_MATERIALS NYG Complete')
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
  cursor = conn.cursor()
  args = ['TRM','SCMNYG0001','TRM_BILL_OF_MATERIALS_EXPORT_CHECK.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)

  printttime('QVD_BILL_OF_MATERIALS TRM Start')

  sql = """

select *
from V_BILL_OF_MATERIALS_EX_CHECK
where
ou_code='N03'
and so_year >=to_char(sysdate,'YY')-1
and shipment_date >=to_date('01/09/2021','dd/mm/yyyy')

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  ##df.to_csv(
  ##    r'C:\QVDatacenter\SCM\GARMENT\NYG\TRM_BILL_OF_MATERIALS_EXPORT_CHECK.csv', index=False,encoding='utf-8-sig')
  _filename = r"C:\QVDatacenter\SCM\GARMENT\NYG\TRM_BILL_OF_MATERIALS_EXPORT_CHECK.csv"
  df.to_csv(_filename, index=False,encoding='utf-8-sig')      
  _filename = r"C:\Qlikview_Report\BILL_OF_MATERIALS\TRM_BILL_OF_MATERIALS_EXPORT_CHECK.csv"
  df.to_csv(_filename, index=False,encoding='utf-8-sig')
  args = ['TRM','SCMNYG0001',1]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('QVD_BILL_OF_MATERIALS TRM Complete')

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
  cursor = conn.cursor()
  args = ['NYV','SCMNYG0001','NYV_BILL_OF_MATERIALS_EXPORT_CHECK.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)

  printttime('QVD_BILL_OF_MATERIALS NYV Start')

  sql = """

select BU,
OU_CODE,
CRE_DATE,
ORDER_TYPE,
SO_YEAR,
SO_NO,
SO_NO_DOC,
CUST_NAME,
GROUP_CODE,
CPART_NO,
PART_DESC,
SPART_NO,
ITEM_NO,
ITEM_CODE,
ITEM_NAME,
BOM_DATE,
UPD_DATE,
UPDATEBOMUSERNAME,
ORDERQTY,
CONSUMPTION,
CONSUMPTION_GMT,
ALLOWANCE_VALUE,
ALLOWANCE_TYPE,
BOMQTY,
BOM_UOM,
SHIPMENT_DATE,
POQTY_BOMDUMMY,
PO_NO_DOC,
PO_DATE,
DEL_DATE,
DEL_DATE_TO_FAC,
to_date(to_char(UPD_INHOUSE,'DD-MON-YY')) UPD_INHOUSE,
FR_SUGGEST,
DELAY_NOTE,
VEND_NAME,
PR_QTY,
PR_NO,
PO_QTY,
FIRST_REC,
LAST_REC,
RECEIVE_QTY,
OUTSTANDING_QTY,
PO_UOM,
PO_STATUS,
STATUS,
POUSERID,
POUSERNAME,
SOUSERID,
SOUSERNAME,
BOMCONFIRMPHURCHASE,
INVOICE_SUPP,
FIRST_QUALITY,
STYLE,
SEASON,
SEASON_BUY,
ACTUAL_REC_DATE,
MFG_LT,
OU,
GMT_TYPE,
PO_NO,
PO_YEAR,
ITEM_SEQ,
DIVISION_CODE,
IMPORT_LOCAL,
MASTER_LT,
RMDS_LT,
CUST_PO,
MRD_DATE,
DELAY_REASON,
MRD_NEED_DATE,
CRE_MRD_NEED_DATE,
PRF_NO,
INSERT_BY,
INSERT_DATE
from QVD_BILL_OF_MATERIALS


  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  ##df.to_csv(
  ##    r'C:\QVDatacenter\SCM\GARMENT\NYG\NYV_BILL_OF_MATERIALS_EXPORT_CHECK.csv', index=False,encoding='utf-8-sig')
  _filename = r"C:\QVDatacenter\SCM\GARMENT\NYG\NYV_BILL_OF_MATERIALS_EXPORT_CHECK.csv"
  df.to_csv(_filename, index=False,encoding='utf-8-sig')      
  _filename = r"C:\Qlikview_Report\BILL_OF_MATERIALS\NYV_BILL_OF_MATERIALS_EXPORT_CHECK.csv"
  df.to_csv(_filename, index=False,encoding='utf-8-sig')
  args = ['NYV','SCMNYG0001',1]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('QVD_BILL_OF_MATERIALS NYV Complete')
###########################################

threads = []

thread1 = CLS_QVD_BILL_OF_MATERIALS_NYG() ;thread1.start() ;threads.append(thread1)
thread2 = CLS_QVD_BILL_OF_MATERIALSTRM() ;thread2.start() ;threads.append(thread2)
thread3 = CLS_qvd_soft_rm_po_bom_NYV() ;thread3.start() ;threads.append(thread3)

for t in threads:
    t.join()



