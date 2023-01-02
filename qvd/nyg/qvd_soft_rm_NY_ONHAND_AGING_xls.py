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


class CLS_SO_ORDER_COSTSHEET(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    SO_ORDER_COSTSHEET()


def SO_ORDER_COSTSHEET():
  my_dsn = cx_Oracle.makedsn("172.16.9.54", port=1521, sid="PROD")
  conn = cx_Oracle.connect(user="RAPPS", password="RAPPS",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()                         
  args = ['NYG','PURNYG0001','NY_ONHAND_AGING_NYG.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)                        

  printttime('NY_ONHAND_AGING Start')

  sql = """
SELECT BU_CODE "BU", 
       RM_TYPE "RM Type",
       ORGANIZATION_NAME "OU",
       SUBINVENTORY_CODE "Subinventory Code",
       TRANSACTION_TYPE_NAME "Transaction Type Name",
       ITEM_CODE "Item Code",
       ITEM_DESC "Item Description",	      
       SO_NO "SO NO (Receive)",	
       CUSTOMER_NAME "Customer Name",	          
       LOCATOR "Locator",   
       LOT_NUMBER "Lot Number",
       LOT_BATCH_NO "Batch No",	
       LOT_COLOR "Color",	
       LOT_SIZE "Size",	
       UOM "UOM",
       PRIMARY_QUANTITY "Primary Quantity",
       ITEM_COST "Cost/Unit",	
       AMOUNT "Cost Amount",	
       RECEIVE_DATE "Receive Date",	
       AGE_DAY "Aging Day",
       RECEIVE_DATE_SUB "Receive Date(Subinventory)",	
       AGE_DAY_SUB "Aging Day (Subinventory)",    
       PO_NO "PO No",
       SHIPMENT_DATE "Shipment Date", 
       VENDOR_NAME "Supplier Name",
       LINE_FREE_STATUS "Free Status",
       RECEIVE_DATE_SUB_ONH "Receive Date (Sub-Onhand)",
       AGE_DAY_SUB_ONH "Aging Day (Sub-Onhand)",
       FABRIC_TYPE "Fabric Type",
       MOQ "MOQ",
       WEB_BARCODE "BARCODE",
       PO_LINE "PO Line",
       0 "PR_QTY",
       VENDOR_CODE,
       STYLE_CODE,
       STYLE_REF_BOM 
FROM NY_ONHAND_AGING
union all  
---soft NYG--
SELECT BU "BU", 
       RM_TYPE "RM Type",
       OU "OU",
       SUBINVENTORY_CODE "Subinventory Code",
       TRANSACTION_TYPE_NAME "Transaction Type Name",
       ITEM_CODE "Item Code",
       ITEM_DESCRIPTION "Item Description",	      
       SO_NO_DOC "SO NO (Receive)",	
       CUSTOMER_NAME "Customer Name",	          
       LOCATOR "Locator",   
       to_char(LOT_NUMBER) "Lot Number",
       BATCH_NO "Batch No",	
       COLOR "Color",	
       ITEM_SIZE "Size",	
       UOM "UOM",
       AVALIABEL_QTY "Primary Quantity",
       COST_PER_UNIT "Cost/Unit",	
       AMOUNT "Cost Amount",	
       RECEIVED_DATE "Receive Date",	
       AGING_DAY "Aging Day",
       RECEIVED_DATE_SUB "Receive Date(Subinventory)",	
       AGING_DAY_SUB "Aging Day (Subinventory)",    
       PO_NO_DOC "PO No",
       to_char(SHIPMENT_DATE,'DD-MON-RR') "Shipment Date", 
       SUPPLIER_NAME "Supplier Name",
       ' '/*LINE_FREE_STATUS*/ "Free Status", --no column in soft
       RECEIVED_DATE_SUB_ON "Receive Date (Sub-Onhand)",
       AGING_SUB_ON "Aging Day (Sub-Onhand)",
       FABRIC_TYPE "Fabric Type",
       MOQ "MOQ" ,
       BARCODE "BARCODE",
       to_char(PO_LINE) "PO Line",
       PR_QTY "PR_QTY",
       decode(length(VENDOR_CODE),5,substr(VENDOR_CODE,1,1)||0||substr(VENDOR_CODE,2,4),VENDOR_CODE) VENDOR_CODE,
       '' STYLE_CODE,
       STYLE_REF_BOM 
       FROM nygm.INACTIVE_INVENTORY_STOCK@APPR.WORLD
  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\Qlikview_Report\INVENTORY\NY_ONHAND_AGING_NYG.csv', index=False,encoding='utf-8-sig')
  args = ['NYG','PURNYG0001',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('NY_ONHAND_AGING Complete')

#################################################################
class CLS_NY_ONHAND_AGING_NYV(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    NY_ONHAND_AGING_NYV()


def NY_ONHAND_AGING_NYV():
  my_dsn = cx_Oracle.makedsn("192.168.101.36", port=1521, sid="PROD")
  conn = cx_Oracle.connect(user="R12GATE", password="R12GATE",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()                         
  args = ['NYV','PURNYG0001','NY_ONHAND_AGING_NYV.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)  
  printttime('NY_ONHAND_AGING_NYV Start')

  sql = """
    SELECT  BU_CODE "BU", 
       RM_TYPE "RM Type",
       ORGANIZATION_NAME "OU",
       SUBINVENTORY_CODE "Subinventory Code",
       TRANSACTION_TYPE_NAME "Transaction Type Name",
       ITEM_CODE "Item Code",
       ITEM_DESC "Item Description",	      
       SO_NO "SO NO (Receive)",	
       CUSTOMER_NAME "Customer Name",	          
       LOCATOR "Locator",   
       LOT_NUMBER "Lot Number",
       LOT_BATCH_NO "Batch No",	
       LOT_COLOR "Color",	
       LOT_SIZE "Size",	
       UOM "UOM",
       PRIMARY_QUANTITY "Primary Quantity",
       ITEM_COST "Cost/Unit",	
       AMOUNT "Cost Amount",	
       RECEIVE_DATE "Receive Date",	
       AGE_DAY "Aging Day",
       RECEIVE_DATE_SUB "Receive Date(Subinventory)",	
       AGE_DAY_SUB "Aging Day (Subinventory)",    
       PO_NO "PO No",
       SHIPMENT_DATE "Shipment Date", 
       VENDOR_NAME "Supplier Name",
       LINE_FREE_STATUS "Free Status",
       RECEIVE_DATE_SUB_ONH "Receive Date (Sub-Onhand)",
       AGE_DAY_SUB_ONH "Aging Day (Sub-Onhand)",
       FABRIC_TYPE "Fabric Type",
       MOQ "MOQ",
       WEB_BARCODE "BARCODE",
       PO_LINE "PO Line",
       0 "PR_QTY",
       VENDOR_CODE,
       STYLE_CODE,
       STYLE_REF_BOM 
    FROM NY_ONHAND_AGING
  """

  df = pd.read_sql_query(sql, conn)
 
  df.to_csv(
      r'C:\Qlikview_Report\INVENTORY\NY_ONHAND_AGING_NYG_NYV.csv', index=False,encoding='utf-8-sig')

  args = ['NYV','PURNYG0001',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()

  printttime('NY_ONHAND_AGING_NYV Complete')

#################################################################

threads = []

thread1 = CLS_SO_ORDER_COSTSHEET()
thread1.start()
threads.append(thread1)

thread2 = CLS_NY_ONHAND_AGING_NYV()
thread2.start()
threads.append(thread2)



for t in threads:
    t.join()



