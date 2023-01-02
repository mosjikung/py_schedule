import cx_Oracle
import csv
import os
from pathlib import Path
import requests
from datetime import datetime
import threading
import time
import pandas as pd


oracle_client = "C:\instantclient_19_5"
os.environ["ORACLE_HOME"]=oracle_client
os.environ["PATH"]=oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"]="AMERICAN_AMERICA.TH8TISASCII"


def sendLine(txt):
  url = 'https://notify-api.line.me/api/notify'
  token = 'ZE6d4wFQO2qQiSMAMqecrPbj6R3nhj0y1STqOJ6xQ1s'
  headers = {'content-type':'application/x-www-form-urlencoded','Authorization':'Bearer ' + token}
  msg = txt
  requests.post(url,headers=headers,data = {'message':msg})

###########################################
class CLS_DATA_CTP_OE(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_CTP_OE()


def DATA_CTP_OE():
  my_dsn = cx_Oracle.makedsn("172.16.6.78", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="CTP", password="MISCTP", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_CTP_OE")
  sql ="""  SELECT H.ORDER_NUMBER SO_NO,L.LINE_NUMBER,
            H.ORDER_NUMBER || '-' || H.ORG_ID AS SO_ORG,
            H.PURCHASE_ORDER_NUM PO_NO,
            H.DATE_ORDERED SO_DATE,
    DECODE(SUBSTR(H.ORDER_NUMBER,4,2),'32','INTERNAL','EXTERNAL') OE_ORDER_TYPE,
            H.CUSTOMER_ID OE_CUSTOMER_ID, C.CUSTOMER_NAME OE_CUSTOMER_NAME,
            H.SHIPPING_INSTRUCTIONS OE_END_BUYER,
            H.SALESREP_ID SALES_ID,
            (SELECT MAX( SL.SALE_NAME) FROM DFORA_SALE SL WHERE SL.SALESREP_ID = H.SALESREP_ID) SALE_NAME,
            H.CUSTOMER_FG FG_WEEK,
            H.CUSTOMER_YEAR FG_YEAR,
            (SELECT MIN(J.MPS_WEEK) FROM CTP_JOB J WHERE J.SO_NO = H.ORDER_NUMBER ) PROD_WEEK,
            (SELECT MIN(J.MPS_YEAR) FROM CTP_JOB J WHERE J.SO_NO = H.ORDER_NUMBER ) PROD_YEAR,
            GET_PRINT_TYPE_DESC(H.ORDER_NUMBER) PRINT_TYPE,
            H.ATTRIBUTE5 FABRIC_TYPE,
            H.ATTRIBUTE1 DESIGN_CODE,
            H.ATTRIBUTE2 DESIGN_DESC,
            SUM(str_to_number(L.ATTRIBUTE6)) OE_QTY_KG,
            SUM(str_to_number(L.ATTRIBUTE7)) OE_QTY_YD,
            NVL(L.SELL_PRICE_PER_KG,0) SELL_PRICE_PER_KG,
            NVL(L.SELL_PRICE_PER_YARD,0) SELL_PRICE_PER_YARD,
            SUM(str_to_number(L.ATTRIBUTE6) *  L.SELL_PRICE_PER_KG) OE_AMOUNT_KG,
            SUM(str_to_number(L.ATTRIBUTE7) * L.SELL_PRICE_PER_YARD) OE_AMOUNT_YD,
            STR_TO_DATE(H.ATTRIBUTE13,'YYYY/MM/DD HH24:MI:SS') DELIVERY_DATE,
            STR_TO_WEEK(H.ATTRIBUTE13,'YYYY/MM/DD HH24:MI:SS') DELIVERY_WW,
            L.SELLING_PRICE,H.ATTRIBUTE10 FABRIC_IN,
            NVL(H.ACTIVE,'N') ACTIVE
    FROM SO_HEADER H, SO_LINE L, DFORA_CUSTOMER C
    WHERE H.ORDER_NUMBER =L.SO_NO
    AND H.CUSTOMER_ID = C.NYF_CUS_ID
    AND H.DATE_ORDERED >= TO_DATE('01/01/2015','DD/MM/YYYY') 
    GROUP BY H.ORDER_NUMBER, L.LINE_NUMBER, H.ORDER_NUMBER || '-' || H.ORG_ID ,
            H.PURCHASE_ORDER_NUM ,
            H.DATE_ORDERED ,
            H.CUSTOMER_ID, C.CUSTOMER_NAME,
            H.SHIPPING_INSTRUCTIONS ,
            H.CUSTOMER_FG ,
            H.CUSTOMER_YEAR ,
            H.ATTRIBUTE5 ,
            H.ATTRIBUTE1 ,
            H.ATTRIBUTE2 ,
            L.SELL_PRICE_PER_KG,
            L.SELL_PRICE_PER_YARD,
            H.SALESREP_ID,H.ATTRIBUTE13,L.SELLING_PRICE,H.ATTRIBUTE10,NVL(H.ACTIVE,'N') """

  _filename = r"C:\QVD_DATA\COM_GARMENT\CTP\DATA_CTP_OE.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_CTP_OE")
  sendLine("COMPLETE CLS_DATA_CTP_OE")

#############################################

###########################################
class CLS_DATA_CTP_NI(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_CTP_NI()

def DATA_CTP_NI():
  my_dsn = cx_Oracle.makedsn("172.16.6.78", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="CTP", password="MISCTP", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_CTP_NI")

  sql = """  SELECT TRX.TRX_NUMBER INVOICE_NO,
    TRX.TRX_NUMBER || '-' || TRX.ORG_ID  AS INV_ORG,
    TRX.TRX_DATE INVOICE_DATE,
    TY.NAME TRANSECTION_TYPE,
    DECODE(SUBSTR(NVL(LRX.SALES_ORDER,TRX.CT_REFERENCE),4,2),'32','INTERNAL','EXTERNAL') NI_ORDER_TYPE,
    TRX.BILL_TO_CUSTOMER_ID NI_CUSTOMER_ID,
    TRX.PRIMARY_SALESREP_ID NI_SALES_ID,
    TRX.PURCHASE_ORDER NI_PO_NO,
    LRX.LINE_NUMBER INVOICE_LINE,
    LRX.SALES_ORDER SO_NO ,
    LRX.SALES_ORDER || '-' || TRX.ORG_ID  AS SO_ORG,
    LRX.SALES_ORDER_LINE SO_LINE,
    LRX.UOM_CODE,LRX.DESCRIPTION NI_PRINT_TYPE,
    LRX.ATTRIBUTE8 INVOICE_QTY_YARD,
    LRX.ATTRIBUTE7 INVOICE_QTY_KG,
    LRX.ATTRIBUTE13 INVOICE_QTY_PCS,
    LRX.QUANTITY_INVOICED INVOICE_QTY,
    LRX.UNIT_SELLING_PRICE,
    LRX.EXTENDED_AMOUNT, AA.NAME NI_SALES_NAME ,
    (SELECT NVL(C.ATTRIBUTE15,C.CUSTOMER_NAME) FROM  rapps.NY_RA_CUSTOMERS@R12INTERFACE.WORLD C WHERE C.CUSTOMER_ID = TRX.BILL_TO_CUSTOMER_ID) NI_CUSTOMER_NAME
    FROM rapps.RA_CUSTOMER_TRX_ALL@R12INTERFACE.WORLD TRX,
         rapps.RA_CUSTOMER_TRX_LINES_ALL@R12INTERFACE.WORLD LRX,
         RA_CUST_TRX_TYPES_ALL@R12INTERFACE.WORLD TY, 
         rapps.RA_SALESREPS_ALL@R12INTERFACE.WORLD AA
    WHERE TRX.CUSTOMER_TRX_ID = LRX.CUSTOMER_TRX_ID
    AND LINE_TYPE = 'LINE'
    AND TY.CUST_TRX_TYPE_ID  = TRX.CUST_TRX_TYPE_ID
    AND TRX.ORG_ID IN (241,244)
    AND TRX.TRX_DATE >= TO_DATE('01/01/2015','DD/MM/YYYY')
    AND TRX.PRIMARY_SALESREP_ID = AA.SALESREP_ID(+) """

  _filename = r"C:\QVD_DATA\COM_GARMENT\CTP\DATA_CTP_NI.xlsx"
   
  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_CTP_NI")
  sendLine("COMPLETE CLS_DATA_CTP_NI")

#############################################


threads = []
thread1 = CLS_DATA_CTP_OE();thread1.start();threads.append(thread1)
thread2 = CLS_DATA_CTP_NI();thread2.start();threads.append(thread2)


for t in threads:
    t.join()
print ("COMPLETE")

