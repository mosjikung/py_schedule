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

class CLS_FMIT_YARN_RECEIVED(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      FMIT_YARN_RECEIVED()

def FMIT_YARN_RECEIVED():
  my_dsn = cx_Oracle.makedsn("172.16.6.75", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="NYF", password="NYF",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  sql = """SELECT *
FROM "NYF"."FMIT_YARN_RECEIVED"
WHERE RECEIVED_DATE >= TO_DATE('01/01/2015','DD/MM/YYYY')
OR (RECEIVED_DATE < TO_DATE('01/01/2015','DD/MM/YYYY') AND YARN_CLOSED = 'N')"""

  cursor.execute(sql)
  _csv = r"C:\Qlikview\QVD\"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)

  conn.close()
  print("COMPLETE FMIT_YARN_RECEIVED")
  sendLine("COMPLETE FMIT_YARN_RECEIVED")


class CLS_FMIT_GF_WAREHOUSE(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      FMIT_GF_WAREHOUSE()

def FMIT_GF_WAREHOUSE():
  my_dsn = cx_Oracle.makedsn("172.16.6.75", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="NYF", password="NYF",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  sql = """ SELECT  KP_LINE_ID, 
	 CONTRACT_NO , 
	TO_CHAR( RECEIVE_DATE ,'YYYY-MM-DD HH24:MI:SS') AS  RECEIVE_DATE , 
	 KP_HEADER_ID , 
	 KP_NO , 
	 PACKING_NO , 
	 ITEM_CODE , 
	 FABRIC_ID , 
	 FABRIC_TYPE , 
	 FABRIC_DETAIL , 
	 FABRIC_WIDTH , 
	 FABRIC_GM , 
	 FABRIC_YARD , 
	 YARN_LOT , 
	 MACHINE_NO , 
	 FABRIC_WEIGHT , 
	 FABRIC_QUALITY , 
	 FABRIC_GRADE , 
	 TRANSFER_NO , 
	 GF_TRANSFER , 
	 GF_LOCKED , 
	 FABRIC_QA , 
	 GF_CLOSED , 
	 GF_COST , 
	 LOCATOR_ACTIVE , 
	 LOCATOR_ID , 
	 OU_CODE , 
	 SO_HEADER_ID , 
	 FABRIC_TAG , 
	 KP_SO , 
	 RM_COST , 
	 STOCK_GROUP , 
	 GF_STOCK_TRANS , 
	 SCAN_BARCODE_ID , 
	 SCAN_LOCATOR_ID , 
	 SCAN_DATA , 
	 SCAN_BY , 
	 SCHEDULE_NO , 
	 NYF_FABRIC_GROUP , 
	 STD_ITEM_KG , 
	 BARCODE_ID , 
	 SPLIT_BY , 
	 GF_COST_KG , 
	TO_CHAR( GF_RECEIVE_DATE ,'YYYY-MM-DD HH24:MI:SS') AS  GF_RECEIVE_DATE , 
	 SCAN_LOC_BY , 
	TO_CHAR( ISSUE_DATE ,'YYYY-MM-DD HH24:MI:SS') AS  ISSUE_DATE , 
	 ISSUE_BY , 
	 SCHEDULE_ID , 
	 AUTO_JOB_ACTIVE , 
	 AUTO_JOB_BY , 
	 PL_SO_NO , 
	 PL_SO_LINE , 
	 PL_SO_ITEM , 
	 JOB_ID , 
	 SO_NO , 
	 LINE_ID , 
	 QC_GRADE , 
	 OLD_LOCATOR_ID , 
	 KP_PALLET_NO , 
	 STD_GF_COST 
FROM  NYBI . FMIT_GF_WAREHOUSE_V 
WHERE RECEIVE_DATE >= TO_DATE('01/01/2015','DD/MM/YYYY') """

  cursor.execute(sql)
  _csv = r"C:\Qlikview\QVD\FMIT_GF_WAREHOUSE.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)

  conn.close()
  print("COMPLETE FMIT_GF_WAREHOUSE")
  sendLine("COMPLETE FMIT_GF_WAREHOUSE")



class CLS_DFIT_KNIT_MC_ITEM(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      DFIT_KNIT_MC_ITEM()

def DFIT_KNIT_MC_ITEM():
  my_dsn = cx_Oracle.makedsn("172.16.6.75", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="NYF", password="NYF",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  sql = """ SELECT  KNIT_MC_CAT , 
	 KNIT_MC_GROUP , 
	 KNIT_MC_GUAGE , 
	 KNIT_ITEM_CODE , 
	 MC_RPM , 
	 KG_MINUTE , 
	 MC_SEQ , 
	 MC_REVOL_ROLL , 
	 MC_REVOL_WEIGHT , 
	 MACHINE_EFF_PRD , 
	 KG_PERDAY , 
	 SL , 
	 NEEDLE , 
	 FEED , 
	 DYE_NUM , 
	 ENTRY_DATE , 
	 ENTRY_BY , 
	 UPDATE_DATE , 
	 UPDATE_BY , 
	 SLM , 
	 KG_PERDAY_EFF , 
	 KG_REVOL , 
	 KG_MINUTE_EFF , 
	 ADD_IN_BY , 
	 ACTIVE_FLAG , 
	 ACTIVE_DATE , 
	 ACTIVE_STATUS , 
	 HEADE_GROUP_ITEM  
FROM  NYBI.DFIT_KNIT_MC_ITEM_V """

  cursor.execute(sql)
  _csv = r"C:\Qlikview\QVD\DFIT_KNIT_MC_ITEM.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)

  conn.close()
  print("COMPLETE DFIT_KNIT_MC_ITEM")
  sendLine("COMPLETE DFIT_KNIT_MC_ITEM")


class CLS_FMIT_PO_HEADER(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      FMIT_PO_HEADER()

def FMIT_PO_HEADER():
  my_dsn = cx_Oracle.makedsn("172.16.6.75", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="NYF", password="NYF",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  sql = """ SELECT  PO_HEADER_ID , 
	 AGENT_ID , 
	 PO_NO , 
	 PO_DATE , 
	 CREATED_BY , 
	 VENDOR_ID , 
	 VENDOR_SITE_ID , 
	 CURRENCY_CODE , 
	 AUTHORIZATION_STATUS , 
	 REVISION_NUM , 
	 APPROVED_FLAG , 
	 APPROVED_DATE , 
	 ORG_ID , 
	 UPDATE_DATE , 
	 UPDATE_BY , 
	 SEGMENT2 , 
	 SEGMENT3 , 
	 SEGMENT4 , 
	 SEGMENT5 , 
	 ATTRIBUTE1 , 
	 ATTRIBUTE2 , 
	 ATTRIBUTE3 , 
	 ATTRIBUTE4 , 
	 ATTRIBUTE5 , 
	 ATTRIBUTE6 , 
	 ATTRIBUTE7 , 
	 ATTRIBUTE8 , 
	 ATTRIBUTE9 , 
	 ATTRIBUTE10 , 
	 ATTRIBUTE11 , 
	 ATTRIBUTE12 , 
	 ATTRIBUTE13 , 
	 ATTRIBUTE14 , 
	 ATTRIBUTE15 , 
	 REMARK , 
	 CLOSED_DATE , 
	 CLOSED_CODE , 
	 CLOSED_REASON , 
	 VENDOR_NAME , 
	 VENDOR_NAME_ALT , 
	 PO_TYPE , 
	 REQUEST_WEIGHT_K , 
	 TRANSFER_WEIGHT_K , 
	 CONTRACT_NO , 
	 PO_TYPE_NAME , 
	 CLOSED_STATUS , 
	 RATE , 
	 SALE_ID , 
	 SALE_NAME , 
	 TEAM_NAME , 
	 CUSTOMER_ID , 
	 CUSTOMER_NAME , 
	 BUYYER  
FROM  NYBI.FMIT_PO_HEADER_V 
WHERE PO_DATE >= TO_DATE('01/01/2015','DD/MM/YYYY') """

  cursor.execute(sql)
  _csv = r"C:\Qlikview\QVD\FMIT_PO_HEADER.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)

  conn.close()
  print("COMPLETE FMIT_PO_HEADER")
  sendLine("COMPLETE FMIT_PO_HEADER")


class CLS_FMIT_PO_DETAIL(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      FMIT_PO_DETAIL()

def FMIT_PO_DETAIL():
  my_dsn = cx_Oracle.makedsn("172.16.6.75", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="NYF", password="NYF",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  sql = """ SELECT  PO_HEADER_ID , 
	 PO_LINE_ID , 
	 PO_NO , 
	 LINE_ID , 
	 CATEGORY_ID , 
	 ITEM_ID , 
	 ITEM_CODE , 
	 ITEM_DESC , 
	 TOTAL_QTY_K , 
	 TOTAL_QTY_P , 
	 SHIPPED_ORDER , 
	 ITEM_UNIT_K , 
	 ITEM_PRICE_K , 
	 ORA_ITEM_UNIT , 
	 ORA_ITEM_PRICE , 
	 CLOSED_DATE , 
	 CLOSED_CODE , 
	 CLOSED_REASON , 
	 ORG_ID , 
	 CLOSED_FLAG , 
	 CANCEL_FLAG , 
	 CANCELLED_BY , 
	 CANCEL_DATE , 
	 UPDATE_DATE , 
	 UPDATE_BY , 
	 ATTRIBUTE1 , 
	 ATTRIBUTE2 , 
	 ATTRIBUTE3 , 
	 ATTRIBUTE4 , 
	 ATTRIBUTE5 , 
	 ATTRIBUTE6 , 
	 ATTRIBUTE7 , 
	 ATTRIBUTE8 , 
	 ATTRIBUTE9 , 
	 ATTRIBUTE10 , 
	 ATTRIBUTE11 , 
	 ATTRIBUTE12 , 
	 ATTRIBUTE13 , 
	 ATTRIBUTE14 , 
	 ATTRIBUTE15 , 
	 REMARK , 
	 LINE_TYPE , 
	 CONS_TRANS_QTY , 
	 CONS_ISSUE_QTY , 
	 CONS_TRANS_BAL , 
	 CONS_ISSUE_BAL , 
	 REC_BAL_KG , 
	 ISS_BAL_KG , 
	 TOTAL_QTY_K_1 , 
	 TOTAL_QTY_P_1 , 
	 ITEM_PRICE_K_1 , 
	 ORA_ITEM_PRICE_1 , 
	 YARN_IN_DATE , 
	 PO_IN_DATE , 
	 PR_NO , 
	 NEED_DATE  
FROM  NYBI.FMIT_PO_DETAIL_V 
WHERE PO_HEADER_ID IN (SELECT PO_HEADER_ID from  NYBI.FMIT_PO_HEADER_V  where PO_DATE >= TO_DATE('01/01/2015','DD/MM/YYYY')) """

  cursor.execute(sql)
  _csv = r"C:\Qlikview\QVD\FMIT_PO_DETAIL.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)

  conn.close()
  print("COMPLETE FMIT_PO_DETAIL")
  sendLine("COMPLETE FMIT_PO_DETAIL")


class CLS_FMIT_KP_SCHEDULE(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      FMIT_KP_SCHEDULE()

def FMIT_KP_SCHEDULE():
  my_dsn = cx_Oracle.makedsn("172.16.6.75", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="NYF", password="NYF",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  sql = """ SELECT *
FROM NYBI.FMIT_KP_SCHEDULE_V
WHERE TO_CHAR(SCHEDULE_START,'YYYY') >= '2014' """

  cursor.execute(sql)
  _csv = r"C:\Qlikview\QVD\FMIT_KP_SCHEDULE.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)

  conn.close()
  print("COMPLETE FMIT_KP_SCHEDULE")
  sendLine("COMPLETE FMIT_KP_SCHEDULE")

#############################################
threads = []

thread1 = CLS_FMIT_YARN_RECEIVED()
thread1.start()
threads.append(thread1)

thread2 = CLS_FMIT_GF_WAREHOUSE()
thread2.start()
threads.append(thread2)

thread3 = CLS_DFIT_KNIT_MC_ITEM()
thread3.start()
threads.append(thread3)


thread4 = CLS_FMIT_PO_HEADER()
thread4.start()
threads.append(thread4)

thread5 = CLS_FMIT_PO_DETAIL()
thread5.start()
threads.append(thread5)

thread6 = CLS_FMIT_KP_SCHEDULE()
thread6.start()
threads.append(thread6)

for t in threads:
    t.join()
print("COMPLETE")