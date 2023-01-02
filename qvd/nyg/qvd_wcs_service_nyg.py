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
class CLS_PROD_OUTPUT(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        PROD_OUTPUT()
        
def PROD_OUTPUT():
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  args = ['NYG','SOUSER0001','PUR_Allocate_Vendor_Service.xlsx']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print ("PUR_Allocate_Vendor_Service Start")
  sendLine("START PUR_Allocate_Vendor_Service")
  sql="""    select distinct b.so_status,b.so_no_doc,a.OU_CODE,b.order_type,a.SO_YEAR,a.SO_NO,b.shipment_date,b.cust_code,OE.CUST_DESC(b.cust_code) Cust_name,b.brand_code,OE.BRAND_DESC(b.ou_code,b.brand_code) Brand_name,
a.VEND_ID	,rm.VENDOR_DESC(a.vend_id) Vendname,ALLOCATE_TYPE,
sum(c.fr_qty) fr_qty,
GET_FR_START_SEW_DATE (a.OU_CODE, a.SO_NO, a.SO_YEAR, null, null, null, null) START_SEWING,GET_FR_START_CUT_DATE (a.OU_CODE, a.SO_NO, a.SO_YEAR, null, null, null, null) START_CUTTING 
from SO_OE_ALLOCATE_SUB_VENDOR a ,NYG_OE_SO b, SO_OE_ALLOCATE_NEW_SUB C
where 
a.ou_code=b.ou_code
and a.so_year=b.so_year
and a.so_no=b.so_no
and a.ou_code = c.ou_code
and a.so_no = c.so_no
and a.so_year = c.so_year
and A.ORDER_ID = C.ORDER_ID
and a.so_year >=to_char(sysdate,'YY')-2
and a.ou_code='N03'
group by a.ou_code, b.so_status,b.so_no_doc,a.OU_CODE,b.order_type,a.SO_YEAR,a.SO_NO,b.shipment_date,b.cust_code,OE.CUST_DESC(b.cust_code),b.brand_code,OE.BRAND_DESC(b.ou_code,b.brand_code),
a.VEND_ID	,rm.VENDOR_DESC(a.vend_id),ALLOCATE_TYPE,
GET_FR_START_SEW_DATE (a.OU_CODE, a.SO_NO, a.SO_YEAR, null, null, null, null)
order by A.so_year, A.so_no
      """
  
  _filename = r"C:\Qlikview_Report\PRINT_EMB\PUR_Allocate_Vendor_Service.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  args = ['NYG','SOUSER0001',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()
  print ("PUR_Allocate_Vendor_Service Done")
  sendLine("PUR_Allocate_Vendor_Service")


###########################################
class CLS_PROD_PLANING(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        PROD_PLANING()
        
def PROD_PLANING():
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  args = ['NYG','SOUSER0002','PUR_Master_Routing_Style_GSD.xlsx']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print("START PUR_Master_Routing_Style_GSD ")
  sendLine("START CPUR_Master_Routing_Style_GSD")
  sql=""" select  *
from OE_ROUTING_STYLE_M
where 
ou_code='N03'
and nvl(active,'Y')='Y'
--and style_ref =nvl(V_STRYLE_S,style_ref)
order by style_ref,SEASON_CODE,rt_st_id
 """
  
  _filename = r"C:\Qlikview_Report\PRINT_EMB\PUR_Master_Routing_Style_GSD.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  args = ['NYG','SOUSER0002',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()
  print ("PUR_Master_Routing_Style_GSD Done")
  sendLine("PUR_Master_Routing_Style_GSD")


###########################################
class CLS_DATA_KP(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_KP()


def DATA_KP():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  args = ['NYG','SOUSER0003','PUR_MasterPlan.xlsx']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print("PUR_MasterPlan Start")
  sendLine("PUR_MasterPlan Start")
  sql =""" select * from V_MASTER_PLAN """

  _filename = r"C:\Qlikview_Report\PRINT_EMB\PUR_MasterPlan.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  args = ['NYG','SOUSER0003',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()
  print("PUR_MasterPlan Done")
  sendLine("PUR_MasterPlan")


  ###########################################
class CLS_WCS_SEVICE(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        WCS_SEVICE()
        
def WCS_SEVICE():
  my_dsn = cx_Oracle.makedsn("172.16.6.82",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  args = ['G1','SOUSER0004','PUR_WCSG1-G4_Service.xlsx']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print ("Start PUR_WCSG1-G4_Service")
  sendLine("START PUR_WCSG1-G4_Service")
  sql=""" 
   select *
from QVD_WCS_SERVICE_NYG
union all
select *
from nyg_pho.QVD_WCS_SERVICE_NYG@nyg2.world 
union all
select *
from nyg3.QVD_WCS_SERVICE_NYG
union all
select *
from nyg4.QVD_WCS_SERVICE_NYG@nyg4.world 
      """
  
  _filename = r"C:\Qlikview_Report\PRINT_EMB\PUR_WCSG1-G4_Service.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  args = ['G1','SOUSER0004',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()
  print ("PUR_WCSG1-G4_Service Done")
  sendLine("PUR_WCSG1-G4_Service")

#############################################
threads = []

thread1 = CLS_PROD_OUTPUT();thread1.start();threads.append(thread1) 
thread2 = CLS_PROD_PLANING();thread2.start();threads.append(thread2)
thread3 = CLS_DATA_KP();thread3.start();threads.append(thread3)
thread4 = CLS_WCS_SEVICE();thread4.start();threads.append(thread4)




for t in threads:
    t.join()
print ("COMPLETE")

