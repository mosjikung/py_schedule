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
  my_dsn = cx_Oracle.makedsn("172.16.9.54", port=1521, sid="PROD")
  conn = cx_Oracle.connect(user="RAPPS", password="RAPPS",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  printttime('RM PO BOM Start')

  sql = """

 select BU_CODE "BU",
RM_TYPE "RM Type",
PO_YEAR "PO Year",
PO_NO "PO No",
PO_NO_DOC "PO No Doc",
PO_DATE_SS "PO Date (SS)",
PO_DATE_R12 "PO Date (R12)",
PO_STATUS "PO Status (SS)",
VENDOR_CODE "Vendor Code",
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(VENDOR_NAME, CHR(10)||CHR(13),' '), CHR(9), ' '), CHR(10), ' '), CHR(13), ' '),'"',' '),',','') "Vendor Name",
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CUSTOMER, CHR(10)||CHR(13),' '), CHR(9), ' '), CHR(10), ' '), CHR(13), ' '),'"',' '),',','')  "Customer",
PO_LINE "PO Line",
ITEM_CODE "Item Code",
ITEM_COLOR,
ITEM_SIZE,
UOM_CODE "Primary UOM Code",
PO_BOM_QTY "BOM (Qty)",
PO_BOM_AMOUNT "BOM (Amount)",
PO_QTY_PRICE "PO Price (Qty)",
PO_AMOUNT_PRICE "PO Price (Amount)",
PO_QTY_FREE "PO Free (Qty)",
PO_AMOUNT_FREE "PO Free (Amount)",
PO_REC_QTY_PRICE "Receive PO Price (Qty)",
PO_REC_AMOUNT_PRICE "Receive PO Price (Amount)",
PO_REC_QTY_FREE "Receive PO Free (Qty)",
PO_REC_AMOUNT_FREE "Receive PO Free (Amount)",
TOT_PO_REC_QTY "Total Receive PO (Qty)",
TOT_PO_REC_AMOUNT "Total Receive PO (Amount)",
NET_PROD_QTY "Net Production (Qty)",
NET_PROD_AMOUNT "Net Production (Amount)",
NET_BALANCE_QTY "Bal(TOTRec-NetPro(Qty)",---max30digit
NET_BALANCE_AMOUNT "Bal(TOTRec-NetPro)(AMT)",---max30digit
DECODE(NVL(UNIT_PRICE,0), 0, 0, NVL(NET_PROD_QTY,0)) "NetPro(Qty)POLinePrice",---max30digit
DECODE(NVL(UNIT_PRICE,0), 0, 0, NVL(NET_PROD_AMOUNT,0)) "NetPro(Amount)POLinePrice",---max30digit
DECODE(NVL(UNIT_PRICE,0), 0, NVL(NET_PROD_QTY,0), 0) "NetPro(Qty)POLineFree",---max30digit
DECODE(NVL(UNIT_PRICE,0), 0, NVL(NET_PROD_AMOUNT,0), 0) "NetPro(Amount)POLineFree" , ---max30digit
ISS_SAMPLE_QTY "Issue sample (Qty)", --notsupport thai
ISS_SAMPLE_AMOUNT "Issue sample (Amount)", --notsupport thai
ISS_SALE_QTY "Issue sale (Qty)",--notsupport thai
ISS_SALE_AMOUNT "Issue sale (Amount)",--notsupport thai
ISS_OTH_QTY "Issue Others (Qty)",
ISS_OTH_AMOUNT "Issue Others (Amount)",
BALANCE_QTY "Balance (Qty)",
BALANCE_AMOUNT "Balance (Amount)",
SO_NO_DOC "SO No Doc",
SO_STATUS "SO Status",
PO_TYPE ,
MOQ,
CREATED_BY,
PO_LINE_SS_STATUS,
PO_CREATED_BY,
PO_LINE_TYPE,
REVISE_SO
from NY_PO_NYG_RM
where
po_year >='20'
union all
select BU_CODE "BU",
RM_TYPE "RM Type",
PO_YEAR "PO Year",
PO_NO "PO No",
PO_NO_DOC "PO No Doc",
PO_DATE_SS "PO Date (SS)",
PO_DATE_R12 "PO Date (R12)",
PO_STATUS "PO Status (SS)",
VENDOR_CODE "Vendor Code",
--decode(RM_TYPE,'FABRIC',decode(length(VENDOR_CODE),5,substr(VENDOR_CODE,1,1)||0||substr(VENDOR_CODE,2,4),VENDOR_CODE),VENDOR_CODE) "Vendor Code",
VENDOR_NAME "Vendor Name",
CUSTOMER  "Customer",
PO_LINE "PO Line",
ITEM_CODE "Item Code",
ITEM_COLOR,
ITEM_SIZE,
UOM_CODE "Primary UOM Code",
PO_BOM_QTY "BOM (Qty)",
PO_BOM_AMOUNT "BOM (Amount)",
PO_QTY_PRICE "PO Price (Qty)",
PO_AMOUNT_PRICE "PO Price (Amount)",
PO_QTY_FREE "PO Free (Qty)",
PO_AMOUNT_FREE "PO Free (Amount)",
PO_REC_QTY_PRICE "Receive PO Price (Qty)",
PO_REC_AMOUNT_PRICE "Receive PO Price (Amount)",
PO_REC_QTY_FREE "Receive PO Free (Qty)",
PO_REC_AMOUNT_FREE "Receive PO Free (Amount)",
TOT_PO_REC_QTY "Total Receive PO (Qty)",
TOT_PO_REC_AMOUNT "Total Receive PO (Amount)",
NET_PROD_QTY "Net Production (Qty)",
NET_PROD_AMOUNT "Net Production (Amount)",
NET_BALANCE_QTY "Bal(TOTRec-NetPro(Qty)",---max30digit
NET_BALANCE_AMOUNT "Bal(TOTRec-NetPro)(AMT)",---max30digit
DECODE(NVL(UNIT_PRICE,0), 0, 0, NVL(NET_PROD_QTY,0)) "NetPro(Qty)POLinePrice",---max30digit
DECODE(NVL(UNIT_PRICE,0), 0, 0, NVL(NET_PROD_AMOUNT,0)) "NetPro(Amount)POLinePrice",---max30digit
DECODE(NVL(UNIT_PRICE,0), 0, NVL(NET_PROD_QTY,0), 0) "NetPro(Qty)POLineFree",---max30digit
DECODE(NVL(UNIT_PRICE,0), 0, NVL(NET_PROD_AMOUNT,0), 0) "NetPro(Amount)POLineFree" , ---max30digit
ISS_SAMPLE_QTY "Issue sample (Qty)", --notsupport thai
ISS_SAMPLE_AMOUNT "Issue sample (Amount)", --notsupport thai
ISS_SALE_QTY "Issue sale (Qty)",--notsupport thai
ISS_SALE_AMOUNT "Issue sale (Amount)",--notsupport thai
ISS_OTH_QTY "Issue Others (Qty)",
ISS_OTH_AMOUNT "Issue Others (Amount)",
TOT_PO_REC_QTY-(NET_PROD_QTY)-(ISS_SAMPLE_QTY+ISS_SALE_QTY+ISS_OTH_QTY) "Balance (Qty)",
(TOT_PO_REC_QTY-(NET_PROD_QTY)-(ISS_SAMPLE_QTY+ISS_SALE_QTY+ISS_OTH_QTY) ) *PRICE_STOCK "Balance (Amount)",
SO_NO_DOC "SO No Doc",
SO_STATUS "SO Status",
DECODE(PO_FLAG,'B','BOM','D','DIRECT') "PO_TYPE",
MOQ,
'SOFTSQUARE' CREATED_BY,
GET_PO_LINE_STATUS@APPR.WORLD('N03',PO_YEAR,PO_NO,GET_ITEM_SEQ@APPR.WORLD('N03',substr(ITEM_CODE,1,6),substr(ITEM_CODE,7,10)),item_color,item_size,'',PO_LINE) PO_LINE_SS_STATUS,
GET_PO_CRE_NAME@APPR.WORLD('N03',po_year,po_no) PO_CREATED_BY,
PO_LINE_TYPE,
/*GET_PO_REVISE_SO@APPR.WORLD('N03',PO_YEAR,PO_NO)*/''  REVISE_SO
from nygm.NY_PO_NYG_RM@APPR.WORLD

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\Qlikview_Report\INVENTORY\SUMMARY_RM_PO_BOM_ALLBU.csv', index=False,encoding='utf-8-sig')


  conn.close()

  printttime('RM PO BOM Complete')
###########################################

class CLS_FC_PO(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    FC_PO()


def FC_PO():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="NYGM", password="NYGM",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  printttime('SO_FC_PO Start')

  sql = """
  select substr(a.user_dep,1,2)BU,a.CRE_BY  user_Fc,a.cre_date FC_DATE,GET_SO_NO_DOC(a.so_no,a.so_year,a.ou_code) so_no_doc,a.doc_no FC_NO,a.so_year,a.so_no,a.style_ref,
b.CRE_DATE PO_DATE,b.user_id User_PO,OE.USER_DESC(b.user_id) PO_USER_NAME
,b.vend_id,OE.VENDOR_DESC(b.vend_id) VEND_NAME,b.IMPORT_LOCAL, b.po_year,b.po_no
,b.po_no_doc,GET_SO_CUST(nvl(a.ou_code,b.ou_code),nvl(a.so_year,substr(b.CANCEL_REM,3,2)),nvl(a.so_no,substr(b.CANCEL_REM,5,8))) CUST_NAME,b.PO_TEAM,b.CANCEL_REM SO_reference,b.po_status,b.del_date,b.rea_code,d.REA_TYPE,d.REASON_GROUP,e.group_code,e.ITEM_CODE,e.ITEM_NAME
,c.ITEM_COLOR,c.ITEM_SIZE,c.PO_LINE,c.PO_QTY,c.PRICE,c.SUOM_CODE,(c.po_qty*c.price) AMT,b.currency_code
,b.EXC_RATE,((c.po_qty*c.price)*decode(b.EXC_RATE,'THB',1,b.exc_rate)) AMT_BAHT,b.po_flag,decode(a.doc_no,null,'DIRECT NORMAL','DIRECT FC') DIRECT_TYPE
from FC_RF_B_H a, po_head b,po_detail c,PO_REASON_CODE d,rm_item e
where
a.ou_code(+)=b.ou_code
and substr(a.NEW_PO_DOC(+),3,2)=b.po_YEAR
and substr(a.NEW_PO_DOC(+),5,8)=b.po_no
and b.ou_code=c.ou_code
and b.po_year=c.po_year
and b.po_no=c.po_no
and b.REA_CODE=d.REA_CODE(+)
and b.PO_STATUS<>'C'
and c.DETAIL_STATUS<>'C'
and nvl(a.STATUS_APPROVE,'N')<>'C'
and c.ITEM_SEQ=e.ITEM_SEQ
and b.po_flag='D'
and b.po_year >=to_char(sysdate,'YY')-2
and trunc(b.po_date) >=to_date('01/01/2020','dd/mm/yyyy')
and a.ou_code(+)='N03'
--and b.po_no_doc='AL2042452'
and b.OU_CODE='N03'
--and b.po_no_doc='AL2033901'
order by b.po_date,b.po_no_doc,c.item_seq,c.ITEM_COLOR,c.ITEM_SIZE

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\Qlikview_Report\INVENTORY\NYG_PO_DIRECT_FC_DETAIL.csv', index=False,encoding='utf-8-sig')


  conn.close()

  printttime('FC_PO Complete')

  ##################################################################################

threads = []

thread1 = CLS_qvd_soft_rm_po_bom() ;thread1.start() ;threads.append(thread1)
thread2 = CLS_FC_PO() ;thread2.start() ;threads.append(thread2)


for t in threads:
    t.join()



