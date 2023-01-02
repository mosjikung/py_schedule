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
class CLS_SO_ORDER_NYG_2019(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    SO_ORDER_NYG_2019()

def SO_ORDER_NYG_2019():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  args = ['NYG','SPONYG0001','SO_ORDER_NYG_2019.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print("sart  SO_ORDER_NYG_2019")
  ##sendLine("START CLS_DATA_BATCH_PRODUCTION_COST")

  sql =""" 
SELECT 'NYG' BUSINESS,SUB.CUS_PO_NO,SUB.CUS_PO_DATE,SO.ORDER_TYPE,OE.ORDER_TYPE_DESC(SO.ORDER_TYPE)ORDERTYPE,FAC.FACTORY_NAME,SO.DEPT_CODE,
  SO.SO_NO_DOC,SO.CRE_DATE,SUB.SUB_NO,SUB.SHIP_DATE,TO_CHAR(SUB.SHIP_DATE,'YYYY') YEARWEEK,SO.SHIPMENT_DATE,
  Get_Ww(TO_CHAR(sub.ship_date,'dd/mm/yyyy')) WEEKNANYANGSUB,   Get_Ww(TO_CHAR(SO.SHIPMENT_DATE,'dd/mm/yyyy')) WEEKNANYANGHEAD,
  GET_WW_MONTH(TO_CHAR(SUB.SHIP_DATE,'dd/mm/yyyy')) MONTHWEEKNANYANG,  SO.CUST_CODE,OE.CUST_DESC(SO.CUST_CODE) CUSTOMERNAME,SO.BRAND_CODE,
  OE.BRAND_DESC('N03',SO.BRAND_CODE) BRANDNAME,OE.SHIPTO_COUNTRY(SHIPTO_ID) SHIPCOUNTRY, OE.SEASON_DESC('N03',SO.SEA_CODE) ORDERSEASON,
  SO.STYLE_REF STYLE_CODE,SO.STYLE_CODE STYLECODECUS, SO.GMT_TYPE,OE.GMT_TYPE_DESC(SO.GMT_TYPE) PRODUCTTYPE,GMT.COL_FC,GMT.SIZE_FC,GMT.PO_ITEM,SUM(GMT.QTY) QTY,
  SUM(NVL(GMT.QTY,0)/NVL(DECODE(SO.SAM_GMT,0,1,SO.SAM_GMT) ,1) * NVL(SO.SAM,0)) SUM_SAM,SO.SAM, SO.SCORE,
  GMT.PRICE, SUM(NVL(GMT.QTY,0)*NVL(GMT.PRICE,0)*NVL(SUB.EX_RATE,0)) AMOUNTUSD,SUB.EX_RATE RATESUB,
  SUB.CURRENCY_CODE SUBCURRENCY,SUM(NVL(GMT.QTY,0)*(NVL(GMT.PRICE,0)*NVL(SUB.EX_RATE,0))*NVL(SO.EXC_RATE,0)) AMOUNTBATH ,SO.CURRENCY_CODE,SO.EXC_RATE , SO.CUST_CATE,
  SO.CRE_BY, OE.USER_DESC(SO.CRE_BY) USERNAME,GET_PROMO(SO.SO_NO,SO.SO_YEAR,SO.OU_CODE)  Priority_type, SO.LEAD_TIME
  ,GET_BASE_STYLE(SO.OU_CODE, SO.STYLE_CODE, SO.BRAND_CODE, SO.CUST_CODE, SO.DEPT_CODE) BASE_STYLE, SO.SO_NO,
  GET_CONFIRM_MRD('NYG', SO.so_no, SO.so_year, 'G') SCMG_WK,
  GET_CONFIRM_MRD('NYG', SO.so_no, SO.so_year, 'K') SCMK_WK
  FROM OE_SO SO, OE_SO_SUB SUB, OE_SUB_GMT GMT ,OE_FACTORY FAC,OE_COLOR_SO COL,OE_SIZE_SO SIZ
  WHERE     SO.OU_CODE    ='N03'
and so.so_year in('16','17','18','19','20','21')
AND    SO.SHIPMENT_DATE   >= TO_DATE('01/01/2018','DD/MM/RRRR')
AND    SO.SHIPMENT_DATE   < TO_DATE('01/01/2020','DD/MM/RRRR')
  AND    SO.SO_STATUS    <>'C'
  AND    SUB.SUB_STATUS  <>'C'
  AND GMT.QTY >0
  AND SO.DEPT_CODE<>'GM'
  AND    SO.OU_CODE    = SUB.OU_CODE
  AND    SO.SO_YEAR    = SUB.SO_YEAR
  AND    SO.SO_NO    = SUB.SO_NO
  AND    SUB.OU_CODE     = GMT.OU_CODE
  AND    SUB.SO_YEAR     = GMT.SO_YEAR
  AND    SUB.SO_NO    = GMT.SO_NO
  AND    SUB.SUB_NO     = GMT.SUB_NO
  AND    GMT.OU_CODE     = COL.OU_CODE
  AND    GMT.SO_YEAR     = COL.SO_YEAR
  AND    GMT.SO_NO    = COL.SO_NO
  AND    GMT.COL_FC =COL.COL_FC
  AND    GMT.OU_CODE     = SIZ.OU_CODE
  AND    GMT.SO_YEAR     = SIZ.SO_YEAR
  AND    GMT.SO_NO    = SIZ.SO_NO
  AND    GMT.SIZE_FC =SIZ.SIZE_FC
  AND   SO.OU_CODE =FAC.OU_CODE(+)
  AND    SO.F_FACTORY=FAC.FACTORY_CODE(+)
  GROUP BY SUB.CUS_PO_NO,SUB.CUS_PO_DATE,SO.ORDER_TYPE,SO.SO_NO_DOC,SO.CRE_DATE,SUB.SUB_NO,SUB.SHIP_DATE,SO.DEPT_CODE ,
  SO.SHIPMENT_DATE,SO.CUST_CODE,SO.BRAND_CODE,
  SO.SEA_CODE, SO.STYLE_REF,SO.STYLE_CODE,SO.GMT_TYPE, GMT.COL_FC,GMT.SIZE_FC,GMT.PO_ITEM,sub.SHIPTO_ID,SO.SAM,SO.SCORE,
  GMT.PRICE,SUB.EX_RATE,SUB.CURRENCY_CODE,SO.CUST_CATE,SO.CURRENCY_CODE,SO.EXC_RATE,FAC.FACTORY_NAME,SO.CRE_BY, SO.LEAD_TIME,
  SO.so_no, SO.so_year,so.ou_code

  """

  df = pd.read_sql_query(sql, conn)

  # _filename = r"C:\QVD_DATA\COST_SPO\DATA_BATCH_PRODUCTION_COST.xlsx" 
  # df.to_excel(_filename, index=False)

  _filename = r"C:\IT_ONLY\NYG\SO_ORDER_NYG_2019.csv"
  df.to_csv(_filename,  index=False,encoding='utf-8-sig')
  _filename = r"C:\Qlikview_Report\Order\SO_ORDER_NYG_2018-2019.csv"
  df.to_csv(_filename, index=False,encoding='utf-8-sig')
  args = ['NYG','SPONYG0001',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)

  conn.close()

  print("CLS_SO_ORDER_NYG_2019 Done")
  ##sendLine("COMPLETE CLS_DATA_BATCH_PRODUCTION_COST")

###########################################

class CLS_SO_ORDER_NYG_2020(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    SO_ORDER_NYG_2020()


def SO_ORDER_NYG_2020():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  print("sart SO_ORDER_NYG_2020")
  args = ['NYG','SPONYG0002','SO_ORDER_NYG_2020.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  ##sendLine("START CLS_DATA_BATCH_PRODUCTION_COST")

  sql =""" SELECT 'NYG' BUSINESS,SUB.CUS_PO_NO,SUB.CUS_PO_DATE,SO.ORDER_TYPE,OE.ORDER_TYPE_DESC(SO.ORDER_TYPE)ORDERTYPE,FAC.FACTORY_NAME,SO.DEPT_CODE,
  SO.SO_NO_DOC,SO.CRE_DATE,SUB.SUB_NO,SUB.SHIP_DATE,TO_CHAR(SUB.SHIP_DATE,'YYYY') YEARWEEK,SO.SHIPMENT_DATE,
  Get_Ww(TO_CHAR(sub.ship_date,'dd/mm/yyyy')) WEEKNANYANGSUB,   Get_Ww(TO_CHAR(SO.SHIPMENT_DATE,'dd/mm/yyyy')) WEEKNANYANGHEAD,
  GET_WW_MONTH(TO_CHAR(SUB.SHIP_DATE,'dd/mm/yyyy')) MONTHWEEKNANYANG,  SO.CUST_CODE,OE.CUST_DESC(SO.CUST_CODE) CUSTOMERNAME,SO.BRAND_CODE,
  OE.BRAND_DESC('N03',SO.BRAND_CODE) BRANDNAME,OE.SHIPTO_COUNTRY(SHIPTO_ID) SHIPCOUNTRY, OE.SEASON_DESC('N03',SO.SEA_CODE) ORDERSEASON,
  SO.STYLE_REF STYLE_CODE,SO.STYLE_CODE STYLECODECUS, SO.GMT_TYPE,OE.GMT_TYPE_DESC(SO.GMT_TYPE) PRODUCTTYPE,GMT.COL_FC,GMT.SIZE_FC,GMT.PO_ITEM,SUM(GMT.QTY) QTY,
  SUM(NVL(GMT.QTY,0)/NVL(DECODE(SO.SAM_GMT,0,1,SO.SAM_GMT) ,1) * NVL(SO.SAM,0)) SUM_SAM,SO.SAM, SO.SCORE,
  GMT.PRICE, SUM(NVL(GMT.QTY,0)*NVL(GMT.PRICE,0)*NVL(SUB.EX_RATE,0)) AMOUNTUSD,SUB.EX_RATE RATESUB,
  SUB.CURRENCY_CODE SUBCURRENCY,SUM(NVL(GMT.QTY,0)*(NVL(GMT.PRICE,0)*NVL(SUB.EX_RATE,0))*NVL(SO.EXC_RATE,0)) AMOUNTBATH ,SO.CURRENCY_CODE,SO.EXC_RATE , SO.CUST_CATE,
  SO.CRE_BY, OE.USER_DESC(SO.CRE_BY) USERNAME,GET_PROMO(SO.SO_NO,SO.SO_YEAR,SO.OU_CODE)  Priority_type, SO.LEAD_TIME
  ,GET_BASE_STYLE(SO.OU_CODE, SO.STYLE_CODE, SO.BRAND_CODE, SO.CUST_CODE, SO.DEPT_CODE) BASE_STYLE, SO.SO_NO,
  GET_CONFIRM_MRD('NYG', SO.so_no, SO.so_year, 'G') SCMG_WK,
  GET_CONFIRM_MRD('NYG', SO.so_no, SO.so_year, 'K') SCMK_WK
  FROM OE_SO SO, OE_SO_SUB SUB, OE_SUB_GMT GMT ,OE_FACTORY FAC,OE_COLOR_SO COL,OE_SIZE_SO SIZ
  WHERE     SO.OU_CODE    ='N03'
  and  so.so_year >=to_char(sysdate,'YY')-6
  AND    SO.SHIPMENT_DATE   >= TO_DATE('01/01/2020','DD/MM/RRRR')
  AND    SO.SO_STATUS    <>'C'
  AND    SUB.SUB_STATUS  <>'C'
  AND GMT.QTY >0
  AND SO.DEPT_CODE<>'GM'
  AND    SO.OU_CODE    = SUB.OU_CODE
  AND    SO.SO_YEAR    = SUB.SO_YEAR
  AND    SO.SO_NO    = SUB.SO_NO
  AND    SUB.OU_CODE     = GMT.OU_CODE
  AND    SUB.SO_YEAR     = GMT.SO_YEAR
  AND    SUB.SO_NO    = GMT.SO_NO
  AND    SUB.SUB_NO     = GMT.SUB_NO
  AND    GMT.OU_CODE     = COL.OU_CODE
  AND    GMT.SO_YEAR     = COL.SO_YEAR
  AND    GMT.SO_NO    = COL.SO_NO
  AND    GMT.COL_FC =COL.COL_FC
  AND    GMT.OU_CODE     = SIZ.OU_CODE
  AND    GMT.SO_YEAR     = SIZ.SO_YEAR
  AND    GMT.SO_NO    = SIZ.SO_NO
  AND    GMT.SIZE_FC =SIZ.SIZE_FC
  AND   SO.OU_CODE =FAC.OU_CODE(+)
  AND    SO.F_FACTORY=FAC.FACTORY_CODE(+)
  GROUP BY SUB.CUS_PO_NO,SUB.CUS_PO_DATE,SO.ORDER_TYPE,SO.SO_NO_DOC,SO.CRE_DATE,SUB.SUB_NO,SUB.SHIP_DATE,SO.DEPT_CODE ,
  SO.SHIPMENT_DATE,SO.CUST_CODE,SO.BRAND_CODE,
  SO.SEA_CODE, SO.STYLE_REF,SO.STYLE_CODE,SO.GMT_TYPE, GMT.COL_FC,GMT.SIZE_FC,GMT.PO_ITEM,sub.SHIPTO_ID,SO.SAM,SO.SCORE,
  GMT.PRICE,SUB.EX_RATE,SUB.CURRENCY_CODE,SO.CUST_CATE,SO.CURRENCY_CODE,SO.EXC_RATE,FAC.FACTORY_NAME,SO.CRE_BY, SO.LEAD_TIME,
  SO.so_no, SO.so_year,so.ou_code
  """

  df = pd.read_sql_query(sql, conn)

  # _filename = r"C:\QVD_DATA\COST_SPO\DATA_BATCH_PRODUCTION_COST.xlsx" 
  # df.to_excel(_filename, index=False)

  _filename = r"C:\IT_ONLY\NYG\SO_ORDER_NYG_2020.csv"
  df.to_csv(_filename,  index=False,encoding='utf-8-sig')
  _filename = r"C:\Qlikview_Report\Order\SO_ORDER_NYG_2020-present.csv"
  df.to_csv(_filename, index=False, encoding='utf-8-sig')
  args = ['NYG','SPONYG0002',1]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()
  print("CLS_SO_ORDER_NYG_2020 Done")
  ##sendLine("COMPLETE CLS_DATA_BATCH_PRODUCTION_COST")

#############################################


threads = []

thread1 = CLS_SO_ORDER_NYG_2019();thread1.start();threads.append(thread1)
thread2 = CLS_SO_ORDER_NYG_2020();thread2.start();threads.append(thread2)

for t in threads:
    t.join()
print ("COMPLETE")

