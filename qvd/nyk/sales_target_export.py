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

class CLS_QVD_SALE_TARGET_OE_ACTUAL(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    QVD_SALE_TARGET_OE_ACTUAL()


def QVD_SALE_TARGET_OE_ACTUAL():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  printttime('SALES_TARGET_OE_ACTUAL Start')

  sql = """
        SELECT  m.RNK,
                m.SO_NO, m.SO_LINE, TO_CHAR(m.SO_NO_DATE,'DD/MM/RRRR') as SO_NO_DATE, m.SO_MONTH, m.PL_COLORLAB, m.COLOR_CODE, m.FINISHING_TYPE, m.OE_CUS_CODE, m.OE_CUS_NAME, m.BUYYER, 
                m.ORDER_TYPE_NAME, m.ORDER_TYPE_DESC, m.STYLE, m.NYF_CUS_PO, m.SO_FG_YEAR, m.SO_FG_WEEK, m.OE_SALE_ID, m.OE_SALE_NAME, m.OE_TEAM_NAME, m.OE_DIVISION_NAME,
                m.OE_FOB_CODE, m.OE_ORDER, m.SO_STATUS, m.OE_ORDER_PRICE, m.ITEM_CODE, m.OE_SO_ITEM, m.OE_SO_ITEM_GREY, m.COLOR_SHADE, m.SO_QN, m.OE_CONVERSION, 
                m.OE_CONVERSION_RATE, m.OE_SO_UOM, m.CUST_ORDER_QTY, m.SALE_FGWEEK, m.RU_CONFIRM_FG, m.OE_AMOUNT, m.YARN_COST, m.PRINT_COST, m.FINISING_COST, 
                m.TOTAL_COST_KG, m.YARN_COST_AMOUNT, m.PRINT_COST_AMOUNT, m.FINISING_COST_AMOUNT, m.TOTAL_COST_AMOUNT, m.OE_GAP, m.ITEM_DESC, m.ITEM_CATEGORY_SALES, m.ITEM_CATEGORY, 
                m.ITEM_STRUCTURE, m.MACHINE_GROUP, m.O_FN_OPEN, m.O_FN_TUBULAR, m.O_FN_YARD, m.O_FN_GM, m.O_YARN_COUNT, m.O_GAUGE, m.O_MAT_CONS, m.ACT_ORDER_QTY, 
                m.SO_RESERVE, m.SO_BILL_REF, m.SO_TYPE, m.OE_DIVISION_NAME_OLD, m.OE_TEAM_NAME_OLD, m.ORDER_KGS
              , d1.rnk as d1_rnk, D1.ORDER_NUMBER, D1.KNIT_YEAR, D1.KNIT_WEEK, D1.KNIT_QTY, D1.USED_QTY, D1.USED_KGs, D1.FOB_POINT_CODE, D1.FG_YEAR, D1.FG_WEEK, D1.FG_YEARWEEK,D1.REQ_FGTYPE, D1.PENDING
              , d2.ACT_KNIT_YEAR,d2.ACT_KNIT_WEEK, d2.ACT_KNIT_QTY,d2.SO_LINE_ID,d2.ACT_FG_WEEK
              , NVL(d2.ACT_KNIT_QTY,0) - NVL(D1.USED_KGS,0) as PENDING_ACTUAL
              from export_oe_actual m, (select RANK() OVER(
                    PARTITION BY SO_NO, ITEM_CODE
                    ORDER BY KNIT_YEAR, KNIT_WEEK) RNK
              ,D.SO_NO, D.ITEM_CODE,
                D.ORDER_NUMBER, D.KNIT_YEAR, D.KNIT_WEEK, D.KNIT_QTY, D.USED_QTY, D.USED_KGs, D.FOB_POINT_CODE, D.FG_YEAR, D.FG_WEEK, D.FG_YEARWEEK,D.REQ_FGTYPE
                , NVL(D.KNIT_QTY,0) - NVL(D.USED_KGs,0) AS PENDING
                          FROM SF5.DUMMY_MAIN_RESERVE_D D
                          where  knit_year >= 2021
                          AND  D.CONF_RU_TYPE='CONFIRM_WEEK') d1,
                          (
                          SELECT RANK() OVER(
                    PARTITION BY SO_NO, ITEM_CODE
                    ORDER BY ACT_KNIT_YEAR, ACT_KNIT_WEEK) RNK, so_no, item_code, ACT_KNIT_YEAR, ACT_KNIT_WEEK, ACT_KNIT_QTY,SO_LINE_ID,ACT_FG_WEEK
              FROM (
              SELECT  KNIT_MC_SO SO_NO, KNIT_ITEM_CODE ITEM_CODE, KNIT_MC_YEAR ACT_KNIT_YEAR, KNIT_MC_WW ACT_KNIT_WEEK, SUM(KNIT_MC_KG) ACT_KNIT_QTY ,SO_LINE_ID,
                        GET_KNIT_FG(KNIT_MC_SO,TO_NUMBER(LTRIM(KNIT_MC_YEAR||LTRIM(TO_CHAR(KNIT_MC_WW,'09')))))  ACT_FG_WEEK
              FROM DFIT_KNIT_SOKP
              where KNIT_MC_YEAR >= 2021
              GROUP BY KNIT_MC_SO , KNIT_ITEM_CODE , KNIT_MC_YEAR , KNIT_MC_WW, SO_LINE_ID
              ORDER BY 1,2,3,4 ) D2
                          ) d2
              where m.so_no = d1.so_no(+)
              and m.OE_SO_ITEM_GREY = d1.item_code(+)
              and m.rnk = d1.rnk(+)
              and m.so_no = d2.so_no(+)
              and m.OE_SO_ITEM_GREY = d2.item_code(+)
              and m.rnk = d2.rnk(+)

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\QVD_DATA\PRO_NYK\SALES_TARGET_OE_ACTUAL.csv', index=False,encoding='utf-8-sig')


  conn.close()

  printttime('SALES_TARGET_OE_ACTUAL Complete')
###########################################


threads = []

thread1 = CLS_QVD_SALE_TARGET_OE_ACTUAL() ;thread1.start() ;threads.append(thread1)


for t in threads:
    t.join()



