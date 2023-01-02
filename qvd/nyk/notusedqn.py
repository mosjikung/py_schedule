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
os.environ["ORACLE_HOME"] = oracle_client
os.environ["PATH"] = oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.TH8TISASCII"


class CLS_DFIT_QN_REH(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      DFIT_QN_REH()


def DFIT_QN_REH():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="sf5", password="omsf5",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

#   sql = """SELECT M.ORDER_NO QN_NO, TRUNC(ORDER_DATE) QN_DATE, TO_CHAR(ORDER_DATE,'RRRR') AS QN_YEAR, TO_CHAR(ORDER_DATE,'IYYYIW') AS QN_WW, 
# CUSTOMER_ID , CUSTOMER_NAME,  BF_TYPE, C_TYPE, CUSTOMER_END, 
# SALES_ID, SALE_NAME, PRICE_TYPE, COUNTRY, TEAM_NAME, 
# CONVERT_QN, TRUNC(CONVERT_DATE) CONVERT_QN_DATE,  TO_CHAR(ORDER_DATE,'IYYYIW') AS CONVERT_QN_WW, 
# QN_TYPE, QN_TYPE_SUB,RQN_APPROVED QN_STATUS
# FROM DFIT_QN_REH M
# WHERE ORDER_NO >= 618692 """
  sql = """ select m.*
  ,(SELECT EXPIRED_DATE FROM DFIC_MORDER O WHERE O.ORDER_NO = M.ORDER_NO) EXPIRE_DATE
  ,(SELECT max(d.LINE_REMARK) FROM DFIT_DORDER D WHERE D.ORDER_NO = M.ORDER_NO) LINE_REMARK
from NYBI.DFIT_QN_REH_V m """

  cursor.execute(sql)
  _csv = r"C:\QlikView\QVD\QN\DFIT_QN_REH.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)

  conn.close()
  print("COMPLETE DFIT_QN_REH")
  


class CLS_DFIT_QN_RED(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      DFIT_QN_RED()


def DFIT_QN_RED():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="sf5", password="omsf5",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

#   sql = """SELECT D.ORDER_NO QN_NO, LINE_ID,  ITEM_CODE, ITEM_TYPE,  ITEM_WEIGHT, ACTIVE_STATUS, FABRIC_SHADE, D.PRICE_TYPE AS ITEM_PRICE_TYPE, TOPDYED_COLOR, 
# ITEM_PRICE AS COST_PRICE, ORIGINAL_PRICE AS UNIT_PRICE,
# H.CURRENCY, H.EXCHANGE_RATE, ORIGINAL_PRICE * EXCHANGE_RATE AS UNIT_PRICE_BAHT,
# SHADE_DESC, SHADE_L, SHADE_M, SHADE_D, SHADE_S, USER_REMARK, FIRST_BATCH, MATCH_CONTRAST, BODY_TRIM, UOM, TOTAL_QTY
# FROM DFIT_QN_RED D, DFIT_QN_REH H
# WHERE D.ORDER_NO >= 618692 
# AND D.ORDER_NO = H.ORDER_NO
# AND D.ACTIVE_STATUS <> 4
# ORDER BY LINE_ID DESC """

  sql = """  SELECT m.* 
  ,(SELECT EXPIRED_DATE FROM DFIC_MORDER O WHERE O.ORDER_NO = M.ORDER_NO) EXPIRE_DATE
  FROM NYBI.DFIT_QN_RED_V m """

  cursor.execute(sql)

  _csv = r"C:\QlikView\QVD\QN\DFIT_QN_RED.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)

  conn.close()
  print("COMPLETE DFIT_QN_RED")
  


class CLS_DFIT_QN_OPT(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      DFIT_QN_OPT()


def DFIT_QN_OPT():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="sf5", password="omsf5",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  sql = """ SELECT M.* FROM DFIT_QN_OPT M  """

  cursor.execute(sql)

  _csv = r"C:\QlikView\QVD\QN\DFIT_QN_OPT.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)
  conn.close()
  print("COMPLETE DFIT_QN_OPT")


class CLS_DFIT_QN_RED_PLAN_V(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      DFIT_QN_RED_PLAN_V()


def DFIT_QN_RED_PLAN_V():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="sf5", password="omsf5",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  # cursor = conn.cursor()

  sql = """ SELECT PLAN_SEQ, 
      ORDER_NO, 
      LINE_ID, 
      PLAN_TYPE, 
      ITEM_1, 
      PERIOD_WEEK, 
      QUANTITY, 
      USED, 
      PENDING 
      FROM NYBI.DFIT_QN_RED_PLAN_V
      WHERE PERIOD_WEEK >= '2020/01'
      """

  _csv = r"C:\QlikView\QVD\QN\DFIT_QN_RED_PLAN_V.csv"

  df = pd.read_sql_query(sql, conn)
  # print(df)

  df.to_csv(_csv, index=False)

  # with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
  #   csv_writer = csv.writer(csv_file)
  #   csv_writer.writerow([i[0] for i in cursor.description])  # write headers
  #   csv_writer.writerows(cursor)
  conn.close()
  print("COMPLETE DFIT_QN_RED_PLAN_V")

#############################################
threads = []

thread1 = CLS_DFIT_QN_REH()
thread1.start()
threads.append(thread1)

thread2 = CLS_DFIT_QN_RED()
thread2.start()
threads.append(thread2)

thread3 = CLS_DFIT_QN_OPT()
thread3.start()
threads.append(thread3)


# Script Error คำนวนใน Query ดึงไม่ได้ ใช้ QVD แทน
# thread4 = CLS_DFIT_QN_RED_PLAN_V()
# thread4.start()
# threads.append(thread4)

for t in threads:
    t.join()
print("COMPLETE")
