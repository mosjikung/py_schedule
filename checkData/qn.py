#-*-coding: utf-8 -*-
import cx_Oracle
import openpyxl
from openpyxl.styles import PatternFill, Border, Side, Alignment, Protection, Font
import os
import datetime
import threading
import smtplib
import ssl
import sys
import numpy as np
import pandas as pd
from datetime import datetime
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email import encoders


oracle_client = "C:\instantclient_19_5"
os.environ["ORACLE_HOME"] = oracle_client
os.environ["PATH"] = oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.TH8TISASCII"


def chkprice(itm):
  
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="sf5", password="omsf5",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  # sql = """ SELECT M.*
  # FROM(
  #     SELECT M.ITEM_CODE, M.ITEM_CODE_SHADE, M.DYE_CHARGE_FABRIC, M.DYE_TYPE_CAL, D.DYE_FABRIC_SHADE, D.DYE_COST, D.DYE_LOST, D.DYECOST_ACTIVE, M.ITEM_CATEGORY, M.ITEM_STRUCTURE, DEV_NO
  #     FROM
  #     (SELECT SUBSTR(ITEM_CODE, 1, LENGTH(ITEM_CODE)-2) as ITEM_CODE, ITEM_CODE AS ITEM_CODE_SHADE, DYE_TYPE, TRIM(DYE_CHARGE_FABRIC) DYE_CHARGE_FABRIC, 10 AS OVH_COST, DEV_NO,
  #      CASE WHEN DYE_TYPE IN('P', 'T') THEN 'P' ELSE 'Y' END DYE_TYPE_CAL,
  #      CASE WHEN DYE_TYPE='P' OR DYE_TYPE='T' THEN 'P' ELSE NULL END IS_PIECE_DYE,
  #      CASE WHEN DYE_TYPE NOT IN('P', 'T') THEN 'Y' ELSE NULL END IS_YARN_DYE,
  #      ITEM_DESC, ITEM_CATEGORY, ITEM_STRUCTURE, O_CONTENT,  O_YARN_COUNT,
  #      O_MACHINE_GROUP, O_GAUGE,
  #      O_FN_TUBULAR as WIDTH_TUBULAR,
  #      O_FN_OPEN as WIDTH_OPEN, O_FN_GM,
  #      MAX(O_RAW_WHITE) O_RAW_WHITE
  #      FROM FMIT_ITEM M
  #      WHERE ITEM_CODE IS NOT NULL
  #      AND SUBSTR(ITEM_CODE, LENGTH(ITEM_CODE)-1, 1) IN('A', 'B')
  #      AND SUBSTR(ITEM_CODE, 1, 1)=('F')
  #      AND ACTIVE='Y'
  #      AND SUBSTR(ITEM_CODE, LENGTH(ITEM_CODE), 1) <> 'Z'
  #      AND ITEM_CATEGORY NOT IN('CANCEL')
  #      AND ITEM_CODE <> 'FAA'
  #      GROUP BY SUBSTR(ITEM_CODE, 1, LENGTH(ITEM_CODE)-2),  ITEM_CODE, DYE_TYPE, TRIM(DYE_CHARGE_FABRIC), DEV_NO,
  #      ITEM_DESC, ITEM_CATEGORY, ITEM_STRUCTURE, O_CONTENT,  O_YARN_COUNT,
  #      O_MACHINE_GROUP, O_GAUGE,
  #      O_FN_TUBULAR,
  #      O_FN_OPEN, O_FN_GM,
  #      O_RAW_WHITE
  #      ORDER BY ITEM_CODE) M INNER JOIN(SELECT TRIM(DYE_FABRIC_TYPE)  AS DYE_CHARGE_FABRIC,
  #                                       DYE_FABRIC_SHADE,
  #                                       MAX(DYE_COST) DYE_COST,
  #                                       MAX(DYE_LOST) DYE_LOST,
  #                                       ACTIVE AS DYECOST_ACTIVE
  #                                       FROM  ERP_DYE_CHARGE M
  #                                       WHERE DYE_FABRIC_SHADE IN('A', 'W')
  #                                       GROUP BY TRIM(DYE_FABRIC_TYPE), DYE_FABRIC_SHADE, ACTIVE
  #                                       ORDER BY 1) D ON M.DYE_CHARGE_FABRIC=D.DYE_CHARGE_FABRIC) M
  # WHERE   ITEM_CODE = '{itm}' """.format(itm=itm)

  # df = pd.read_sql_query(sql, conn)
  # df.fillna("", inplace=True)

  # df = df.applymap(lambda x: x.encode('unicode_escape').
  #                  decode('utf-8') if isinstance(x, str) else x)

  # ITEM_CATEGORY = df.loc[0, 'ITEM_CATEGORY'].strip()

  sql = """SELECT DISTINCT  Y.YARN_ITEM, 
          CASE WHEN I.ACTIVE ='Y' THEN Y.ITEM_CONTAIN ELSE 0 END ITEM_CONTAIN, 
          I.UNIT_PRICE ,
          CASE WHEN I.ACTIVE ='Y' AND I.UNIT_PRICE > 10 THEN 'Y' ELSE 'N' END NO_PRICE, 
          I.RM_COST,I.YARN_DYE_COST DYE_COST,I.PER_LOSS ,i.ACTIVE
          FROM SF5.DFIT_KNIT_YARN Y, SF5.FMIT_YARN_PRICE I
          WHERE I.ITEM_CODE = Y.YARN_ITEM
          AND Y.ITEM_CODE LIKE 'F%'
          AND Y.ITEM_CODE IN ('{itm}A0','{itm}B0')
          AND SUBSTR(Y.ITEM_CODE,LENGTH(Y.ITEM_CODE)-1,1) IN ('A','B')  
          ORDER BY  Y.YARN_ITEM """.format(itm=itm)

  dfChkPrice = pd.read_sql_query(sql, conn)
  dfChkPrice.fillna("", inplace=True)
  dfChkPrice = dfChkPrice.applymap(lambda x: x.encode('unicode_escape').
                   decode('utf-8') if isinstance(x, str) else x)
  # print(dfChkPrice)

  # YARN_CONTAIN_PERCENT <> 100 -> No Price
  # YARN_ITEM NO PRICE -> No Price
  for index, row in dfChkPrice.iterrows():
    if(row['UNIT_PRICE']==0):
      print('Yarn ' + row['YARN_ITEM'] + ' no Price')


chkprice('FD6GNTMR109')


