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

my_nyg = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
my_nyg_user = 'NYGM'
my_nyg_pwd = 'NYGM'


QVDPath = r'C:\QVD_DATA\COM_GARMENT\COST SHEET'


def GM_CS_LIST():
  
  conn = cx_Oracle.connect(user=my_nyg_user, password=my_nyg_pwd,
                           dsn=my_nyg, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  sql = """ SELECT l.LIST_ID AS LIST_ID,
  h.TYPE_SHEET,
  h.CUSTOMER_NAME,
  h.SEASON_NAME,
  h.HEADER_DATE,
  h.BRAND_NAME,
  b.BU_NAME,
  h.NEW_TEAM,
  h.STYLE_NO,
  h.PRODUCT_TYPE_NAME,
  h.BASE_ON_SIZE,
  p.PLANT_NAME,
  h.ORDER_SIZE,
  l.PRODUCT_COLOR,
  h.FOR_PRICING,
  h.UNIT_FOR_PRICING,
  s.SALE_NAME||' - '||t.TEAM_NAME AS OLD_TEAM,
  sh.FS_GMM2     AS SAM,
  ft.BASE_ON     AS BASE_ON_EFF,
  ft.FOB_USPCS   AS FOB,
  l.DESCRIPTION  AS GAP_TURN,
  DECODE(h.FLAG_STATUS,'finish','final',h.FLAG_STATUS) FLAG_STATUS
   ,(SELECT SUBSTR(nl.NO_LIST_YEAR, 3, 2) || LPAD(nl.NO_LIST_MONTH, 2, '0') || LPAD(nl.NO_LIST_NUMBER, 4, '0')
  FROM GM_CS_NO_LIST nl WHERE l.NO_LIST_ID_REF = nl.NO_LIST_ID) COST_SHEET_NO,
  ft.AGENT_COMMISSION
FROM GM_CS_LIST l
INNER JOIN GM_CS_HEADER h
ON l.LIST_ID = h.LIST_ID_REF
INNER JOIN GM_CS_PLANT p
ON h.PLANT_ID_REF = p.PLANT_ID
INNER JOIN GM_CS_BU b
ON b.BU_ID = h.BU_ID_REF
INNER JOIN GM_CS_FOOTER ft
ON l.LIST_ID = ft.LIST_ID_REF
INNER JOIN GM_CS_SHEET sh
ON l.LIST_ID = sh.LIST_ID_REF
LEFT JOIN GM_CS_SALE s
ON s.SALE_ID = h.SALE_ID_REF
LEFT JOIN GM_CS_TEAM t
ON s.TEAM_ID_REF  = t.TEAM_ID
WHERE l.ACTIVE    = 'Y'
AND l.USED        = 'Y'
AND sh.GROUP_NAME = 'CMP' """

  df = pd.read_sql_query(sql, conn)

  df.to_excel(QVDPath + r'\GM_CS_LIST.xlsx', engine='xlsxwriter', index=False)


def GM_CS_SHEET():

  conn = cx_Oracle.connect(user=my_nyg_user, password=my_nyg_pwd,
                           dsn=my_nyg, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  sql = """ SELECT LIST_ID_REF AS LIST_ID, 
  GROUP_NAME,
  ITEM_NAME,
  UNIT_PRICE_NAME,      
  UNIT_ITEM_NAME,       
  FS_YDPC_0_USAGEPERPC, 
  FS_GMM2,
  FS_WIDTH,
  FP_COLOR,                                     
  FP_PRICE_0_UNITPRICEBYITEM,                    
  FP_UNIT_0_UNITPERPC,                          
  FP_UNIT_PRICE,                                
  'KG' UNIT_PER_PC_FOR_GROUP_FABRIC,             
  FP_UNIT_0_UNITPERPC UNIT_PER_PC_FOR_GROUP_ALL, 
  CREATED_AT,
  UNIT_FS_YDPC, 
  CUST_ITEM,    
  NVL((
  CASE WHEN GROUP_NAME = 'Fabric & Trim Spec' --'Fabric'||'&'||'Trim Spec'
    THEN FABRIC_CIF
    ELSE FP_HANDLING
  END), 0) AS CIF,
  UOM_CIF,      
  FS_ALLOWANCE, 
  NVL((
  CASE WHEN GROUP_NAME = 'Fabric & Trim Spec' --'Fabric'||'&'||'Trim Spec'
    THEN FP_HANDLING
    ELSE FABRIC_CIF
  END), 0) AS HANDLING
FROM GM_CS_SHEET
WHERE ACTIVE = 'Y' """

  df = pd.read_sql_query(sql, conn)

  df.to_excel(QVDPath + r'\GM_CS_SHEET.xlsx', engine='xlsxwriter', index=False)
  df.to_csv(QVDPath + r'\GM_CS_SHEET.csv', index=False)



GM_CS_LIST()
GM_CS_SHEET()
