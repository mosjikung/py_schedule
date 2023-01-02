import os
import io
import cx_Oracle
import numpy as np
import pandas as pd
import json
import requests
# from flask import jsonify
from pandas import json_normalize
from datetime import datetime, timedelta





oracle_client = r"C:\instantclient_19_5"
os.environ["ORACLE_HOME"] = oracle_client
os.environ["PATH"] = oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.TH8TISASCII"

my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")

# ต้องมี PPR_BOARD1_OTIF ก่อนเพื่อหา Target ของ SO
# ทำทุก 1 ชม. เริ่ม 7:00 ถึง 19:00

def sendLine(txt):
    url = 'https://notify-api.line.me/api/notify'
    token = 'xNt29CUwvuIQWzQOgmfpYgXY0dF7wcG46cSnQ7H2atF'
    headers = {
            'content-type':
            'application/x-www-form-urlencoded',
            'Authorization':'Bearer '+token
           }
    r = requests.post(url, headers=headers, data={'message': txt})

def PO_NYG():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  cursor.execute("""DELETE FROM OE_SO_READINESS_PO_V_TMP WHERE BU_CODE = 'NYG'  """)

  conn.commit()
  
  
  cursor.execute("""INSERT INTO OE_SO_READINESS_PO_V_TMP
                  SELECT CASE WHEN DECODE(PO_STATUS, 'A', 'APPROVED', '9', 'CLOSED', 'N', 'NORMAL') = 'CLOSED' THEN 'CLOSED'
                  WHEN REC_STATUS = 'CLOSED' THEN 'CLOSED'
                  WHEN REC_QTY >= PO_QTY THEN 'CLOSED'
                  ELSE 'PENDING' END CHK_CLOSED,  M.*
                  , NULL, NULL
              FROM OE_SO_READINESS_PO M
              WHERE 1 = 1
              AND PO_NO_DOC  NOT LIKE 'KL%' 
              AND PO_YEAR >= 19 """)
  
  conn.commit()
  
  cursor.execute("""DELETE FROM OE_SO_READINESS_PO_V_TMP2 WHERE BU_CODE = 'NYG' """)

  conn.commit()
  
  cursor.execute("""INSERT /*+ append */ INTO OE_SO_READINESS_PO_V_TMP2                  
                  SELECT M.CHK_CLOSED
                  ,M.BU_CODE
                  ,M.SO_YEAR
                  ,M.SO_NO
                  ,M.SO_NO_DOC
                  ,M.PO_NO
                  ,M.PO_YEAR
                  ,M.PO_NO_DOC
                  ,M.SUPPLIER_NAME
                  ,M.DEL_DATE
                  ,M.CONFIRM_DATE
                  ,M.RM_TYPE
                  ,M.ITEM_NAME
                  ,M.ITEM_COLOR
                  ,M.ITEM_SIZE
                  ,M.PO_QTY
                  ,M.REC_QTY
                  ,M.PURCHASER_NAME
                  ,M.PO_STATUS
                  ,M.REC_STATUS
                  ,M.PO_VI
                  ,M.MOQ
                  ,M.GROUP_CODE
                  ,M.ITEM_CODE
                  ,M.BOMDATE
                  ,M.PO_DATE
                  ,M.DELAY_NOTE
                  ,M.USAGE
                  ,M.DATA_TYPE
                  ,M.LAST_REC_DATE
                  ,M.OTP_DATE
                  ,M.SHIPMENT_DATE
                  ,S.ORDER_TYPE
                  ,S.TARGET_BY_RDD
                  ,TO_NUMBER(GET_READINESS_FIND_TARGET(S.RDD_WK,4)) TARGET_PACK
                  FROM OE_SO_READINESS_PO_V_TMP M, PPR_BOARD1_OTIF S
                  WHERE S.BU = M.BU_CODE AND S.SO_NO_DOC = M.SO_NO_DOC
                  AND PO_NO_DOC  NOT LIKE 'KL%' 
                  AND S.RDD_WK >= '202040'
                  and m.bu_code = 'NYG'
                 """)
  
  
  conn.commit()
  
  sql = """SELECT M.* FROM OE_SO_READINESS_PO_V_TMP2 M WHERE BU_CODE = 'NYG' """
            
  df = pd.read_sql_query(sql, conn)
  conn.close()
  
  sendLine('PO NYG had ' +  f"{df.shape[0]:,}" + ' Records')
  
  
def PO_TRM():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  cursor.execute("""DELETE FROM OE_SO_READINESS_PO_V_TMP WHERE BU_CODE = 'TRM'  """)

  conn.commit()
  
  
  cursor.execute("""INSERT INTO OE_SO_READINESS_PO_V_TMP
                  SELECT CASE WHEN DECODE(PO_STATUS, 'A', 'APPROVED', '9', 'CLOSED', 'N', 'NORMAL') = 'CLOSED' THEN 'CLOSED'
                  WHEN REC_STATUS = 'CLOSED' THEN 'CLOSED'
                  WHEN REC_QTY >= PO_QTY THEN 'CLOSED'
                  ELSE 'PENDING' END CHK_CLOSED,  M.*
                  , NULL, NULL
              FROM TRM.OE_SO_READINESS_PO@TRM.WORLD M
              WHERE 1 = 1
              AND PO_NO_DOC  NOT LIKE 'KL%' 
              AND PO_YEAR >= 19 """)
  
  conn.commit()
  
  cursor.execute("""DELETE FROM OE_SO_READINESS_PO_V_TMP2 WHERE BU_CODE = 'TRM' """)

  conn.commit()
  
  cursor.execute("""INSERT /*+ append */ INTO OE_SO_READINESS_PO_V_TMP2                  
                  SELECT M.CHK_CLOSED
                  ,M.BU_CODE
                  ,M.SO_YEAR
                  ,M.SO_NO
                  ,M.SO_NO_DOC
                  ,M.PO_NO
                  ,M.PO_YEAR
                  ,M.PO_NO_DOC
                  ,M.SUPPLIER_NAME
                  ,M.DEL_DATE
                  ,M.CONFIRM_DATE
                  ,M.RM_TYPE
                  ,M.ITEM_NAME
                  ,M.ITEM_COLOR
                  ,M.ITEM_SIZE
                  ,M.PO_QTY
                  ,M.REC_QTY
                  ,M.PURCHASER_NAME
                  ,M.PO_STATUS
                  ,M.REC_STATUS
                  ,M.PO_VI
                  ,M.MOQ
                  ,M.GROUP_CODE
                  ,M.ITEM_CODE
                  ,M.BOMDATE
                  ,M.PO_DATE
                  ,M.DELAY_NOTE
                  ,M.USAGE
                  ,M.DATA_TYPE
                  ,M.LAST_REC_DATE
                  ,M.OTP_DATE
                  ,M.SHIPMENT_DATE
                  ,S.ORDER_TYPE
                  ,S.TARGET_BY_RDD
                  ,TO_NUMBER(GET_READINESS_FIND_TARGET(S.RDD_WK,4)) TARGET_PACK
                  FROM OE_SO_READINESS_PO_V_TMP M, PPR_BOARD1_OTIF S
                  WHERE S.BU = M.BU_CODE AND S.SO_NO_DOC = M.SO_NO_DOC
                  AND PO_NO_DOC  NOT LIKE 'KL%' 
                  AND S.RDD_WK >= '202040'
                  and m.bu_code = 'TRM'
                 """)
  
  
  conn.commit()
  
  sql = """SELECT M.* FROM OE_SO_READINESS_PO_V_TMP2 M WHERE BU_CODE = 'TRM' """
            
  df = pd.read_sql_query(sql, conn)
  conn.close()
  
  sendLine('PO TRM had ' +  f"{df.shape[0]:,}" + ' Records')
  



def PO_GW():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  cursor.execute("""DELETE FROM OE_SO_READINESS_PO_V_TMP WHERE BU_CODE = 'GW'  """)

  conn.commit()
  
  
  cursor.execute("""INSERT /*+ append */ INTO OE_SO_READINESS_PO_V_TMP
                  SELECT CASE WHEN DECODE(PO_STATUS, 'A', 'APPROVED', '9', 'CLOSED', 'N', 'NORMAL') = 'CLOSED' THEN 'CLOSED'
                  WHEN REC_STATUS = 'CLOSED' THEN 'CLOSED'
                  WHEN REC_QTY >= PO_QTY THEN 'CLOSED'
                  ELSE 'PENDING' END CHK_CLOSED,  M.*
                  , NULL, NULL
              FROM NYGM.OE_SO_READINESS_PO@NGWSP.WORLD M
              WHERE 1 = 1
              AND PO_NO_DOC  NOT LIKE 'KL%' 
              AND PO_YEAR >= 19 """)
  
  conn.commit()
  
  cursor.execute("""DELETE FROM OE_SO_READINESS_PO_V_TMP2 WHERE BU_CODE = 'GW' """)

  conn.commit()
  
  cursor.execute("""INSERT /*+ append */ INTO OE_SO_READINESS_PO_V_TMP2                  
                  SELECT M.CHK_CLOSED
                  ,M.BU_CODE
                  ,M.SO_YEAR
                  ,M.SO_NO
                  ,M.SO_NO_DOC
                  ,M.PO_NO
                  ,M.PO_YEAR
                  ,M.PO_NO_DOC
                  ,M.SUPPLIER_NAME
                  ,M.DEL_DATE
                  ,M.CONFIRM_DATE
                  ,M.RM_TYPE
                  ,M.ITEM_NAME
                  ,M.ITEM_COLOR
                  ,M.ITEM_SIZE
                  ,M.PO_QTY
                  ,M.REC_QTY
                  ,M.PURCHASER_NAME
                  ,M.PO_STATUS
                  ,M.REC_STATUS
                  ,M.PO_VI
                  ,M.MOQ
                  ,M.GROUP_CODE
                  ,M.ITEM_CODE
                  ,M.BOMDATE
                  ,M.PO_DATE
                  ,M.DELAY_NOTE
                  ,M.USAGE
                  ,M.DATA_TYPE
                  ,M.LAST_REC_DATE
                  ,M.OTP_DATE
                  ,M.SHIPMENT_DATE
                  ,S.ORDER_TYPE
                  ,S.TARGET_BY_RDD
                  ,TO_NUMBER(GET_READINESS_FIND_TARGET(S.RDD_WK,4)) TARGET_PACK
                  FROM OE_SO_READINESS_PO_V_TMP M, PPR_BOARD1_OTIF S
                  WHERE S.BU = M.BU_CODE AND S.SO_NO_DOC = M.SO_NO_DOC
                  AND PO_NO_DOC  NOT LIKE 'KL%' 
                  AND S.RDD_WK >= '202040'
                  and m.bu_code = 'GW'
                 """)
  
  
  conn.commit()
  
  sql = """SELECT M.* FROM OE_SO_READINESS_PO_V_TMP2 M WHERE BU_CODE = 'GW' """
            
  df = pd.read_sql_query(sql, conn)
  conn.close()
  
  sendLine('PO GW had ' +  f"{df.shape[0]:,}" + ' Records')
  
  
  
def PO_VN():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  cursor.execute("""DELETE FROM OE_SO_READINESS_PO_V_TMP WHERE BU_CODE = 'NYV'  """)

  conn.commit()
  
  
  cursor.execute("""INSERT /*+ append */ INTO OE_SO_READINESS_PO_V_TMP
                  SELECT CASE WHEN DECODE(PO_STATUS, 'A', 'APPROVED', '9', 'CLOSED', 'N', 'NORMAL') = 'CLOSED' THEN 'CLOSED'
                  WHEN REC_STATUS = 'CLOSED' THEN 'CLOSED'
                  WHEN REC_QTY >= PO_QTY THEN 'CLOSED'
                  ELSE 'PENDING' END CHK_CLOSED,  M.*
                  , NULL, NULL
              FROM VN.OE_SO_READINESS_PO@VNSQPROD.WORLD M
              WHERE 1 = 1
              AND PO_NO_DOC  NOT LIKE 'KL%' 
              AND PO_YEAR >= 19 """)
  
  conn.commit()
  
  cursor.execute("""DELETE FROM OE_SO_READINESS_PO_V_TMP2 WHERE BU_CODE = 'NYV' """)

  conn.commit()
  
  cursor.execute("""INSERT /*+ append */ INTO OE_SO_READINESS_PO_V_TMP2                  
                  SELECT M.CHK_CLOSED
                  ,M.BU_CODE
                  ,M.SO_YEAR
                  ,M.SO_NO
                  ,M.SO_NO_DOC
                  ,M.PO_NO
                  ,M.PO_YEAR
                  ,M.PO_NO_DOC
                  ,M.SUPPLIER_NAME
                  ,M.DEL_DATE
                  ,M.CONFIRM_DATE
                  ,M.RM_TYPE
                  ,M.ITEM_NAME
                  ,M.ITEM_COLOR
                  ,M.ITEM_SIZE
                  ,M.PO_QTY
                  ,M.REC_QTY
                  ,M.PURCHASER_NAME
                  ,M.PO_STATUS
                  ,M.REC_STATUS
                  ,M.PO_VI
                  ,M.MOQ
                  ,M.GROUP_CODE
                  ,M.ITEM_CODE
                  ,M.BOMDATE
                  ,M.PO_DATE
                  ,M.DELAY_NOTE
                  ,M.USAGE
                  ,M.DATA_TYPE
                  ,M.LAST_REC_DATE
                  ,M.OTP_DATE
                  ,M.SHIPMENT_DATE
                  ,S.ORDER_TYPE
                  ,S.TARGET_BY_RDD
                  ,TO_NUMBER(GET_READINESS_FIND_TARGET(S.RDD_WK,4)) TARGET_PACK
                  FROM OE_SO_READINESS_PO_V_TMP M, PPR_BOARD1_OTIF S
                  WHERE S.BU = 'VN' AND S.SO_NO_DOC = M.SO_NO_DOC
                  AND PO_NO_DOC  NOT LIKE 'KL%' 
                  AND S.RDD_WK >= '202040'
                  and m.bu_code = 'NYV'
                 """)
  
  
  conn.commit()
  
  sql = """SELECT M.* FROM OE_SO_READINESS_PO_V_TMP2 M WHERE BU_CODE = 'NYV' """
            
  df = pd.read_sql_query(sql, conn)
  conn.close()
  
  sendLine('PO NYV had ' +  f"{df.shape[0]:,}" + ' Records')
  
  
def create_otp_nike():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  try:
    cursor.execute(""" DROP TABLE OTP_FABRIC_NIKE_TMP PURGE """)
    conn.commit()
  except:
    print('NYG Error')
    
  cursor.execute(""" CREATE TABLE OTP_FABRIC_NIKE_TMP
                    AS
                    SELECT M.*
                    FROM OTP_FABRIC_NIKE M""")
  
  conn.commit()
  conn.close()
  
def create_otp_nonnike():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  try:
    cursor.execute(""" DROP TABLE OTP_FABRIC_NONNIKE_TMP PURGE """)
    conn.commit()
  except:
    print('Error')
    
  cursor.execute(""" CREATE TABLE OTP_FABRIC_NONNIKE_TMP
                    AS
                    SELECT M.*
                    FROM OTP_FABRIC_NONNIKE M""")
  
  conn.commit()
  conn.close()
  
def create_otp_scm():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  try:
    cursor.execute(""" DROP TABLE OTP_FABRIC_SCM_TMP PURGE """)
    conn.commit()
  except:
    print('Error')
    
  cursor.execute(""" CREATE TABLE OTP_FABRIC_SCM_TMP
                    AS
                    SELECT M.*
                    FROM OTP_FABRIC_SCM M""")
  
  conn.commit()
  conn.close()
  
def create_otp_vn():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  try:
    cursor.execute(""" DROP TABLE OTP_FABRIC_VN_TMP PURGE """)
    conn.commit()
  except:
    print('Error')
    
  cursor.execute(""" CREATE TABLE OTP_FABRIC_VN_TMP
                    AS
                    SELECT M.*
                    FROM OTP_FABRIC_VN M""")
  
  conn.commit()
  conn.close()
  
  
def create_otp_sew():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  
  try:
    cursor.execute(""" DROP TABLE OTP_SEW_NIKE_TMP PURGE """)
    conn.commit()
  except:
    print('Error')
    
  cursor.execute(""" CREATE TABLE OTP_SEW_NIKE_TMP
                    AS
                    SELECT M.*
                    FROM OTP_SEW_NIKE M""")
  conn.commit()
  
  try:
    cursor.execute(""" DROP TABLE OTP_SEW_NONNIKE_TMP PURGE """)
    conn.commit()
  except:
    print('Error')
    
  cursor.execute(""" CREATE TABLE OTP_SEW_NONNIKE_TMP
                    AS
                    SELECT M.*
                    FROM OTP_SEW_NONNIKE M""")
  conn.commit()
  
  try:
    cursor.execute(""" DROP TABLE OTP_SEW_VN_TMP PURGE """)
    conn.commit()
  except:
    print('Error')
    
  cursor.execute(""" CREATE TABLE OTP_SEW_VN_TMP
                    AS
                    SELECT M.*
                    FROM OTP_SEW_VN M""")
  conn.commit()
  conn.close()
  

def create_otp_pack():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  
  try:
    cursor.execute(""" DROP TABLE OTP_PACK_NIKE_TMP PURGE """)
    conn.commit()
  except:
    print('Error')
    
  cursor.execute(""" CREATE TABLE OTP_PACK_NIKE_TMP
                    AS
                    SELECT M.*
                    FROM OTP_PACK_NIKE M""")
  conn.commit()
  
  try:
    cursor.execute(""" DROP TABLE OTP_PACK_NONNIKE_TMP PURGE """)
    conn.commit()
  except:
    print('Error')
    
  cursor.execute(""" CREATE TABLE OTP_PACK_NONNIKE_TMP
                    AS
                    SELECT M.*
                    FROM OTP_PACK_NONNIKE M""")
  conn.commit()
  
  try:
    cursor.execute(""" DROP TABLE OTP_PACK_VN_TMP PURGE """)
    conn.commit()
  except:
    print('Error')
    
  cursor.execute(""" CREATE TABLE OTP_PACK_VN_TMP
                    AS
                    SELECT M.*
                    FROM OTP_PACK_VN M""")
  conn.commit()
  conn.close()
 


  
def process():
  PO_NYG()
  PO_TRM()
  PO_GW()
  PO_VN()

  create_otp_nike()
  create_otp_nonnike()
  create_otp_vn()
  create_otp_scm()
  create_otp_sew()
  create_otp_pack()
  
  return None, 200
  


def keep_shipment():
    now = datetime.now()
    date_time = now.strftime("%Y-%m-%d")
    print("date and time:",date_time)

    my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
    conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
    cursor = conn.cursor()

    sql = """ select m.*
from OE_SO_READINESS_PLANSHIP_V_TMP m """

    df = pd.read_sql_query(sql, conn)


    df.to_csv(r'C:\QVD_DATA\COM_GARMENT\ControlRoom\board7_{}.csv'.format(date_time), index=False, encoding='utf-8-sig')


keep_shipment()
process()

