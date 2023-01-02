import cx_Oracle
import csv
import os
from pathlib import Path
import requests
from datetime import datetime
import threading
import time
import openpyxl
import mysql.connector
from openpyxl.styles import PatternFill, Border, Side, Alignment, Protection, Font
import numpy as np
import pandas as pd


oracle_client = "C:\instantclient_19_5"
os.environ["ORACLE_HOME"]=oracle_client
os.environ["PATH"]=oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"]="AMERICAN_AMERICA.TH8TISASCII"

###########################################


def exportpeople():
  my_dsn2 = cx_Oracle.makedsn("172.16.6.80", port=1521, sid="NYTG")
  conn2 = cx_Oracle.connect(user="HRMS", password="HRMS", dsn=my_dsn2, encoding = "UTF-8", nencoding = "UTF-8")

  sql = """ SELECT M.EMP, M.NA_TH, NA_EN, NIC, MAIL, BU, DEPT, POS_TH, POS_EN, TRIM(NVL(D.PHONE_EXT,EXT)) PHONE_EXT
          ,DENSE_RANK() OVER (ORDER BY NA_TH) AS ROW_ID
          FROM PEOPLEFINDER M, EMP_TMP D
          WHERE BU <> 'VN' 
          AND M.EMP = D.EM_CODE_NEW(+)  """

  df = pd.read_sql_query(sql, conn2)

  writer = pd.ExcelWriter("people.xlsx", engine='xlsxwriter')
  df.to_excel(writer, sheet_name='people', index=False)
  writer.save()

        
def people():
  my_dsn = cx_Oracle.makedsn("172.16.9.127",port=1521,sid="HRIS")
  conn = cx_Oracle.connect(user="NYTG", password="Hris2@17", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  my_dsn2 = cx_Oracle.makedsn("172.16.6.80",port=1521,sid="NYTG")
  conn2 = cx_Oracle.connect(user="HRMS", password="HRMS", dsn=my_dsn2, encoding = "UTF-8", nencoding = "UTF-8")

  cursor = conn.cursor()
  cursor2 = conn2.cursor()
  cursor.execute("""SELECT M.* FROM (
    SELECT DISTINCT DENSE_RANK() OVER (ORDER BY EM.NAME, EM.CODE, B.NAMEALT, PEM.EMAILADDRESS) AS ROW_ID,
        EM.CODE AS EMP,
        EM.CARDNO AS CANO,
        TRIM(EM.NAME) AS NA_TH,
        TRIM(EM.NAMEALT) AS NA_EN,
        NVL(P.NICKNAME,'') AS NIC,
        REPLACE( PEM.EMAILADDRESS,'''','') AS MAIL,
        '' AS EXT,
        C.CODE AS BU,
        B.NAMEALT DEPT,
        POS.NAMEALT POS_TH,
        EML.NAME POS_EN,
        'eunite' AS F_DB
    FROM NYTG.AM_EMPLOYEE EM 
        INNER JOIN NYTG.AM_TRAF TRAF ON TRAF.EMID = EM.ID
            AND TRAF.DOCSTAT = 2
            --AND((TRAF.DATETO IS NOT NULL AND TRUNC(SYSDATE) BETWEEN TRAF.DATEFO AND TRAF.DATETO)
            --OR (TRAF.DATETO IS NULL AND TRUNC(SYSDATE) >= TRAF.DATEFO))
            -- AND SYSDATE BETWEEN TRAF.DATEFO AND NVL(TRAF.DATETO,SYSDATE)
            AND TRIM(SYSDATE) BETWEEN TRIM(TRAF.DATEFO) AND TRIM(NVL(TRAF.DATETO,SYSDATE))
        LEFT JOIN NYTG.AM_STUUNITD B ON TRAF.STUID = B.STUID
            --AND ((B.DATETO IS NOT NULL AND TRUNC(SYSDATE) BETWEEN B.DATEFO AND TRAF.DATETO)
            --OR (B.DATETO IS NULL AND TRUNC(SYSDATE) >= B.DATEFO))
            -- AND SYSDATE BETWEEN B.DATEFO AND NVL(B.DATETO,SYSDATE)
            AND TRIM(SYSDATE) BETWEEN TRIM(TRAF.DATEFO) AND TRIM(NVL(TRAF.DATETO,SYSDATE))
        INNER JOIN NYTG.AM_LOCATION LOC ON TRAF.LOCAID = LOC.ID
        INNER JOIN NYTG.AM_COMPANY C ON EM.COMPANYID = C.ID
        INNER JOIN NYTG.AM_POSITION POS ON TRAF.POSID = POS.ID
        INNER JOIN NYTG.AM_EMLEVEL EML ON TRAF.EMLID = EML.ID 
        INNER JOIN NYTG.AM_JOBROLE JR ON TRAF.JOBROLEID = JR.ID 
        INNER JOIN NYTG.AM_PERSON P ON EM.PERSID = P.ID 
        LEFT JOIN (SELECT DISTINCT PERSID, EMAILADDRESS FROM NYTG.AM_PERSEMAIL WHERE EMAILTYPE = 100001) PEM ON EM.PERSID = PEM.PERSID
        LEFT JOIN NYTG.AM_EMPLOYEETYPE ET ON TRAF.EMPLOYEETYPE = ET.ID 
        LEFT JOIN NYTG.AM_EMPLOYMENTTYPE EMT ON ET.EMPLMTYPE = EMT.ID 
        LEFT JOIN NYTG.AM_PAYTYPE PT ON ET.PAYTYPE = PT.ID 
        LEFT JOIN NYTG.AM_StuUnitD div on div.stuid = B.u4 
    WHERE ET.CODE NOT IN ('PM-D','CT-D')
        AND (EM.ENDJOBD IS NULL OR EM.ENDJOBD >= sysdate)
        AND PT.NAME = 'รายเดือน'
    ) M ORDER BY ROW_ID """)

  cursor2.execute("""delete from PEOPLEFINDER""")
  conn2.commit()

  for row in cursor:
    # print(row[0])
    nic = "NULL" if row[5] is None else "'{}'".format(row[5])  
    mail = "NULL" if row[6] is None else "'{}'".format(row[6])               

    sql = """Insert into PEOPLEFINDER (ROW_ID,EMP,CANO,NA_TH,NA_EN,NIC,MAIL,EXT,BU,DEPT,POS_TH,POS_EN,F_DB) values ('{}','{}','{}','{}','{}',{},{},{},'{}','{}','{}','{}','{}')""".format(row[0], row[1], row[2], row[3], row[4], nic, mail, 'NULL', row[8], row[9], row[10], row[11], row[12])
    # print(sql)
    cursor2.execute(sql)

  conn2.commit()
  conn.close()
  conn2.close()
  print('Complete People')


###########################################

###########################################

def peopleVN():
  mydb = mysql.connector.connect(
  host="172.16.9.20",
  user="kkk",
  password="Spe@ker9",
  database="nytg_center"
)

  mycursor = mydb.cursor()

  mycursor.execute(""" SELECT 
                  EM_CODE_NEW*100000 AS ROW_ID,
                  EM_CODE_NEW AS EMP,
                  EM_CODE_OLD AS CANO,
                  CONCAT(FIRSTNAME_ENG, ' ', LASTNAME_ENG) AS NA_TH,
                  CONCAT(FIRSTNAME_ENG, ' ', LASTNAME_ENG) AS NA_EN,
                  NICKNAME_TH AS NIC,
                  EMAIL_ADDRESS AS MAIL,
                  BU_HRIS AS BU,
                  DEPARTMENT_HRIS AS DEPT,
                  POSITION_HRIS AS POS_TH,
                  LEVEL_HRIS AS POS_EN,
                  'vn' AS F_DB,
                  PHONE_EXT AS EXT
              FROM km_employee_new 
              WHERE STATUS_ACTIVE = 'Y' """)

  my_dsn2 = cx_Oracle.makedsn("172.16.6.80",port=1521,sid="NYTG")
  conn2 = cx_Oracle.connect(user="HRMS", password="HRMS", dsn=my_dsn2, encoding = "UTF-8", nencoding = "UTF-8")
  cursor2 = conn2.cursor()

  for row in mycursor:
    print(row)
    th = "NULL" if row[3] is None else "'{}'".format(row[3])  
    en = "NULL" if row[4] is None else "'{}'".format(row[4])  
    nic = "NULL" if row[5] is None else "'{}'".format(row[5])  
    mail = "NULL" if row[6] is None else "'{}'".format(row[6])   
    bu = "NULL" if row[7] is None else "'{}'".format(row[7]) 
    dept = "NULL" if row[8] is None else "'{}'".format(row[8])    
    ext = "NULL" if row[12] is None else "'{}'".format(row[12])              

    # sql = """
    #     INSERT INTO PEOPLEFINDER (NA_TH, NA_EN, NIC, MAIL, BU, DEPT, EXT) VALUES ({},{},{},{},{},{},{})
    # """.format( th, en, nic, mail, bu, dept, ext)
    # print(sql)
    # cursor2.execute(sql)

  conn2.commit()
  mydb.close()
  print('Complete VN')




people()
# exportpeople()
# peopleVN()

print ("COMPLETE All")
