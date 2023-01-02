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


def newemp():
  dsnHR= cx_Oracle.makedsn("172.16.9.127", port=1521, sid="HRIS")
  connHR = cx_Oracle.connect(
      user="NYTG", password="Hris2@17", dsn=dsnHR, encoding="UTF-8", nencoding="UTF-8")

  dsnCtrl = cx_Oracle.makedsn("172.16.6.80", port=1521, sid="NYTG")
  connCtrl = cx_Oracle.connect(
      user="WEBCONTROL", password="WEBCONTROL", dsn=dsnCtrl, encoding="UTF-8", nencoding="UTF-8")
  
  sql = """ SELECT DISTINCT
                    EM.CODE AS EMCODE,
                    LPAD(EM.CARDNO, 10, '0') as CARDNO,
                    EM.NAME AS EMNAME,
                    EM.NAMEALT AS EMNAMEALT,
                    NVL(P.FIRSTNAMEALT, P.FIRSTNAME) AS FIRSTNAMEALT,
                    NVL(P.LASTNAMEALT, P.LASTNAME) AS LASTNAMEALT,
                    P.NICKNAME NICKNAME,
                    PEM.EMAILADDRESS EMAILADDRESS,
                    '' AS PHONE_EXT,
                    C.CODE COMPANYCODE,
                    B.NAMEALT DEPT,
                    POS.NAMEALT POSNAMEALT,
                    EML.NAME EMLEVELNAME,
                    'eunite' AS FROM_DB
                FROM NYTG.AM_EMPLOYEE EM 
                    INNER JOIN NYTG.AM_TRAF TRAF ON TRAF.EMID = EM.ID
                        AND TRAF.DOCSTAT = 2
                        AND((TRAF.DATETO IS NOT NULL AND TRUNC(SYSDATE) BETWEEN TRAF.DATEFO AND TRAF.DATETO)
                        OR (TRAF.DATETO IS NULL AND TRUNC(SYSDATE) >= TRAF.DATEFO))
                    LEFT JOIN NYTG.AM_STUUNITD B ON TRAF.STUID = B.STUID
                        AND ((B.DATETO IS NOT NULL AND TRUNC(SYSDATE) BETWEEN B.DATEFO AND TRAF.DATETO)
                        OR (B.DATETO IS NULL AND TRUNC(SYSDATE) >= B.DATEFO))
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
                     """

  df = pd.read_sql_query(sql, connHR)
  # print(df)

  cursor = connCtrl.cursor()
  insert = 0

  for index, row in df.iterrows():
    # print(row['EMCODE'])
    sql = """ SELECT M.* FROM CTL_USER M WHERE APPROVE5 = '{empid}'""".format(
        empid=row['EMCODE'])

    dfChk = pd.read_sql_query(sql, connCtrl)

    cnt = dfChk.shape[0]
    

    if (cnt==0):
      insert = insert + 1
      # print(row['EMCODE'])
      try:
    
        sql = """INSERT INTO CTL_USER (USER_ID, USER_PASSWORD, USER_FNAME, USER_LNAME, USER_ACTIVE, USER_EMP_ID, USER_MAIL, APPROVE4, APPROVE5, CREATED_DATE, CREATED_BY)
                  VALUES ('{uid}','123456','{fname}','{lname}','Y','{uid}','{mail}','{uid}','{uid}',SYSDATE,'python')
              """.format(uid=row['EMCODE'], fname=row['FIRSTNAMEALT'], lname=row['LASTNAMEALT'], mail=row['EMAILADDRESS'])
        # print(sql)
        cursor.execute(sql)
        connCtrl.commit()

        sql = """ INSERT INTO CTL_USER_SYSTEM (USER_ID, SYSCODE, ROLE_ID) VALUES ('{uid}','BS1','USER')""".format(
            uid=row['EMCODE'])

        cursor.execute(sql)
        connCtrl.commit()

      except:
        a = 'b'

    else:
      sql = """ UPDATE CTL_USER SET USER_MAIL = '{mail}' WHERE APPROVE5 = '{uid}' AND USER_MAIL IS NULL """.format(
            uid=row['EMCODE'], mail=row['EMAILADDRESS'])

      cursor.execute(sql)
      connCtrl.commit()


  print(insert)
  connHR.close()
  connCtrl.close()


def leaveemp():
  dsnHR = cx_Oracle.makedsn("172.16.9.127", port=1521, sid="HRIS")
  connHR = cx_Oracle.connect(
      user="NYTG", password="Hris2@17", dsn=dsnHR, encoding="UTF-8", nencoding="UTF-8")

  dsnCtrl = cx_Oracle.makedsn("172.16.6.80", port=1521, sid="NYTG")
  connCtrl = cx_Oracle.connect(
      user="WEBCONTROL", password="WEBCONTROL", dsn=dsnCtrl, encoding="UTF-8", nencoding="UTF-8")

  sql = """ SELECT DISTINCT
                    EM.CODE AS EMCODE
                FROM NYTG.AM_EMPLOYEE EM
                    INNER JOIN NYTG.AM_TRAF TRAF ON TRAF.EMID = EM.ID
                        AND TRAF.DOCSTAT = 2
                        AND((TRAF.DATETO IS NOT NULL AND TRUNC(SYSDATE) BETWEEN TRAF.DATEFO AND TRAF.DATETO)
                        OR(TRAF.DATETO IS NULL AND TRUNC(SYSDATE) >= TRAF.DATEFO))
                    LEFT JOIN NYTG.AM_EMPLOYEETYPE ET ON TRAF.EMPLOYEETYPE = ET.ID
                    LEFT JOIN NYTG.AM_PAYTYPE PT ON ET.PAYTYPE = PT.ID
                WHERE ET.CODE NOT IN('PM-D', 'CT-D')
                    AND PT.NAME = 'รายเดือน'
                    AND EM.endJobD IS NOT NULL
                    AND EM.QUITREASON IS NOT NULL
                    AND TRUNC(EM.endJobD) >= TRUNC(sysdate - 365)
                    AND TRUNC(EM.endJobD) < TRUNC(sysdate)
                     """

  df = pd.read_sql_query(sql, connHR)

  cursor = connCtrl.cursor()

  for index, row in df.iterrows():
    sql = """ UPDATE CTL_USER SET USER_ACTIVE = 'N' 
              WHERE (USER_EMP_ID = '{empid}' 
              OR APPROVE5 = '{empid}') """.format(
        empid=row['EMCODE'])
    cursor.execute(sql)
    connCtrl.commit()

newemp()
# leaveemp()
