import cx_Oracle
import csv
import os
from pathlib import Path
import requests
import threading
import time
import datetime
from datetime import datetime, timedelta

oracle_client = "C:\instantclient_19_5"
os.environ["ORACLE_HOME"]=oracle_client
os.environ["PATH"]=oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"]="AMERICAN_AMERICA.TH8TISASCII"

my_dsn = cx_Oracle.makedsn("172.16.6.76",port=1521,sid="NYTG")
conn = cx_Oracle.connect(user="DEMO", password="DEMO", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
cursor = conn.cursor()
args = ['NYK','NYKSPO001','HIS_PREPARE.csv']
result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
print("HIS_PREPARE.csv Start")
sql = """SELECT  TO_CHAR(CREATE_DATE,'YYYY-MM-DD HH24:MI') CREATE_DATE_TXT
        ,RANK() OVER(PARTITION BY TO_CHAR(CREATE_DATE,'YYYY-MM-DD HH24') ORDER BY TO_CHAR(CREATE_DATE,'YYYY-MM-DD HH24') DESC, END_DYE_DATE, BATCH_NO) NEW_RNK, M.*
				, SUBSTR(END_DYE_DATE,7,4) || '-' || SUBSTR(END_DYE_DATE,4,2)|| '-' || SUBSTR(END_DYE_DATE,1,2) || ' ' || SUBSTR(END_DYE_DATE,12,5) PLAN_START_DYE
				FROM CONTROL_HIS_PREPARE M
				WHERE 1 = 1
				AND (TO_CHAR(M.CREATE_DATE,'HH24') = '07' 
        OR TO_CHAR(M.CREATE_DATE,'HH24') = '19' ) """

cursor.execute(sql)

# _csv = r"HIS_PREPARE.csv"
_csv = r"C:\QVD_DATA\PRO_NYK\HIS_PREPARE.csv"
with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
  csv_writer = csv.writer(csv_file)
  csv_writer.writerow([i[0] for i in cursor.description]) # write headers
  csv_writer.writerows(cursor)
args = ['NYK','NYKSPO001',cursor.rowcount]
result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
print("HIS_PREPARE.csv END")  

print("HIS_DRY.csv Start")
args = ['NYK','NYKSPO002','HIS_DRY.csv']
result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
sql = """SELECT  TO_CHAR(CREATE_DATE,'YYYY-MM-DD HH24:MI') CREATE_DATE_TXT
,RANK() OVER(PARTITION BY TO_CHAR(CREATE_DATE,'YYYY-MM-DD HH24') ORDER BY TO_CHAR(CREATE_DATE,'YYYY-MM-DD HH24') DESC,TIME_AGING DESC, BATCH_NO) NEW_RNK, M.*
				, SUBSTR(END_DYE_DATE,7,4) || '-' || SUBSTR(END_DYE_DATE,4,2)|| '-' || SUBSTR(END_DYE_DATE,1,2) || ' ' || SUBSTR(END_DYE_DATE,12,5) END_DYE_DATE2
				FROM CONTROL_HIS_DRY M
				WHERE 1 = 1
				AND (TO_CHAR(M.CREATE_DATE,'HH24') = '07' OR TO_CHAR(M.CREATE_DATE,'HH24') = '19' )"""

cursor.execute(sql)

# _csv = r"HIS_DRY.csv"
_csv = r"C:\QVD_DATA\PRO_NYK\HIS_DRY.csv"

with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
  csv_writer = csv.writer(csv_file)
  csv_writer.writerow([i[0] for i in cursor.description]) # write headers
  csv_writer.writerows(cursor)
args = ['NYK','NYKSPO002',cursor.rowcount]
result_args = cursor.callproc('QVD_RUN_upd_LOG', args) 
print("HIS_DRY.csv END")


print("HIS_FINISHING.csv START")
args = ['NYK','NYKSPO003','HIS_FINISHING.csv']
result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
sql = """ SELECT TO_CHAR(CREATE_DATE,'YYYY-MM-DD HH24:MI') CREATE_DATE_TXT
        ,  RANK() OVER(PARTITION BY TO_CHAR(CREATE_DATE,'YYYY-MM-DD HH24') ORDER BY TO_CHAR(CREATE_DATE,'YYYY-MM-DD HH24') DESC, TIME_AGING DESC, BATCH_NO) NEW_RNK, M.*
				, SUBSTR(END_DYE_DATE,7,4) || '-' || SUBSTR(END_DYE_DATE,4,2)|| '-' || SUBSTR(END_DYE_DATE,1,2) || ' ' || SUBSTR(END_DYE_DATE,12,5) END_DYE_DATE2
				FROM CONTROL_HIS_FINISHING M
				WHERE 1 = 1
				AND (TO_CHAR(M.CREATE_DATE,'HH24') = '07' OR TO_CHAR(M.CREATE_DATE,'HH24') = '19' )
        """

cursor.execute(sql)

# _csv = r"HIS_FINISHING.csv"
_csv = r"C:\QVD_DATA\PRO_NYK\HIS_FINISHING.csv"

with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
  csv_writer = csv.writer(csv_file)
  csv_writer.writerow([i[0] for i in cursor.description]) # write headers
  csv_writer.writerows(cursor)
args = ['NYK','NYKSPO003',cursor.rowcount]
result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
print("HIS_FINISHING.csv END")


print("HIS_INSPECTION.csv START")
args = ['NYK','NYKSPO004','HIS_INSPECTION.csv']
result_args = cursor.callproc('QVD_RUN_INS_LOG', args) 
sql = """ SELECT TO_CHAR(CREATE_DATE,'YYYY-MM-DD HH24:MI') CREATE_DATE_TXT
        , RANK() OVER(PARTITION BY TO_CHAR(CREATE_DATE,'YYYY-MM-DD HH24') ORDER BY TO_CHAR(CREATE_DATE,'YYYY-MM-DD HH24') DESC,TIME_AGING DESC, BATCH_NO) NEW_RNK, 
        KEY,
        OU_CODE,
        BATCH_NO,
        ITEM_CODE,
        COLOR_CODE,
        COLOR_DESC,
        TOTAL_ROLL,
        TOTAL_QTY,
        END_LAST_STEP,
        TIME_AGING,
        TIMESPAN,
        FTIME_AGING,
        FTIME,
        SO_NO,
        CUSTOMER_ID,
        CUSTOMER_NAME,
        VI,
        ICON_COLOR,
        ICON_NAME,
        WARNINGTIME,
        ALERTTIME,
        METHOD_NAME,
        STEP_NO,
        STEP_NAME,
        START_DATE,
        MACHINE_NO,
        DECODE(COLOR_APPROVE,'done','P','clear','R',COLOR_APPROVE) COLOR_APPROVE,
        COLOR_APPROVE_DATE,
        DECODE(QT_APPROVE,'done','P','clear','R',QT_APPROVE) QT_APPROVE,
        QT_APPROVE_DATE,
        CREATE_DATE,
        OU_GROUP,
        BOGIE_DATE,
        BOGIE_NO,
        QA_APPROVE_DATE,
        QA_APPROVE,
        SUBSTR(END_LAST_STEP,7,4) || '-' || SUBSTR(END_LAST_STEP,4,2)|| '-' || SUBSTR(END_LAST_STEP,1,2) || ' ' || SUBSTR(END_LAST_STEP,12,5) END_LAST_STEP2
				FROM CONTROL_HIS_INSPECTION M
				WHERE 1 = 1
				AND (TO_CHAR(M.CREATE_DATE,'HH24') = '07' OR TO_CHAR(M.CREATE_DATE,'HH24') = '19' )

				 """

cursor.execute(sql)

# _csv = r"HIS_INSPECTION.csv"
_csv = r"C:\QVD_DATA\PRO_NYK\HIS_INSPECTION.csv"

with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
  csv_writer = csv.writer(csv_file)
  csv_writer.writerow([i[0] for i in cursor.description]) # write headers
  csv_writer.writerows(cursor)
args = ['NYK','NYKSPO004',cursor.rowcount]
result_args = cursor.callproc('QVD_RUN_upd_LOG', args) 
print("HIS_INSPECTION.csv END")  

conn.close()
