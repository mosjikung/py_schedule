import cx_Oracle
import csv
import os
from pathlib import Path
import requests
import threading
import time
import datetime
from datetime import datetime, timedelta
# import smtplib
# from email.message import EmailMessage
# from email.mime.multipart import MIMEMultipart
# from email.mime.text import MIMEText

# used virtualenv = interface
# virtualenv/pip freeze > requirements.txt
# virtualenv/pip install -r requirements.txt


oracle_client = "C:\instantclient_19_5"
os.environ["ORACLE_HOME"]=oracle_client
os.environ["PATH"]=oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"]="AMERICAN_AMERICA.TH8TISASCII"


time_start  = datetime.now() 
allTxt = ''
py_file = 'controlRoomOutput.py '


def printttime(txt):
  global allTxt
  
  dateTimeObj = datetime.now()
  timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
  now  = datetime.now() 
  duration = now - time_start
  if allTxt == '':
    sendLine(py_file + 'Start')
    allTxt = '\n'
  print(py_file + timestampStr + ' ' + str(duration.total_seconds()) + ' ' + txt)
  txtSend = py_file + timestampStr + ' ' + str(duration.total_seconds()) + ' ' + txt +'\n'
  allTxt = allTxt + txtSend


def sendLine(txt):
  url = 'https://notify-api.line.me/api/notify'
  token = 'ZE6d4wFQO2qQiSMAMqecrPbj6R3nhj0y1STqOJ6xQ1s'
  headers = {'content-type':'application/x-www-form-urlencoded','Authorization':'Bearer ' + token}
  msg = txt
  requests.post(url,headers=headers,data = {'message':msg})
 
  
  
class CLS_OUTPUT_DRY_03(threading.Thread):
      def __init__(self, sDte, sOU):
        threading.Thread.__init__(self)
        self.sDte = sDte
        self.sOU = sOU
      def run(self):
        OUTPUT_DRY_03(self.sDte, self.sOU)

def OUTPUT_DRY_03(sDte, sOU):
  my_dsn = cx_Oracle.makedsn("172.16.6.76",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="DEMO", password="DEMO", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()

  sql = """INSERT INTO OUTPUT_CONTROL_ROOM
            SELECT 'DRY' AS PROCESS
            ,TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI') - INTERVAL '4' HOUR AS START_DATE
            ,TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI') + INTERVAL '0' HOUR AS END_DATE
            ,SUM(M.QTY) TOTAL_QTY
            ,'{sOU}' AS OU_CODE
            ,TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS') INSERT_DATE
            ,TO_CHAR(TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI') - INTERVAL '4' HOUR,'YYYY-MM-DD HH24:MI:SS') START_DATE_TXT
            ,TO_CHAR(TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI') + INTERVAL '0' HOUR,'YYYY-MM-DD HH24:MI:SS') END_DATE_TXT
            FROM (
            SELECT M.OU_CODE, M.BATCH_NO, MAX(M.END_DATE) END_DATE, MAX(M.TOTAL_QTY) QTY
            FROM DFBT_MONITOR M 
            WHERE 1 = 1
            AND METHOD_CONT = 4
            AND EXISTS (SELECT * FROM DFMS_STEP ST WHERE ST.GROUP_STEP = '04-Dryer' AND ST.STEP_NO = M.STEP_NO)
            AND EXISTS (select mc.* FROM DFMS_MACHINE mc where mc.CAP_TIME_PROD > 0 and mc.machine_no = M.machine_no and mc.OU_CODE='{sOU}' )
            AND EXISTS (SELECT * FROM DFIT_BTDATA BD WHERE  TUBULAR_TYPE =2 and M.OU_CODE = BD.OU_CODE and M.BATCH_NO = BD.BATCH_NO)
            GROUP BY M.OU_CODE, M.BATCH_NO
            HAVING COUNT(M.BATCH_NO) = COUNT(END_DATE)
            ) M
            WHERE END_DATE >= (TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI') - INTERVAL '4' HOUR)
            AND END_DATE < TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI')""".format(sDte = sDte, sOU = sOU)

  cursor.execute(sql)

  conn.commit()
  conn.close()
  printttime('OUTPUT_DRY ' + sOU + ' ' + sDte)
  
  
class CLS_OUTPUT_FINISHING_03(threading.Thread):
      def __init__(self, sDte, sOU):
        threading.Thread.__init__(self)
        self.sDte = sDte
        self.sOU = sOU
      def run(self):
        OUTPUT_FINISHING_03(self.sDte, self.sOU)

def OUTPUT_FINISHING_03(sDte, sOU):
  my_dsn = cx_Oracle.makedsn("172.16.6.76",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="DEMO", password="DEMO", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()

  sql = """ INSERT INTO OUTPUT_CONTROL_ROOM
SELECT 'FINISHING' AS PROCESS
,TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI') - INTERVAL '4' HOUR AS START_DATE
,TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI') + INTERVAL '0' HOUR AS END_DATE
,SUM(M.QTY) TOTAL_QTY
,'{sOU}' AS OU_CODE
,TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS') INSERT_DATE
,TO_CHAR(TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI') - INTERVAL '4' HOUR,'YYYY-MM-DD HH24:MI:SS') START_DATE_TXT
,TO_CHAR(TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI') + INTERVAL '0' HOUR,'YYYY-MM-DD HH24:MI:SS') END_DATE_TXT
FROM (
 SELECT M.OU_CODE, M.BATCH_NO, MAX(M.END_DATE) END_DATE, MAX(M.TOTAL_QTY) QTY
FROM DFBT_MONITOR M
WHERE 1 = 1
AND METHOD_CONT = 4
AND EXISTS (SELECT * FROM DFMS_STEP ST WHERE ST.GROUP_STEP = '05-Finishing' AND ST.STEP_NO = M.STEP_NO)
AND EXISTS (select mc.* FROM DFMS_MACHINE mc where mc.machine_group in ('FM02','FM03','FM11') and mc.CAP_TIME_PROD > 0 and mc.machine_no = M.machine_no and mc.OU_CODE='{sOU}' )
AND EXISTS (SELECT * FROM DFIT_BTDATA BD WHERE  TUBULAR_TYPE =2 and M.OU_CODE = BD.OU_CODE and M.BATCH_NO = BD.BATCH_NO)
GROUP BY M.OU_CODE, M.BATCH_NO
HAVING COUNT(M.BATCH_NO) = COUNT(END_DATE)
) M
WHERE END_DATE >= (TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI') - INTERVAL '4' HOUR)
AND END_DATE < TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI') """.format(sDte = sDte, sOU = sOU)

  cursor.execute(sql)
  
  conn.commit()
  conn.close()
  printttime('OUTPUT_FINISHING ' + sOU + ' ' + sDte)
  

class CLS_OUTPUT_INSPECTION(threading.Thread):
      def __init__(self, sDte, sOU):
        threading.Thread.__init__(self)
        self.sDte = sDte
        self.sOU = sOU
      def run(self):
        OUTPUT_INSPECTION(self.sDte, self.sOU)

def OUTPUT_INSPECTION(sDte, sOU):
  my_dsn = cx_Oracle.makedsn("172.16.6.76",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="DEMO", password="DEMO", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()

  sql = """ INSERT INTO OUTPUT_CONTROL_ROOM
SELECT 'INSPECTION' AS PROCESS
,TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI') - INTERVAL '4' HOUR AS START_DATE
,TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI') + INTERVAL '0' HOUR AS END_DATE
,SUM(M.QTY) TOTAL_QTY
,'{sOU}' AS OU_CODE
,TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS') INSERT_DATE
,TO_CHAR(TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI') - INTERVAL '4' HOUR,'YYYY-MM-DD HH24:MI:SS') START_DATE_TXT
,TO_CHAR(TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI') + INTERVAL '0' HOUR,'YYYY-MM-DD HH24:MI:SS') END_DATE_TXT
FROM (
 SELECT M.OU_CODE, M.BATCH_NO, MAX(M.END_DATE) END_DATE, MAX(M.TOTAL_QTY) QTY
FROM DFBT_MONITOR M
WHERE 1 = 1
AND METHOD_CONT in ('7')
AND EXISTS (SELECT * FROM DFMS_STEP ST WHERE ST.GROUP_STEP = '09-Inspection' AND ST.STEP_NO = M.STEP_NO)
AND EXISTS (select mc.* FROM DFMS_MACHINE mc where mc.CAP_TIME_PROD > 0 and mc.machine_no = M.machine_no and mc.OU_CODE='{sOU}' )
GROUP BY M.OU_CODE, M.BATCH_NO
HAVING COUNT(M.BATCH_NO) = COUNT(END_DATE)
) M
WHERE END_DATE >= (TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI') - INTERVAL '4' HOUR)
AND END_DATE < TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI') """.format(sDte = sDte, sOU = sOU)

  cursor.execute(sql)

  conn.commit()
  conn.close()
  printttime('OUTPUT_INSPECTION ' + sOU + ' ' + sDte)


  
class CLS_OUTPUT_DYE(threading.Thread):
      def __init__(self, sDte, sOU):
        threading.Thread.__init__(self)
        self.sDte = sDte
        self.sOU = sOU
      def run(self):
        OUTPUT_DYE(self.sDte, self.sOU)

def OUTPUT_DYE(sDte, sOU):
  my_dsn = cx_Oracle.makedsn("172.16.6.76",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="DEMO", password="DEMO", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()

  sql = """ INSERT INTO OUTPUT_CONTROL_ROOM
SELECT 'DYEING' AS PROCESS
,TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI') - INTERVAL '4' HOUR AS START_DATE
,TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI') + INTERVAL '0' HOUR AS END_DATE
,SUM(M.QTY) TOTAL_QTY
,'{sOU}' AS OU_CODE
,TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS') INSERT_DATE
,TO_CHAR(TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI') - INTERVAL '4' HOUR,'YYYY-MM-DD HH24:MI:SS') START_DATE_TXT
,TO_CHAR(TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI') + INTERVAL '0' HOUR,'YYYY-MM-DD HH24:MI:SS') END_DATE_TXT
FROM (
 SELECT M.OU_CODE, M.BATCH_NO, MAX(M.END_DATE) END_DATE, MAX(M.TOTAL_QTY) QTY
FROM DFBT_MONITOR M
WHERE 1 = 1
AND METHOD_CONT ='2'
AND EXISTS (select mc.* FROM DFMS_MACHINE mc where mc.machine_no = M.machine_no and mc.OU_CODE='{sOU}' )
GROUP BY M.OU_CODE, M.BATCH_NO
HAVING COUNT(M.BATCH_NO) = COUNT(END_DATE)
) M
WHERE END_DATE >= (TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI') - INTERVAL '4' HOUR)
AND END_DATE < TO_DATE( '{sDte}' ,'YYYY/MM/DD HH24:MI') """.format(sDte = sDte, sOU = sOU)

  cursor.execute(sql)

  conn.commit()
  conn.close()
  printttime('OUTPUT_DYE ' + sOU + ' ' + sDte)

  

printttime('Start 4 Process * 3 OU files')

_date = datetime.now()
_dts = str(_date)[0:13].replace("-", "/")+':00'
print(_dts)

# _dts = '2020/06/07 08:00'


threads = []
thread01 = CLS_OUTPUT_DRY_03(_dts, 'D03'); thread01.start(); threads.append(thread01)
thread02 = CLS_OUTPUT_DRY(_dts, 'D02'); thread02.start(); threads.append(thread02)
thread03 = CLS_OUTPUT_DRY(_dts, 'D06'); thread03.start(); threads.append(thread03)

thread04 = CLS_OUTPUT_FINISHING_03(_dts, 'D03'); thread04.start(); threads.append(thread04)
thread05 = CLS_OUTPUT_FINISHING(_dts, 'D02'); thread05.start(); threads.append(thread05)
thread06 = CLS_OUTPUT_FINISHING(_dts, 'D06'); thread06.start(); threads.append(thread06)

thread07 = CLS_OUTPUT_INSPECTION(_dts, 'D03'); thread07.start(); threads.append(thread07)
thread08 = CLS_OUTPUT_INSPECTION(_dts, 'D02'); thread08.start(); threads.append(thread08)
thread09 = CLS_OUTPUT_INSPECTION(_dts, 'D06'); thread09.start(); threads.append(thread09)

thread10 = CLS_OUTPUT_DYE(_dts, 'D03'); thread10.start(); threads.append(thread10)
thread11 = CLS_OUTPUT_DYE(_dts, 'D02'); thread11.start(); threads.append(thread11)
thread12 = CLS_OUTPUT_DYE(_dts, 'D06'); thread12.start(); threads.append(thread12)



for t in threads:
    t.join()
print (allTxt)
sendLine(allTxt)