# callNygOTP.bat
import cx_Oracle
import csv
import os
from pathlib import Path
import requests
from datetime import datetime
import threading
import time
# import smtplib
# from email.message import EmailMessage
# from email.mime.multipart import MIMEMultipart
# from email.mime.text import MIMEText


oracle_client = "C:\instantclient_19_5"
os.environ["ORACLE_HOME"]=oracle_client
os.environ["PATH"]=oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"]="AMERICAN_AMERICA.TH8TISASCII"


time_start  = datetime.now() 
allTxt = ''
allHtml = ''
py_file = 'RoomNygLT.py '


def printttime(txt):
  global allTxt
  global allHtml
  
  dateTimeObj = datetime.now()
  timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
  now  = datetime.now() 
  duration = now - time_start
  if allTxt == '':
    # sendLine(py_file + 'Start', '')
    allTxt = '\n'
    allHtml = '<br>'
  print(py_file + timestampStr + ' ' + str(duration.total_seconds()) + ' ' + txt)
  txtSend = py_file + timestampStr + ' ' + str(duration.total_seconds()) + ' ' + txt
  allTxt = allTxt + txtSend + '\n'
  allHtml = allHtml + txtSend + '<br>'


def sendLine(txt, txtmail):
  # url = 'https://notify-api.line.me/api/notify'
  # token = 'ZE6d4wFQO2qQiSMAMqecrPbj6R3nhj0y1STqOJ6xQ1s'
  # headers = {'content-type':'application/x-www-form-urlencoded','Authorization':'Bearer ' + token}
  # msg = txt
  # requests.post(url,headers=headers,data = {'message':msg})
  url = 'http://peoplefinder.nanyangtextile.com/service/mail/sendmailQN2.ashx'
  params = {'subject':'Interface NYG Control Room LT','body':txt, 'to':'157bb490.nanyangtextile.com@apac.teams.ms'}
  requests.post(url,params=params)
  # msg = MIMEMultipart()
  # msg['Subject'] = 'Interface NYG Control Room Readiness'
  # msg['From'] = 'interface_data@nanyangtextile.com'
  # msg['To'] = '157bb490.nanyangtextile.com@apac.teams.ms'
  # body = """Source = D:-GITProject-Interface'+'\n"""
  # body = ''
  # body = body + txt
  # msg.attach(MIMEText('txt', 'plain'))
  # with smtplib.SMTP('smtp.nanyangtextile.com', 25) as s:
    # s.send_message(msg) 
 
  
  
  
class CLS_EFF_LT_SAMPLE(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        EFF_LT_SAMPLE()
        
def EFF_LT_SAMPLE():
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""DELETE FROM EFF_LT_SAMPLE""")

  conn.commit()

  cursor.execute("""INSERT INTO EFF_LT_SAMPLE
SELECT EDD_W,  SAMPLE_TARGET_WK AS TARGET_WK
,DECODE(BU,'NYG','TH','NYV','VN',BU) || CASE WHEN BU = 'TRM' THEN '' ELSE ('-' || CUST_GROUP) END AS TEAM
,NVL(SUB_TEAM,'NA') SUB_TEAM
,DECODE(BU,'NYG','TH','NYV','VN',BU) || CASE WHEN BU = 'TRM' THEN '' ELSE ('-' || CUST_GROUP) END || '-' || NVL(SUB_TEAM,'NA') TEAMKEY
,COUNT(SO_NO_DOC) TOTAL_SO
,SUM(ORDER_QTY) TOTAL_PCS
,SUM(CASE WHEN SAMPLE_RELEASE_WK <= SAMPLE_TARGET_WK THEN 1 ELSE 0 END) SO_SUCCESS
,SUM(CASE WHEN SAMPLE_RELEASE_WK <= SAMPLE_TARGET_WK THEN ORDER_QTY ELSE 0 END) PCS_SUCCESS
FROM CONTROL_WIP_READINESS M
WHERE  SAMPLE_TARGET_WK >= '202010'
AND SAMPLE_TARGET_WK <= TO_CHAR(TRUNC(SYSDATE)+42,'IYYYIW')
AND SAMPLE_RELEASE_WK IS NOT NULL
AND ORDER_QTY > 0
GROUP BY EDD_W, SAMPLE_TARGET_WK
,DECODE(BU,'NYG','TH','NYV','VN',BU) || CASE WHEN BU = 'TRM' THEN '' ELSE ('-' || CUST_GROUP) END
,NVL(SUB_TEAM,'NA') 
UNION ALL
SELECT EDD_W,  SAMPLE_TARGET_WK AS TARGET_WK
,DECODE(BU,'NYG','TH','NYV','VN',BU) || CASE WHEN BU = 'TRM' THEN '' ELSE ('-' || CUST_GROUP) END AS TEAM
,NVL(SUB_TEAM,'NA') SUB_TEAM
,DECODE(BU,'NYG','TH','NYV','VN',BU) || CASE WHEN BU = 'TRM' THEN '' ELSE ('-' || CUST_GROUP) END || '-' || NVL(SUB_TEAM,'NA') TEAMKEY
,COUNT(SO_NO_DOC) TOTAL_SO
,SUM(ORDER_QTY) TOTAL_PCS
,SUM(CASE WHEN SAMPLE_RELEASE_WK <= SAMPLE_TARGET_WK THEN 1 ELSE 0 END) SO_SUCCESS
,SUM(CASE WHEN SAMPLE_RELEASE_WK <= SAMPLE_TARGET_WK THEN ORDER_QTY ELSE 0 END) PCS_SUCCESS
FROM CONTROL_WIP_READINESS M
WHERE SAMPLE_TARGET_WK >= '202010'
AND SAMPLE_TARGET_WK <= TO_CHAR(TRUNC(SYSDATE)+42,'IYYYIW')
AND SAMPLE_RELEASE_WK IS NULL
AND ORDER_QTY > 0
GROUP BY EDD_W, SAMPLE_TARGET_WK
,DECODE(BU,'NYG','TH','NYV','VN',BU) || CASE WHEN BU = 'TRM' THEN '' ELSE ('-' || CUST_GROUP) END
,NVL(SUB_TEAM,'NA') """)

  conn.commit()
  
  conn.close()
  printttime('EFF_LT_SAMPLE')
  
  
  
class CLS_EFF_LT_DOC(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        EFF_LT_DOC()
        
def EFF_LT_DOC():
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""DELETE FROM EFF_LT_DOC""")

  conn.commit()

  cursor.execute("""INSERT INTO EFF_LT_DOC
SELECT EDD_W,  SO_TARGET_WK AS TARGET_WK
,DECODE(BU,'NYG','TH','NYV','VN',BU) || CASE WHEN BU = 'TRM' THEN '' ELSE ('-' || CUST_GROUP) END AS TEAM
,NVL(SUB_TEAM,'NA') SUB_TEAM
,DECODE(BU,'NYG','TH','NYV','VN',BU) || CASE WHEN BU = 'TRM' THEN '' ELSE ('-' || CUST_GROUP) END || '-' || NVL(SUB_TEAM,'NA') TEAMKEY
,COUNT(SO_NO_DOC) TOTAL_SO
,SUM(ORDER_QTY) TOTAL_PCS
,SUM(CASE WHEN SO_RELEASE_WK <= SO_TARGET_WK THEN 1 ELSE 0 END) SO_SUCCESS
,SUM(CASE WHEN SO_RELEASE_WK <= SO_TARGET_WK THEN ORDER_QTY ELSE 0 END) PCS_SUCCESS
FROM CONTROL_WIP_READINESS M
WHERE  SO_TARGET_WK >= '202010'
AND SO_TARGET_WK <= TO_CHAR(TRUNC(SYSDATE)+42,'IYYYIW')
AND SO_RELEASE_WK IS NOT NULL
AND ORDER_QTY > 0
GROUP BY EDD_W, SO_TARGET_WK
,DECODE(BU,'NYG','TH','NYV','VN',BU) || CASE WHEN BU = 'TRM' THEN '' ELSE ('-' || CUST_GROUP) END
,NVL(SUB_TEAM,'NA')
UNION ALL
SELECT EDD_W,  SO_TARGET_WK AS TARGET_WK
,DECODE(BU,'NYG','TH','NYV','VN',BU) || CASE WHEN BU = 'TRM' THEN '' ELSE ('-' || CUST_GROUP) END AS TEAM
,NVL(SUB_TEAM,'NA') SUB_TEAM
,DECODE(BU,'NYG','TH','NYV','VN',BU) || CASE WHEN BU = 'TRM' THEN '' ELSE ('-' || CUST_GROUP) END || '-' || NVL(SUB_TEAM,'NA') TEAMKEY
,COUNT(SO_NO_DOC) TOTAL_SO
,SUM(ORDER_QTY) TOTAL_PCS
,SUM(CASE WHEN SO_RELEASE_WK <= SO_TARGET_WK THEN 1 ELSE 0 END) SO_SUCCESS
,SUM(CASE WHEN SO_RELEASE_WK <= SO_TARGET_WK THEN ORDER_QTY ELSE 0 END) PCS_SUCCESS
FROM CONTROL_WIP_READINESS M
WHERE SO_TARGET_WK >= '202010'
AND SO_TARGET_WK <= TO_CHAR(TRUNC(SYSDATE)+42,'IYYYIW')
AND SO_RELEASE_WK IS NULL
AND ORDER_QTY > 0
GROUP BY EDD_W, SO_TARGET_WK
,DECODE(BU,'NYG','TH','NYV','VN',BU) || CASE WHEN BU = 'TRM' THEN '' ELSE ('-' || CUST_GROUP) END
,NVL(SUB_TEAM,'NA')  """)

  conn.commit()
  
  conn.close()
  printttime('EFF_LT_DOC')
  
  
class CLS_EFF_LT_FAB(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        EFF_LT_FAB()
        
def EFF_LT_FAB():
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""DELETE FROM EFF_LT_FAB""")

  conn.commit()

  cursor.execute("""INSERT INTO EFF_LT_FAB
SELECT EDD_W,  FAB_TARGET_WK AS TARGET_WK
,FAB_RELEASE_TEAM AS TEAM
,NVL(SCM_ALLOCATE,'NA') SUB_TEAM
,FAB_RELEASE_TEAM||'-'||NVL(SCM_ALLOCATE,'NA')  TEAMKEY
,COUNT(SO_NO_DOC) TOTAL_SO
,SUM(ORDER_QTY) TOTAL_PCS
,SUM(CASE WHEN FAB_RELEASE_WK <= FAB_TARGET_WK THEN 1 ELSE 0 END) SO_SUCCESS
,SUM(CASE WHEN FAB_RELEASE_WK <= FAB_TARGET_WK THEN ORDER_QTY ELSE 0 END) PCS_SUCCESS
FROM CONTROL_WIP_READINESS M
WHERE FAB_TARGET_WK >= '202010'
AND FAB_TARGET_WK <= TO_CHAR(TRUNC(SYSDATE)+42,'IYYYIW')
AND FAB_RELEASE_WK IS NOT NULL
AND ORDER_QTY > 0
GROUP BY EDD_W, FAB_TARGET_WK,FAB_RELEASE_TEAM, NVL(SCM_ALLOCATE,'NA') 
UNION ALL
SELECT EDD_W,  FAB_TARGET_WK AS TARGET_WK
,FAB_RELEASE_TEAM AS TEAM
,NVL(SCM_ALLOCATE,'NA')  SUB_TEAM
,FAB_RELEASE_TEAM||'-'||NVL(SCM_ALLOCATE,'NA')  TEAMKEY
,COUNT(SO_NO_DOC) TOTAL_SO
,SUM(ORDER_QTY) TOTAL_PCS
,0 SO_SUCCESS
,0 PCS_SUCCESS
FROM CONTROL_WIP_READINESS M
WHERE FAB_TARGET_WK >= '202010'
AND FAB_TARGET_WK <= TO_CHAR(TRUNC(SYSDATE)+42,'IYYYIW')
AND FAB_RELEASE_WK IS NULL
AND ORDER_QTY > 0
GROUP BY EDD_W, FAB_TARGET_WK,FAB_RELEASE_TEAM, NVL(SCM_ALLOCATE,'NA')  """)

  conn.commit()
  
  conn.close()
  printttime('EFF_LT_FAB')
  
  
  
  
  
class CLS_EFF_LT_SEW(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        EFF_LT_SEW()
        
def EFF_LT_SEW():
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""DELETE FROM EFF_LT_ACC_SEW""")

  conn.commit()

  cursor.execute("""INSERT INTO EFF_LT_ACC_SEW
SELECT EDD_W,  ACC_SEW_TARGET_WK AS TARGET_WK
,NVL(SCM_ALLOCATE,'NA') AS TEAM
,NVL(SCM_ALLOCATE,'NA')  SUB_TEAM
,NVL(SCM_ALLOCATE,'NA') TEAMKEY
,COUNT(SO_NO_DOC) TOTAL_SO
,SUM(ORDER_QTY) TOTAL_PCS
,SUM(CASE WHEN ACC_SEW_RELEASE_WK <= ACC_SEW_TARGET_WK THEN 1 ELSE 0 END) SO_SUCCESS
,SUM(CASE WHEN ACC_SEW_RELEASE_WK <= ACC_SEW_TARGET_WK THEN ORDER_QTY ELSE 0 END) PCS_SUCCESS
FROM CONTROL_WIP_READINESS M
WHERE  ACC_SEW_TARGET_WK >= '202010'
AND ACC_SEW_TARGET_WK <= TO_CHAR(TRUNC(SYSDATE)+42,'IYYYIW')
AND ACC_SEW_RELEASE_WK IS NOT NULL
AND ORDER_QTY > 0
GROUP BY EDD_W, ACC_SEW_TARGET_WK, NVL(SCM_ALLOCATE,'NA') 
UNION ALL
SELECT EDD_W,  ACC_SEW_TARGET_WK AS TARGET_WK
,NVL(SCM_ALLOCATE,'NA') AS TEAM
,NVL(SCM_ALLOCATE,'NA')  SUB_TEAM
,NVL(SCM_ALLOCATE,'NA') TEAMKEY
,COUNT(SO_NO_DOC) TOTAL_SO
,SUM(ORDER_QTY) TOTAL_PCS
,0 SO_SUCCESS
,0 PCS_SUCCESS
FROM CONTROL_WIP_READINESS M
WHERE ACC_SEW_TARGET_WK >= '202010'
AND ACC_SEW_TARGET_WK <= TO_CHAR(TRUNC(SYSDATE)+42,'IYYYIW')
AND ACC_SEW_RELEASE_WK IS NULL
AND ORDER_QTY > 0
GROUP BY EDD_W, ACC_SEW_TARGET_WK, NVL(SCM_ALLOCATE,'NA')  """)

  conn.commit()
  
  conn.close()
  printttime('EFF_LT_SEW')
  
  
class CLS_EFF_LT_PACK(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        EFF_LT_PACK()
        
def EFF_LT_PACK():
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""DELETE FROM EFF_LT_ACC_PACK""")

  conn.commit()

  cursor.execute("""INSERT INTO EFF_LT_ACC_PACK
SELECT EDD_W,  ACC_PACK_TARGET_WK AS TARGET_WK
,NVL(SCM_ALLOCATE,'NA') AS TEAM
,NVL(SCM_ALLOCATE,'NA')  SUB_TEAM
,NVL(SCM_ALLOCATE,'NA') TEAMKEY
,COUNT(SO_NO_DOC) TOTAL_SO
,SUM(ORDER_QTY) TOTAL_PCS
,SUM(CASE WHEN ACC_PACK_RELEASE_WK <= ACC_PACK_TARGET_WK THEN 1 ELSE 0 END) SO_SUCCESS
,SUM(CASE WHEN ACC_PACK_RELEASE_WK <= ACC_PACK_TARGET_WK THEN ORDER_QTY ELSE 0 END) PCS_SUCCESS
FROM CONTROL_WIP_READINESS M
WHERE ACC_PACK_TARGET_WK >= '202010'
AND ACC_PACK_TARGET_WK <= TO_CHAR(TRUNC(SYSDATE)+42,'IYYYIW')
AND ACC_PACK_RELEASE_WK IS NOT NULL
AND ORDER_QTY > 0
GROUP BY EDD_W, ACC_PACK_TARGET_WK, NVL(SCM_ALLOCATE,'NA') 
UNION ALL
SELECT EDD_W,  ACC_PACK_TARGET_WK AS TARGET_WK
,NVL(SCM_ALLOCATE,'NA') AS TEAM
,NVL(SCM_ALLOCATE,'NA')  SUB_TEAM
,NVL(SCM_ALLOCATE,'NA')  TEAMKEY
,COUNT(SO_NO_DOC) TOTAL_SO
,SUM(ORDER_QTY) TOTAL_PCS
,0 SO_SUCCESS
,0 PCS_SUCCESS
FROM CONTROL_WIP_READINESS M
WHERE ACC_PACK_TARGET_WK >= '202010'
AND ACC_PACK_TARGET_WK <= TO_CHAR(TRUNC(SYSDATE)+42,'IYYYIW')
AND ACC_PACK_RELEASE_WK IS NULL
AND ORDER_QTY > 0
GROUP BY EDD_W, ACC_PACK_TARGET_WK, NVL(SCM_ALLOCATE,'NA') """)

  conn.commit()
  
  conn.close()
  printttime('EFF_LT_PACK')
  
  
  
class CLS_EFF_LT_FC(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        EFF_LT_FC()
        
def EFF_LT_FC():
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""DELETE FROM EFF_LT_FC""")

  conn.commit()

  cursor.execute("""INSERT INTO EFF_LT_FC
SELECT  TO_CHAR(FABRIC_COMPLETE_DATE + 3 ,'IYYYIW') AS TARGET_WK
,NVL(SCM_ALLOCATE,'NA') AS TEAM
,NVL(SCM_ALLOCATE,'NA')  SUB_TEAM
,NVL(SCM_ALLOCATE,'NA') TEAMKEY
,COUNT(SO_NO_DOC) TOTAL_SO
,SUM(ORDER_QTY) TOTAL_PCS
,SUM(FC_RELEASE_HIT) SO_SUCCESS
,SUM(CASE WHEN FC_RELEASE_HIT = 1 THEN ORDER_QTY ELSE 0 END) PCS_SUCCESS
FROM CONTROL_WIP_READINESS M
WHERE TO_CHAR(FABRIC_COMPLETE_DATE + 3 ,'IYYYIW') >= '202010'
AND TO_CHAR(FABRIC_COMPLETE_DATE + 3 ,'IYYYIW') <= TO_CHAR(SYSDATE,'IYYYIW')
AND FC_RELEASE_DATE IS NOT NULL
AND ORDER_QTY > 0
GROUP BY TO_CHAR(FABRIC_COMPLETE_DATE + 3 ,'IYYYIW'), NVL(SCM_ALLOCATE,'NA') 
UNION ALL
SELECT  TO_CHAR(FABRIC_COMPLETE_DATE + 3 ,'IYYYIW') AS TARGET_WK
,NVL(SCM_ALLOCATE,'NA') AS TEAM
,NVL(SCM_ALLOCATE,'NA')  SUB_TEAM
,NVL(SCM_ALLOCATE,'NA') TEAMKEY
,COUNT(SO_NO_DOC) TOTAL_SO
,SUM(ORDER_QTY) TOTAL_PCS
,SUM(FC_RELEASE_HIT) SO_SUCCESS
,SUM(CASE WHEN FC_RELEASE_HIT = 1 THEN ORDER_QTY ELSE 0 END) PCS_SUCCESS
FROM CONTROL_WIP_READINESS M
WHERE TO_CHAR(FABRIC_COMPLETE_DATE + 3 ,'IYYYIW') >= '202010'
AND TO_CHAR(FABRIC_COMPLETE_DATE + 3 ,'IYYYIW') <= TO_CHAR(SYSDATE,'IYYYIW')
AND FC_RELEASE_DATE IS NULL
AND ORDER_QTY > 0
GROUP BY TO_CHAR(FABRIC_COMPLETE_DATE + 3 ,'IYYYIW'), NVL(SCM_ALLOCATE,'NA')  """)

  conn.commit()
  
  conn.close()
  printttime('EFF_LT_FC')
  
  
class CLS_EFF_LT_PATTERN(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        EFF_LT_PATTERN()
        
def EFF_LT_PATTERN():
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""DELETE FROM EFF_LT_PATTERN""")

  conn.commit()

  cursor.execute("""INSERT INTO EFF_LT_PATTERN
SELECT TO_CHAR(FC_RELEASE_DATE + 2 ,'IYYYIW') AS TARGET_WK
,NVL(SCM_ALLOCATE,'NA') AS TEAM
,NVL(SCM_ALLOCATE,'NA')  SUB_TEAM
,NVL(SCM_ALLOCATE,'NA') TEAMKEY
,COUNT(SO_NO_DOC) TOTAL_SO
,SUM(ORDER_QTY) TOTAL_PCS
,SUM(PATTERN_RELEASE_HIT) SO_SUCCESS
,SUM(CASE WHEN PATTERN_RELEASE_HIT = 1 THEN ORDER_QTY ELSE 0 END) PCS_SUCCESS
FROM CONTROL_WIP_READINESS M
WHERE TO_CHAR(FC_RELEASE_DATE + 2 ,'IYYYIW') >= '202010'
AND TO_CHAR(FC_RELEASE_DATE + 2 ,'IYYYIW') <= TO_CHAR(SYSDATE,'IYYYIW')
AND PATTERN_RELEASE_DATE IS NOT NULL
AND ORDER_QTY > 0
GROUP BY TO_CHAR(FC_RELEASE_DATE + 2 ,'IYYYIW'), NVL(SCM_ALLOCATE,'NA') 
UNION ALL
SELECT  TO_CHAR(FC_RELEASE_DATE + 2 ,'IYYYIW') AS TARGET_WK
,NVL(SCM_ALLOCATE,'NA') AS TEAM
,NVL(SCM_ALLOCATE,'NA')  SUB_TEAM
,NVL(SCM_ALLOCATE,'NA') TEAMKEY
,COUNT(SO_NO_DOC) TOTAL_SO
,SUM(ORDER_QTY) TOTAL_PCS
,SUM(FC_RELEASE_HIT) SO_SUCCESS
,SUM(CASE WHEN FC_RELEASE_HIT = 1 THEN ORDER_QTY ELSE 0 END) PCS_SUCCESS
FROM CONTROL_WIP_READINESS M
WHERE TO_CHAR(FC_RELEASE_DATE + 2 ,'IYYYIW') >= '202010'
AND TO_CHAR(FC_RELEASE_DATE + 2 ,'IYYYIW') <= TO_CHAR(SYSDATE,'IYYYIW')
AND PATTERN_RELEASE_DATE IS NULL
AND ORDER_QTY > 0
GROUP BY TO_CHAR(FC_RELEASE_DATE + 2 ,'IYYYIW'), NVL(SCM_ALLOCATE,'NA') """)

  conn.commit()
  
  conn.close()
  printttime('EFF_LT_PATTERN')
  
printttime('Start 7 files')

thread1 = CLS_EFF_LT_PATTERN()
thread2 = CLS_EFF_LT_SAMPLE()
thread3 = CLS_EFF_LT_DOC()
thread4 = CLS_EFF_LT_FAB()
thread5 = CLS_EFF_LT_SEW()
thread6 = CLS_EFF_LT_PACK()
thread7 = CLS_EFF_LT_FC()



thread1.start()
thread2.start()
thread3.start()
thread4.start()
thread5.start()
thread6.start()
thread7.start()


threads = []

threads.append(thread1)
threads.append(thread2)
threads.append(thread3)
threads.append(thread4)
threads.append(thread5)
threads.append(thread6)
threads.append(thread7)





for t in threads:
    t.join()
print (allTxt)
sendLine(allTxt,allHtml)
