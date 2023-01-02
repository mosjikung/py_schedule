# call_Readiness.bat
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
py_file = 'RoomNygReadiness.py '


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
  params = {'subject':'Interface NYG Control Room Readiness','body':txt, 'to':'157bb490.nanyangtextile.com@apac.teams.ms'}
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
 

class CLS_IMPORT_READINESS_RESULT(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        IMPORT_READINESS_RESULT()


def IMPORT_READINESS_RESULT():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""DELETE FROM OE_SO_READINESS_RESULT_TMP""")

  cursor.execute(""" INSERT INTO OE_SO_READINESS_RESULT_TMP
 SELECT BU, VI, MER_NAME, GMT_TYPE, HEAD_MER_NAME, SUB_TEAM, ORDER_QTY, CUST_GROUP, CUST_NAME, BRAND_NAME, SHIPMENT_DATE
 , STYLE_REF, SEA_CODE, SO_YEAR, SO_NO, SO_NO_DOC, EDD_W, SO_RELEASE_DATE, SO_RELEASE_WK, SO_TARGET_WK, SO_RELEASE_HIT
 , SAMPLE_RELEASE_DATE, SAMPLE_RELEASE_WK, SAMPLE_TARGET_WK, SAMPLE_RELEASE_HIT, PATTERN_RELEASE_DATE, PATTERN_RELEASE_WK
 , PATTERN_TARGET_WK, PATTERN_RELEASE_HIT, FC_RELEASE_DATE, FC_RELEASE_WK, FC_TARGET_WK, FC_RELEASE_HIT, PACK_ACC_COMPLETE_DATE
 , ACC_PACK_RELEASE_WK, ACC_PACK_TARGET_WK, ACC_PACK_RELEASE_HIT, SEW_ACC_COMPLETE_DATE, ACC_SEW_RELEASE_WK, ACC_SEW_TARGET_WK
 , ACC_SEW_RELEASE_HIT, FABRIC_COMPLETE_DATE, FAB_RELEASE_WK, FAB_TARGET_WK, FAB_RELEASE_HIT, FIRST_CUT_DATE, SCM_ALLOCATE
 , FC_RELEASE_TEAM, PATTERN_RELEASE_TEAM, FAB_RELEASE_TEAM, ACC_SEW_RELEASE_TEAM, ACC_PACK_RELEASE_TEAM, CREATE_DATE
 , ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FAB_VI, FIRST_EDD
 , TO_CHAR(GET_FR_MRDDATE_BY_SO(BU, SO_YEAR,SO_NO,SHIPMENT_DATE),'IYYYIW') MRD_WK
 , GET_FR_MRDDATE_BY_SO(BU, SO_YEAR,SO_NO,SHIPMENT_DATE) MRD_DATE
 , GET_FR_MRDDATE_BY_SO(BU, SO_YEAR,SO_NO,SHIPMENT_DATE) FIRST_EDD_DATE
FROM OE_SO_READINESS_RESULT_NYG
WHERE ORDER_QTY > 0 AND RDD_W >= '202016' """)

  conn.commit()

  cursor.execute(""" INSERT INTO OE_SO_READINESS_RESULT_TMP
 SELECT BU, VI, MER_NAME, GMT_TYPE, HEAD_MER_NAME, SUB_TEAM, ORDER_QTY, CUST_GROUP, CUST_NAME, BRAND_NAME, SHIPMENT_DATE
 , STYLE_REF, SEA_CODE, SO_YEAR, SO_NO, SO_NO_DOC, EDD_W, SO_RELEASE_DATE, SO_RELEASE_WK, SO_TARGET_WK, SO_RELEASE_HIT
 , SAMPLE_RELEASE_DATE, SAMPLE_RELEASE_WK, SAMPLE_TARGET_WK, SAMPLE_RELEASE_HIT, PATTERN_RELEASE_DATE, PATTERN_RELEASE_WK
 , PATTERN_TARGET_WK, PATTERN_RELEASE_HIT, FC_RELEASE_DATE, FC_RELEASE_WK, FC_TARGET_WK, FC_RELEASE_HIT, PACK_ACC_COMPLETE_DATE
 , ACC_PACK_RELEASE_WK, ACC_PACK_TARGET_WK, ACC_PACK_RELEASE_HIT, SEW_ACC_COMPLETE_DATE, ACC_SEW_RELEASE_WK, ACC_SEW_TARGET_WK
 , ACC_SEW_RELEASE_HIT, FABRIC_COMPLETE_DATE, FAB_RELEASE_WK, FAB_TARGET_WK, FAB_RELEASE_HIT, FIRST_CUT_DATE, SCM_ALLOCATE
 , FC_RELEASE_TEAM, PATTERN_RELEASE_TEAM, FAB_RELEASE_TEAM, ACC_SEW_RELEASE_TEAM, ACC_PACK_RELEASE_TEAM, CREATE_DATE
 , ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FAB_VI, FIRST_EDD
 , TO_CHAR(GET_FR_MRDDATE_BY_SO(BU, SO_YEAR,SO_NO,SHIPMENT_DATE),'IYYYIW') MRD_WK
 , GET_FR_MRDDATE_BY_SO(BU, SO_YEAR,SO_NO,SHIPMENT_DATE) MRD_DATE
 , GET_FR_MRDDATE_BY_SO(BU, SO_YEAR,SO_NO,SHIPMENT_DATE) FIRST_EDD_DATE
FROM OE_SO_READINESS_RESULT_TRM
WHERE ORDER_QTY > 0 AND RDD_W >= '202016' """)

  conn.commit()

  cursor.execute(""" INSERT INTO OE_SO_READINESS_RESULT_TMP
 SELECT BU, VI, MER_NAME, GMT_TYPE, HEAD_MER_NAME, SUB_TEAM, ORDER_QTY, CUST_GROUP, CUST_NAME, BRAND_NAME, SHIPMENT_DATE, STYLE_REF, SEA_CODE, SO_YEAR, SO_NO, SO_NO_DOC, EDD_W, SO_RELEASE_DATE, SO_RELEASE_WK, SO_TARGET_WK, SO_RELEASE_HIT, SAMPLE_RELEASE_DATE, SAMPLE_RELEASE_WK, SAMPLE_TARGET_WK, SAMPLE_RELEASE_HIT, PATTERN_RELEASE_DATE, PATTERN_RELEASE_WK, PATTERN_TARGET_WK, PATTERN_RELEASE_HIT, FC_RELEASE_DATE, FC_RELEASE_WK, FC_TARGET_WK, FC_RELEASE_HIT, PACK_ACC_COMPLETE_DATE, ACC_PACK_RELEASE_WK, ACC_PACK_TARGET_WK, ACC_PACK_RELEASE_HIT, SEW_ACC_COMPLETE_DATE, ACC_SEW_RELEASE_WK, ACC_SEW_TARGET_WK, ACC_SEW_RELEASE_HIT, FABRIC_COMPLETE_DATE, FAB_RELEASE_WK, FAB_TARGET_WK, FAB_RELEASE_HIT, FIRST_CUT_DATE, SCM_ALLOCATE, FC_RELEASE_TEAM, PATTERN_RELEASE_TEAM, FAB_RELEASE_TEAM, ACC_SEW_RELEASE_TEAM, ACC_PACK_RELEASE_TEAM, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FAB_VI
, FIRST_EDD, FIRST_EDD MRD_WK, TRUNC(SHIPMENT_DATE)-35 MRD_DATE, TRUNC(SHIPMENT_DATE)-35 FIRST_EDD_DATE
FROM OE_SO_READINESS_RESULT_NGW
WHERE ORDER_QTY > 0 AND RDD_W >= '202016' """)

  conn.commit()

  try:

    cursor.execute(""" INSERT INTO OE_SO_READINESS_RESULT_TMP
  SELECT BU, VI, MER_NAME, GMT_TYPE, HEAD_MER_NAME, SUB_TEAM, ORDER_QTY, CUST_GROUP, CUST_NAME, BRAND_NAME, SHIPMENT_DATE
  , STYLE_REF, SEA_CODE, SO_YEAR, SO_NO, SO_NO_DOC, EDD_W, SO_RELEASE_DATE, SO_RELEASE_WK, SO_TARGET_WK, SO_RELEASE_HIT
  , SAMPLE_RELEASE_DATE, SAMPLE_RELEASE_WK, SAMPLE_TARGET_WK, SAMPLE_RELEASE_HIT, PATTERN_RELEASE_DATE, PATTERN_RELEASE_WK
  , PATTERN_TARGET_WK, PATTERN_RELEASE_HIT, FC_RELEASE_DATE, FC_RELEASE_WK, FC_TARGET_WK, FC_RELEASE_HIT, PACK_ACC_COMPLETE_DATE
  , ACC_PACK_RELEASE_WK, ACC_PACK_TARGET_WK, ACC_PACK_RELEASE_HIT, SEW_ACC_COMPLETE_DATE, ACC_SEW_RELEASE_WK, ACC_SEW_TARGET_WK
  , ACC_SEW_RELEASE_HIT, FABRIC_COMPLETE_DATE, FAB_RELEASE_WK, FAB_TARGET_WK, FAB_RELEASE_HIT, FIRST_CUT_DATE, SCM_ALLOCATE
  , FC_RELEASE_TEAM, PATTERN_RELEASE_TEAM, FAB_RELEASE_TEAM, ACC_SEW_RELEASE_TEAM, ACC_PACK_RELEASE_TEAM, CREATE_DATE
  , ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FAB_VI, FIRST_EDD
  , TO_CHAR(GET_FR_MRDDATE_BY_SO(BU, SO_YEAR,SO_NO,SHIPMENT_DATE),'IYYYIW') MRD_WK
  , GET_FR_MRDDATE_BY_SO(BU, SO_YEAR,SO_NO,SHIPMENT_DATE) MRD_DATE
  , GET_FR_MRDDATE_BY_SO(BU, SO_YEAR,SO_NO,SHIPMENT_DATE) FIRST_EDD_DATE
  FROM OE_SO_READINESS_RESULT_NYV
  WHERE ORDER_QTY > 0 AND RDD_W >= '202016' """)

    conn.commit()
  except:
    print('OE_SO_READINESS_RESULT_TMP VN Error')


  cursor.execute("""DELETE FROM CONTROL_WIP_READINESS""")

  conn.commit()


  cursor.execute("""INSERT INTO CONTROL_WIP_READINESS
                  SELECT RELOAD_DATE, BU, VI, MER_NAME, GMT_TYPE, HEAD_MER_NAME,
SUB_TEAM, ORDER_QTY,  CUST_GROUP,  CUST_NAME,   BRAND_NAME,
SHIPMENT_DATE,  STYLE_REF, SEA_CODE, SO_YEAR, SO_NO, SO_NO_DOC,
EDD_W, SO_RELEASE_DATE, SO_RELEASE_WK, SO_TARGET_WK,
CASE WHEN SO_RELEASE_WK <= SO_TARGET_WK THEN 1 ELSE 0 END SO_RELEASE_HIT,
 SAMPLE_RELEASE_DATE, SAMPLE_RELEASE_WK, SAMPLE_TARGET_WK,
 CASE WHEN SAMPLE_RELEASE_WK <= SAMPLE_TARGET_WK THEN 1 ELSE 0 END SAMPLE_RELEASE_HIT,
  PATTERN_RELEASE_DATE,
                  PATTERN_RELEASE_WK,
                  PATTERN_TARGET_WK,
                  PATTERN_RELEASE_HIT,
                  FC_RELEASE_DATE,
                  FC_RELEASE_WK,
                  FC_TARGET_WK,
                  FC_RELEASE_HIT,
                  PACK_ACC_COMPLETE_DATE,
                  ACC_PACK_RELEASE_WK,
                  ACC_PACK_TARGET_WK,
                  CASE WHEN ACC_PACK_RELEASE_WK <= ACC_PACK_TARGET_WK THEN 1 ELSE 0 END ACC_PACK_RELEASE_HIT,
                  SEW_ACC_COMPLETE_DATE,
                  ACC_SEW_RELEASE_WK,
                  ACC_SEW_TARGET_WK,
                  CASE WHEN ACC_SEW_RELEASE_WK <= ACC_SEW_TARGET_WK THEN 1 ELSE 0 END ACC_SEW_RELEASE_HIT,
                  FABRIC_COMPLETE_DATE,
                  FAB_RELEASE_WK,
                  FAB_TARGET_WK,
                  CASE WHEN FAB_RELEASE_WK <= FAB_TARGET_WK THEN 1 ELSE 0 END FAB_RELEASE_HIT,
                  FIRST_CUT_DATE,
                  SCM_ALLOCATE,
                  FC_RELEASE_TEAM,
                  PATTERN_RELEASE_TEAM,
                  FAB_RELEASE_TEAM,
                  ACC_SEW_RELEASE_TEAM,
                  ACC_PACK_RELEASE_TEAM,
                  CREATE_DATE,
                  ORDER_TYPE,
                  ORDER_TYPE_DESC,
                  RDD_W,
                  CASE WHEN FAB_VI = 'Y' THEN 'SCM VI' ELSE
                          CASE WHEN NVL(SCM_ALLOCATE,'NA')  LIKE 'G%' AND CUST_GROUP = 'NIKE' THEN 'NIKE'
                          ELSE 'PROCUREMENT' END END NEW_PROCUREMENT_TEAM,
                  CASE WHEN NVL(SCM_ALLOCATE,'NA')  LIKE 'G%' AND CUST_GROUP = 'NIKE' THEN CUST_GROUP ELSE ACC_SEW_RELEASE_TEAM END NEW_PROCUREMENT_TEAM2
                  ,0,0,0
                  ,FAB_VI, 0, 0
    ,(SELECT NVL(PR_FLAG,'N') FROM control_pr_po V WHERE RM_TYPE = 'FABRIC' AND   V.BU = MM.BU and V.SO_NO_DOC = MM.SO_NO_DOC)  PR_FAB 
   ,(SELECT NVL(PO_FLAG,'N') FROM control_pr_po V WHERE RM_TYPE = 'FABRIC' AND   V.BU = MM.BU and V.SO_NO_DOC = MM.SO_NO_DOC)  PO_FAB 
    ,(SELECT NVL(PR_FLAG,'N') FROM control_pr_po V WHERE RM_TYPE = 'SEW' AND   V.BU = MM.BU and V.SO_NO_DOC = MM.SO_NO_DOC)  PR_SEW 
   ,(SELECT NVL(PO_FLAG,'N') FROM control_pr_po V WHERE RM_TYPE = 'SEW' AND   V.BU = MM.BU and V.SO_NO_DOC = MM.SO_NO_DOC)  PO_SEW 
    ,(SELECT NVL(PR_FLAG,'N') FROM control_pr_po V WHERE RM_TYPE = 'PACK' AND   V.BU = MM.BU and V.SO_NO_DOC = MM.SO_NO_DOC)  PR_PACK 
   ,(SELECT NVL(PO_FLAG,'N') FROM control_pr_po V WHERE RM_TYPE = 'PACK' AND   V.BU = MM.BU and V.SO_NO_DOC = MM.SO_NO_DOC)  PO_PACK 
   , FIRST_EDD, MRD_WK, MRD_DATE, FIRST_EDD_DATE
FROM (
 SELECT SYSDATE AS RELOAD_DATE, 
                  BU,
                  VI,
                  MER_NAME,
                  GMT_TYPE,
                  HEAD_MER_NAME,
                  SUB_TEAM,
                  ORDER_QTY,
                  CUST_GROUP,
                  CUST_NAME,
                  BRAND_NAME,
                  SHIPMENT_DATE,
                  STYLE_REF,
                  SEA_CODE,
                  SO_YEAR,
                  SO_NO,
                  SO_NO_DOC,
                  EDD_W,
                  SO_RELEASE_DATE,
                  SO_RELEASE_WK,
                  CASE WHEN ORDER_TYPE = '02' THEN TO_NUMBER(GET_READINESS_FIND_TARGET(RDD_W,4))
                        ELSE TO_NUMBER(GET_READINESS_FIND_TARGET(RDD_W,6)) END AS SO_TARGET_WK,
                  SO_RELEASE_HIT,
                  SAMPLE_RELEASE_DATE,
                  SAMPLE_RELEASE_WK,
                  CASE WHEN ORDER_TYPE = '02' THEN TO_NUMBER(GET_READINESS_FIND_TARGET(RDD_W,4))
                        ELSE TO_NUMBER(GET_READINESS_FIND_TARGET(RDD_W,6)) END AS SAMPLE_TARGET_WK,
                  SAMPLE_RELEASE_HIT,
                  PATTERN_RELEASE_DATE,
                  PATTERN_RELEASE_WK,
                  PATTERN_TARGET_WK,
                  PATTERN_RELEASE_HIT,
                  FC_RELEASE_DATE,
                  FC_RELEASE_WK,
                  FC_TARGET_WK,
                  FC_RELEASE_HIT,
                  PACK_ACC_COMPLETE_DATE,
                  ACC_PACK_RELEASE_WK,
                  MRD_WK AS ACC_PACK_TARGET_WK,
                  ACC_PACK_RELEASE_HIT,
                  SEW_ACC_COMPLETE_DATE,
                  ACC_SEW_RELEASE_WK,
                  MRD_WK AS ACC_SEW_TARGET_WK,
                  ACC_SEW_RELEASE_HIT,
                  FABRIC_COMPLETE_DATE,
                  FAB_RELEASE_WK,
                  MRD_WK AS FAB_TARGET_WK,
                  FAB_RELEASE_HIT,
                  FIRST_CUT_DATE,
                  SCM_ALLOCATE,
                  FC_RELEASE_TEAM,
                  PATTERN_RELEASE_TEAM,
                  FAB_RELEASE_TEAM,
                  ACC_SEW_RELEASE_TEAM,
                  ACC_PACK_RELEASE_TEAM,
                  CREATE_DATE,
                  ORDER_TYPE,
                  ORDER_TYPE_DESC,
                  RDD_W,
                  FAB_VI,
                  FIRST_EDD,
                  MRD_WK,
                  MRD_DATE,
                  FIRST_EDD_DATE
                 FROM (
                 
               SELECT BU, VI, MER_NAME, GMT_TYPE, HEAD_MER_NAME, SUB_TEAM, ORDER_QTY, CUST_GROUP, CUST_NAME, BRAND_NAME
                  , SHIPMENT_DATE, STYLE_REF, SEA_CODE, SO_YEAR, SO_NO, SO_NO_DOC, EDD_W, SO_RELEASE_DATE, SO_RELEASE_WK, SO_TARGET_WK
                  , SO_RELEASE_HIT, SAMPLE_RELEASE_DATE, SAMPLE_RELEASE_WK, SAMPLE_TARGET_WK
                  , SAMPLE_RELEASE_HIT, PATTERN_RELEASE_DATE, PATTERN_RELEASE_WK, PATTERN_TARGET_WK
                  , PATTERN_RELEASE_HIT, FC_RELEASE_DATE, FC_RELEASE_WK, FC_TARGET_WK, FC_RELEASE_HIT
                  , PACK_ACC_COMPLETE_DATE, ACC_PACK_RELEASE_WK, ACC_PACK_TARGET_WK, ACC_PACK_RELEASE_HIT
                  , SEW_ACC_COMPLETE_DATE, ACC_SEW_RELEASE_WK, ACC_SEW_TARGET_WK, ACC_SEW_RELEASE_HIT, FABRIC_COMPLETE_DATE
                  , FAB_RELEASE_WK, FAB_TARGET_WK, FAB_RELEASE_HIT, FIRST_CUT_DATE, SCM_ALLOCATE, FC_RELEASE_TEAM, PATTERN_RELEASE_TEAM
                  , FAB_RELEASE_TEAM, ACC_SEW_RELEASE_TEAM, ACC_PACK_RELEASE_TEAM, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W
                  , FAB_VI, FIRST_EDD, MRD_WK, MRD_DATE, FIRST_EDD_DATE
                FROM OE_SO_READINESS_RESULT_TMP
                WHERE ORDER_QTY > 0 

                 ) M 
                 WHERE 1 = 1
                 ) MM  """)

  conn.commit()

#   cursor.execute("""DELETE FROM PPR_BOARD3""")

#   conn.commit()

#   cursor.execute("""INSERT INTO PPR_BOARD3
# SELECT BU, TO_CHAR(trunc(SYSDATE)-0, 'IYYYIW') CWEEK, M.SO_YEAR, SO_NO, SO_NO_DOC, STYLE_REF, EDD_W, CUST_NAME, BRAND_NAME, ORDER_QTY, VI, MER_NAME
# 	, SHIPMENT_DATE,  RDD_W
#     , CASE WHEN ORDER_TYPE = '02' THEN 4 ELSE 5 END AS BACK_PERIOD
# 	, ORDER_TYPE_DESC
# 	, MRD_WK, MRD_DATE
# 	, GMT_TYPE
# 	, CASE WHEN GMT_TYPE = 'MASK' THEN 'Yes' ELSE 'No' END ISMASK
# 	, FIRST_EDD, FIRST_EDD_DATE
# 	, GET_READINESS_USED_WEEK2( FIRST_EDD, RDD_W) - (CASE WHEN ORDER_TYPE = '02' THEN 4 ELSE 5 END) PULL_WK
# 	,CREATE_DATE AS SO_DATE
# 	,NEW_PROCUREMENT_TEAM AS GRP
# 	,NVL(TRIM(SCM_ALLOCATE),'NA') LOC
# 	,(SELECT COUNT(*) FROM CONTROL_STYLE_FOCUS F WHERE UPPER(F.STYLE_CODE) = UPPER(M.STYLE_REF)) STYLE_FOCUS
#     ,(SELECT LISTAGG(PURCHASER_NAME, ',') WITHIN GROUP (ORDER BY PURCHASER_NAME) 
#                 from
#                        (select distinct BU_CODE, SO_NO_DOC, PURCHASER_NAME
# 						FROM OE_SO_READINESS_PO_V_TMP2 P 
# 						WHERE P.RM_TYPE LIKE 'FABRIC') P
#                         WHERE P.BU_CODE = M.BU 
#                         AND P.SO_NO_DOC = M.SO_NO_DOC) PURCHASER_NAME
# 			,FAB_PO_PENDING PO_PENDING, FAB_PO_VI, FAB_PO_NONVI, ORDER_TYPE
#             ,TO_NUMBER(GET_READINESS_FIND_TARGET(RDD_W,CASE WHEN ORDER_TYPE = '02' THEN 4 ELSE 5 END)) TARGET_BY_RDD
#             ,MRD_WK TARGET_BY_MRD
# 			FROM control_wip_readiness M 
# 			WHERE FABRIC_COMPLETE_DATE IS NULL
# 			AND FAB_PO_PENDING > 0
# 			AND RDD_W >= '202016'  """)

#   conn.commit()



  conn.close()



  printttime('IMPORT_READINESS_RESULT')



  
  
class CLS_CONTROL_WIP_READINESS(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        CONTROL_WIP_READINESS()
        
def CONTROL_WIP_READINESS():
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""DELETE FROM CONTROL_WIP_READINESS""")

  conn.commit()

  cursor.execute("""INSERT INTO CONTROL_WIP_READINESS
                  SELECT RELOAD_DATE, BU, VI, MER_NAME, GMT_TYPE, HEAD_MER_NAME,
SUB_TEAM, ORDER_QTY,  CUST_GROUP,  CUST_NAME,   BRAND_NAME,
SHIPMENT_DATE,  STYLE_REF, SEA_CODE, SO_YEAR, SO_NO, SO_NO_DOC,
EDD_W, SO_RELEASE_DATE, SO_RELEASE_WK, SO_TARGET_WK,
CASE WHEN SO_RELEASE_WK <= SO_TARGET_WK THEN 1 ELSE 0 END SO_RELEASE_HIT,
 SAMPLE_RELEASE_DATE, SAMPLE_RELEASE_WK, SAMPLE_TARGET_WK,
 CASE WHEN SAMPLE_RELEASE_WK <= SAMPLE_TARGET_WK THEN 1 ELSE 0 END SAMPLE_RELEASE_HIT,
  PATTERN_RELEASE_DATE,
                  PATTERN_RELEASE_WK,
                  PATTERN_TARGET_WK,
                  PATTERN_RELEASE_HIT,
                  FC_RELEASE_DATE,
                  FC_RELEASE_WK,
                  FC_TARGET_WK,
                  FC_RELEASE_HIT,
                  PACK_ACC_COMPLETE_DATE,
                  ACC_PACK_RELEASE_WK,
                  ACC_PACK_TARGET_WK,
                  ACC_PACK_RELEASE_HIT,
                  SEW_ACC_COMPLETE_DATE,
                  ACC_SEW_RELEASE_WK,
                  ACC_SEW_TARGET_WK,
                  ACC_SEW_RELEASE_HIT,
                  FABRIC_COMPLETE_DATE,
                  FAB_RELEASE_WK,
                  FAB_TARGET_WK,
                  CASE WHEN FAB_RELEASE_WK <= FAB_TARGET_WK THEN 1 ELSE 0 END FAB_RELEASE_HIT,
                  FIRST_CUT_DATE,
                  SCM_ALLOCATE,
                  FC_RELEASE_TEAM,
                  PATTERN_RELEASE_TEAM,
                  FAB_RELEASE_TEAM,
                  ACC_SEW_RELEASE_TEAM,
                  ACC_PACK_RELEASE_TEAM,
                  CREATE_DATE,
                  ORDER_TYPE,
                  ORDER_TYPE_DESC,
                  RDD_W,
                  CASE WHEN FAB_VI = 'Y' THEN 'SCM VI' ELSE
                          CASE WHEN NVL(SCM_ALLOCATE,'NA')  LIKE 'G%' AND CUST_GROUP = 'NIKE' THEN 'NIKE'
                          ELSE 'PROCUREMENT' END END NEW_PROCUREMENT_TEAM,
                  CASE WHEN NVL(SCM_ALLOCATE,'NA')  LIKE 'G%' AND CUST_GROUP = 'NIKE' THEN CUST_GROUP ELSE ACC_SEW_RELEASE_TEAM END NEW_PROCUREMENT_TEAM2
                  ,0,0,0
                  ,FAB_VI, 0, 0
    ,(SELECT NVL(PR_FLAG,'N') FROM control_pr_po V WHERE RM_TYPE = 'FABRIC' AND   V.BU = MM.BU and V.SO_NO_DOC = MM.SO_NO_DOC)  PR_FAB 
   ,(SELECT NVL(PO_FLAG,'N') FROM control_pr_po V WHERE RM_TYPE = 'FABRIC' AND   V.BU = MM.BU and V.SO_NO_DOC = MM.SO_NO_DOC)  PO_FAB 
    ,(SELECT NVL(PR_FLAG,'N') FROM control_pr_po V WHERE RM_TYPE = 'SEW' AND   V.BU = MM.BU and V.SO_NO_DOC = MM.SO_NO_DOC)  PR_SEW 
   ,(SELECT NVL(PO_FLAG,'N') FROM control_pr_po V WHERE RM_TYPE = 'SEW' AND   V.BU = MM.BU and V.SO_NO_DOC = MM.SO_NO_DOC)  PO_SEW 
    ,(SELECT NVL(PR_FLAG,'N') FROM control_pr_po V WHERE RM_TYPE = 'PACK' AND   V.BU = MM.BU and V.SO_NO_DOC = MM.SO_NO_DOC)  PR_PACK 
   ,(SELECT NVL(PO_FLAG,'N') FROM control_pr_po V WHERE RM_TYPE = 'PACK' AND   V.BU = MM.BU and V.SO_NO_DOC = MM.SO_NO_DOC)  PO_PACK 
   , FIRST_EDD, MRD_WK, MRD_DATE, FIRST_EDD_DATE
FROM (
 SELECT SYSDATE AS RELOAD_DATE, 
                  BU,
                  VI,
                  MER_NAME,
                  GMT_TYPE,
                  HEAD_MER_NAME,
                  SUB_TEAM,
                  ORDER_QTY,
                  CUST_GROUP,
                  CUST_NAME,
                  BRAND_NAME,
                  SHIPMENT_DATE,
                  STYLE_REF,
                  SEA_CODE,
                  SO_YEAR,
                  SO_NO,
                  SO_NO_DOC,
                  EDD_W,
                  SO_RELEASE_DATE,
                  SO_RELEASE_WK,
                  CASE WHEN ORDER_TYPE = '02' THEN TO_NUMBER(GET_READINESS_FIND_TARGET(RDD_W,4))
                        ELSE TO_NUMBER(GET_READINESS_FIND_TARGET(RDD_W,6)) END AS SO_TARGET_WK,
                  SO_RELEASE_HIT,
                  SAMPLE_RELEASE_DATE,
                  SAMPLE_RELEASE_WK,
                  CASE WHEN ORDER_TYPE = '02' THEN TO_NUMBER(GET_READINESS_FIND_TARGET(RDD_W,4))
                        ELSE TO_NUMBER(GET_READINESS_FIND_TARGET(RDD_W,6)) END AS SAMPLE_TARGET_WK,
                  SAMPLE_RELEASE_HIT,
                  PATTERN_RELEASE_DATE,
                  PATTERN_RELEASE_WK,
                  PATTERN_TARGET_WK,
                  PATTERN_RELEASE_HIT,
                  FC_RELEASE_DATE,
                  FC_RELEASE_WK,
                  FC_TARGET_WK,
                  FC_RELEASE_HIT,
                  PACK_ACC_COMPLETE_DATE,
                  ACC_PACK_RELEASE_WK,
                  ACC_PACK_TARGET_WK,
                  ACC_PACK_RELEASE_HIT,
                  SEW_ACC_COMPLETE_DATE,
                  ACC_SEW_RELEASE_WK,
                  ACC_SEW_TARGET_WK,
                  ACC_SEW_RELEASE_HIT,
                  FABRIC_COMPLETE_DATE,
                  FAB_RELEASE_WK,
                  CASE WHEN ORDER_TYPE = '02' THEN TO_NUMBER(GET_READINESS_FIND_TARGET(MRD_WK,4))
                        ELSE TO_NUMBER(GET_READINESS_FIND_TARGET(MRD_WK,6)) END AS FAB_TARGET_WK,
                  FAB_RELEASE_HIT,
                  FIRST_CUT_DATE,
                  SCM_ALLOCATE,
                  FC_RELEASE_TEAM,
                  PATTERN_RELEASE_TEAM,
                  FAB_RELEASE_TEAM,
                  ACC_SEW_RELEASE_TEAM,
                  ACC_PACK_RELEASE_TEAM,
                  CREATE_DATE,
                  ORDER_TYPE,
                  ORDER_TYPE_DESC,
                  RDD_W,
                  FAB_VI,
                  FIRST_EDD,
                  MRD_WK,
                  MRD_DATE,
                  FIRST_EDD_DATE
                 FROM (
                 
                  SELECT BU, VI, MER_NAME, GMT_TYPE, HEAD_MER_NAME, SUB_TEAM, ORDER_QTY, CUST_GROUP, CUST_NAME, BRAND_NAME, SHIPMENT_DATE, STYLE_REF, SEA_CODE, SO_YEAR, SO_NO, SO_NO_DOC, EDD_W, SO_RELEASE_DATE, SO_RELEASE_WK, SO_TARGET_WK, SO_RELEASE_HIT, SAMPLE_RELEASE_DATE, SAMPLE_RELEASE_WK, SAMPLE_TARGET_WK, SAMPLE_RELEASE_HIT, PATTERN_RELEASE_DATE, PATTERN_RELEASE_WK, PATTERN_TARGET_WK, PATTERN_RELEASE_HIT, FC_RELEASE_DATE, FC_RELEASE_WK, FC_TARGET_WK, FC_RELEASE_HIT, PACK_ACC_COMPLETE_DATE, ACC_PACK_RELEASE_WK, ACC_PACK_TARGET_WK, ACC_PACK_RELEASE_HIT, SEW_ACC_COMPLETE_DATE, ACC_SEW_RELEASE_WK, ACC_SEW_TARGET_WK, ACC_SEW_RELEASE_HIT, FABRIC_COMPLETE_DATE, FAB_RELEASE_WK, FAB_TARGET_WK, FAB_RELEASE_HIT, FIRST_CUT_DATE, SCM_ALLOCATE, FC_RELEASE_TEAM, PATTERN_RELEASE_TEAM, FAB_RELEASE_TEAM, ACC_SEW_RELEASE_TEAM, ACC_PACK_RELEASE_TEAM, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FAB_VI, FIRST_EDD, FIRST_EDD MRD_WK, TRUNC(SYSDATE) MRD_DATE, TRUNC(SYSDATE) FIRST_EDD_DATE
FROM OE_SO_READINESS_RESULT_NYG
WHERE ORDER_QTY > 0 AND FIRST_EDD >= '202016'
UNION ALL
SELECT BU, VI, MER_NAME, GMT_TYPE, HEAD_MER_NAME, SUB_TEAM, ORDER_QTY, CUST_GROUP, CUST_NAME, BRAND_NAME, SHIPMENT_DATE, STYLE_REF, SEA_CODE, SO_YEAR, SO_NO, SO_NO_DOC, EDD_W, SO_RELEASE_DATE, SO_RELEASE_WK, SO_TARGET_WK, SO_RELEASE_HIT, SAMPLE_RELEASE_DATE, SAMPLE_RELEASE_WK, SAMPLE_TARGET_WK, SAMPLE_RELEASE_HIT, PATTERN_RELEASE_DATE, PATTERN_RELEASE_WK, PATTERN_TARGET_WK, PATTERN_RELEASE_HIT, FC_RELEASE_DATE, FC_RELEASE_WK, FC_TARGET_WK, FC_RELEASE_HIT, PACK_ACC_COMPLETE_DATE, ACC_PACK_RELEASE_WK, ACC_PACK_TARGET_WK, ACC_PACK_RELEASE_HIT, SEW_ACC_COMPLETE_DATE, ACC_SEW_RELEASE_WK, ACC_SEW_TARGET_WK, ACC_SEW_RELEASE_HIT, FABRIC_COMPLETE_DATE, FAB_RELEASE_WK, FAB_TARGET_WK, FAB_RELEASE_HIT, FIRST_CUT_DATE, SCM_ALLOCATE, FC_RELEASE_TEAM, PATTERN_RELEASE_TEAM, FAB_RELEASE_TEAM, ACC_SEW_RELEASE_TEAM, ACC_PACK_RELEASE_TEAM, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FAB_VI, FIRST_EDD, FIRST_EDD MRD_WK, TRUNC(SYSDATE) MRD_DATE, TRUNC(SYSDATE) FIRST_EDD_DATE
FROM OE_SO_READINESS_RESULT_TRM
WHERE ORDER_QTY > 0 AND FIRST_EDD >= '202016'
UNION ALL
SELECT BU, VI, MER_NAME, GMT_TYPE, HEAD_MER_NAME, SUB_TEAM, ORDER_QTY, CUST_GROUP, CUST_NAME, BRAND_NAME, SHIPMENT_DATE, STYLE_REF, SEA_CODE, SO_YEAR, SO_NO, SO_NO_DOC, EDD_W, SO_RELEASE_DATE, SO_RELEASE_WK, SO_TARGET_WK, SO_RELEASE_HIT, SAMPLE_RELEASE_DATE, SAMPLE_RELEASE_WK, SAMPLE_TARGET_WK, SAMPLE_RELEASE_HIT, PATTERN_RELEASE_DATE, PATTERN_RELEASE_WK, PATTERN_TARGET_WK, PATTERN_RELEASE_HIT, FC_RELEASE_DATE, FC_RELEASE_WK, FC_TARGET_WK, FC_RELEASE_HIT, PACK_ACC_COMPLETE_DATE, ACC_PACK_RELEASE_WK, ACC_PACK_TARGET_WK, ACC_PACK_RELEASE_HIT, SEW_ACC_COMPLETE_DATE, ACC_SEW_RELEASE_WK, ACC_SEW_TARGET_WK, ACC_SEW_RELEASE_HIT, FABRIC_COMPLETE_DATE, FAB_RELEASE_WK, FAB_TARGET_WK, FAB_RELEASE_HIT, FIRST_CUT_DATE, SCM_ALLOCATE, FC_RELEASE_TEAM, PATTERN_RELEASE_TEAM, FAB_RELEASE_TEAM, ACC_SEW_RELEASE_TEAM, ACC_PACK_RELEASE_TEAM, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FAB_VI, FIRST_EDD, FIRST_EDD MRD_WK, TRUNC(SYSDATE) MRD_DATE, TRUNC(SYSDATE) FIRST_EDD_DATE
FROM OE_SO_READINESS_RESULT_NGW
WHERE ORDER_QTY > 0 AND FIRST_EDD >= '202016'
                 ) M 
                 WHERE 1 = 1
                 AND M.ORDER_QTY > 0 
                 AND (FIRST_EDD >= '202016' OR MRD_WK >= '202016')) MM  """)


  conn.commit()
  conn.close()

  CONTROL_WIP_READINESS_02()

  printttime('CONTROL_WIP_READINESS_01')


def CONTROL_WIP_READINESS_02():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  try:
    cursor = conn.cursor()

    cursor.execute("""INSERT INTO CONTROL_WIP_READINESS
                  SELECT RELOAD_DATE, BU, VI, MER_NAME, GMT_TYPE, HEAD_MER_NAME,
SUB_TEAM, ORDER_QTY,  CUST_GROUP,  CUST_NAME,   BRAND_NAME,
SHIPMENT_DATE,  STYLE_REF, SEA_CODE, SO_YEAR, SO_NO, SO_NO_DOC,
EDD_W, SO_RELEASE_DATE, SO_RELEASE_WK, SO_TARGET_WK,
CASE WHEN SO_RELEASE_WK <= SO_TARGET_WK THEN 1 ELSE 0 END SO_RELEASE_HIT,
 SAMPLE_RELEASE_DATE, SAMPLE_RELEASE_WK, SAMPLE_TARGET_WK,
 CASE WHEN SAMPLE_RELEASE_WK <= SAMPLE_TARGET_WK THEN 1 ELSE 0 END SAMPLE_RELEASE_HIT,
  PATTERN_RELEASE_DATE,
                  PATTERN_RELEASE_WK,
                  PATTERN_TARGET_WK,
                  PATTERN_RELEASE_HIT,
                  FC_RELEASE_DATE,
                  FC_RELEASE_WK,
                  FC_TARGET_WK,
                  FC_RELEASE_HIT,
                  PACK_ACC_COMPLETE_DATE,
                  ACC_PACK_RELEASE_WK,
                  ACC_PACK_TARGET_WK,
                  ACC_PACK_RELEASE_HIT,
                  SEW_ACC_COMPLETE_DATE,
                  ACC_SEW_RELEASE_WK,
                  ACC_SEW_TARGET_WK,
                  ACC_SEW_RELEASE_HIT,
                  FABRIC_COMPLETE_DATE,
                  FAB_RELEASE_WK,
                  FAB_TARGET_WK,
                  CASE WHEN FAB_RELEASE_WK <= FAB_TARGET_WK THEN 1 ELSE 0 END FAB_RELEASE_HIT,
                  FIRST_CUT_DATE,
                  SCM_ALLOCATE,
                  FC_RELEASE_TEAM,
                  PATTERN_RELEASE_TEAM,
                  FAB_RELEASE_TEAM,
                  ACC_SEW_RELEASE_TEAM,
                  ACC_PACK_RELEASE_TEAM,
                  CREATE_DATE,
                  ORDER_TYPE,
                  ORDER_TYPE_DESC,
                  RDD_W,
                  CASE WHEN FAB_VI = 'Y' THEN 'SCM VI' ELSE
                          CASE WHEN CUST_GROUP = 'NIKE' THEN 'NIKE'
                          ELSE 'PROCUREMENT' END END NEW_PROCUREMENT_TEAM,
                  CASE WHEN CUST_GROUP = 'NIKE' THEN CUST_GROUP ELSE ACC_SEW_RELEASE_TEAM END NEW_PROCUREMENT_TEAM2
                  ,0,0,0
                  ,FAB_VI, 0, 0
    ,(SELECT NVL(PR_FLAG,'N') FROM control_pr_po V WHERE RM_TYPE = 'FABRIC' AND   V.BU = MM.BU and V.SO_NO_DOC = MM.SO_NO_DOC)  PR_FAB 
   ,(SELECT NVL(PO_FLAG,'N') FROM control_pr_po V WHERE RM_TYPE = 'FABRIC' AND   V.BU = MM.BU and V.SO_NO_DOC = MM.SO_NO_DOC)  PO_FAB 
    ,(SELECT NVL(PR_FLAG,'N') FROM control_pr_po V WHERE RM_TYPE = 'SEW' AND   V.BU = MM.BU and V.SO_NO_DOC = MM.SO_NO_DOC)  PR_SEW 
   ,(SELECT NVL(PO_FLAG,'N') FROM control_pr_po V WHERE RM_TYPE = 'SEW' AND   V.BU = MM.BU and V.SO_NO_DOC = MM.SO_NO_DOC)  PO_SEW 
    ,(SELECT NVL(PR_FLAG,'N') FROM control_pr_po V WHERE RM_TYPE = 'PACK' AND   V.BU = MM.BU and V.SO_NO_DOC = MM.SO_NO_DOC)  PR_PACK 
   ,(SELECT NVL(PO_FLAG,'N') FROM control_pr_po V WHERE RM_TYPE = 'PACK' AND   V.BU = MM.BU and V.SO_NO_DOC = MM.SO_NO_DOC)  PO_PACK 
   , FIRST_EDD, MRD_WK, MRD_DATE, FIRST_EDD_DATE
FROM (
 SELECT SYSDATE AS RELOAD_DATE, 
                  BU,
                  VI,
                  MER_NAME,
                  GMT_TYPE,
                  HEAD_MER_NAME,
                  SUB_TEAM,
                  ORDER_QTY,
                  CUST_GROUP,
                  CUST_NAME,
                  BRAND_NAME,
                  SHIPMENT_DATE,
                  STYLE_REF,
                  SEA_CODE,
                  SO_YEAR,
                  SO_NO,
                  SO_NO_DOC,
                  EDD_W,
                  SO_RELEASE_DATE,
                  SO_RELEASE_WK,
                  CASE WHEN ORDER_TYPE = '02' THEN TO_NUMBER(GET_READINESS_FIND_TARGET(RDD_W,4))
                        ELSE TO_NUMBER(GET_READINESS_FIND_TARGET(RDD_W,6)) END AS SO_TARGET_WK,
                  SO_RELEASE_HIT,
                  SAMPLE_RELEASE_DATE,
                  SAMPLE_RELEASE_WK,
                    CASE WHEN ORDER_TYPE = '02' THEN TO_NUMBER(GET_READINESS_FIND_TARGET(RDD_W,4))
                        ELSE TO_NUMBER(GET_READINESS_FIND_TARGET(RDD_W,6)) END AS SAMPLE_TARGET_WK,
                  SAMPLE_RELEASE_HIT,
                  PATTERN_RELEASE_DATE,
                  PATTERN_RELEASE_WK,
                  PATTERN_TARGET_WK,
                  PATTERN_RELEASE_HIT,
                  FC_RELEASE_DATE,
                  FC_RELEASE_WK,
                  FC_TARGET_WK,
                  FC_RELEASE_HIT,
                  PACK_ACC_COMPLETE_DATE,
                  ACC_PACK_RELEASE_WK,
                  ACC_PACK_TARGET_WK,
                  ACC_PACK_RELEASE_HIT,
                  SEW_ACC_COMPLETE_DATE,
                  ACC_SEW_RELEASE_WK,
                  ACC_SEW_TARGET_WK,
                  ACC_SEW_RELEASE_HIT,
                  FABRIC_COMPLETE_DATE,
                  FAB_RELEASE_WK,
                  CASE WHEN ORDER_TYPE = '02' THEN TO_NUMBER(GET_READINESS_FIND_TARGET(MRD_WK,4))
                        ELSE TO_NUMBER(GET_READINESS_FIND_TARGET(MRD_WK,6)) END AS FAB_TARGET_WK,
                  FAB_RELEASE_HIT,
                  FIRST_CUT_DATE,
                  SCM_ALLOCATE,
                  FC_RELEASE_TEAM,
                  PATTERN_RELEASE_TEAM,
                  FAB_RELEASE_TEAM,
                  ACC_SEW_RELEASE_TEAM,
                  ACC_PACK_RELEASE_TEAM,
                  CREATE_DATE,
                  ORDER_TYPE,
                  ORDER_TYPE_DESC,
                  RDD_W,
                  FAB_VI,
                  FIRST_EDD,
                  MRD_WK,
                  MRD_DATE,
                  FIRST_EDD_DATE
                 FROM (
                 
                 SELECT BU, VI, MER_NAME, GMT_TYPE, HEAD_MER_NAME, SUB_TEAM, ORDER_QTY, CUST_GROUP, CUST_NAME, BRAND_NAME, SHIPMENT_DATE, STYLE_REF, SEA_CODE, SO_YEAR, SO_NO, SO_NO_DOC, EDD_W, SO_RELEASE_DATE, SO_RELEASE_WK, SO_TARGET_WK, SO_RELEASE_HIT, SAMPLE_RELEASE_DATE, SAMPLE_RELEASE_WK, SAMPLE_TARGET_WK, SAMPLE_RELEASE_HIT, PATTERN_RELEASE_DATE, PATTERN_RELEASE_WK, PATTERN_TARGET_WK, PATTERN_RELEASE_HIT, FC_RELEASE_DATE, FC_RELEASE_WK, FC_TARGET_WK, FC_RELEASE_HIT, PACK_ACC_COMPLETE_DATE, ACC_PACK_RELEASE_WK, ACC_PACK_TARGET_WK, ACC_PACK_RELEASE_HIT, SEW_ACC_COMPLETE_DATE, ACC_SEW_RELEASE_WK, ACC_SEW_TARGET_WK, ACC_SEW_RELEASE_HIT, FABRIC_COMPLETE_DATE, FAB_RELEASE_WK, FAB_TARGET_WK, FAB_RELEASE_HIT, FIRST_CUT_DATE, SCM_ALLOCATE, FC_RELEASE_TEAM, PATTERN_RELEASE_TEAM, FAB_RELEASE_TEAM, ACC_SEW_RELEASE_TEAM, ACC_PACK_RELEASE_TEAM, CREATE_DATE, ORDER_TYPE, ORDER_TYPE_DESC, RDD_W, FAB_VI, FIRST_EDD, FIRST_EDD MRD_WK, TRUNC(SYSDATE) MRD_DATE, TRUNC(SYSDATE) FIRST_EDD_DATE
FROM OE_SO_READINESS_RESULT_NYV
WHERE ORDER_QTY > 0 AND FIRST_EDD >= '202016'
                 ) M 
                 WHERE 1 = 1
                 AND M.ORDER_QTY > 0 
                 AND (FIRST_EDD >= '202016' OR MRD_WK >= '202016')) MM  """)

    conn.commit()
    conn.close()


  except:
    print('Error')

  printttime('CONTROL_WIP_READINESS_02')
  
  
# class CLS_OE_SO_READINESS_PO_V(threading.Thread):
#       def __init__(self):
#         threading.Thread.__init__(self)
#       def run(self):
#         OE_SO_READINESS_PO_V()
        
# def OE_SO_READINESS_PO_V():
#   my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
#   conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
#   cursor = conn.cursor()
#   cursor.execute("""DELETE FROM OE_SO_READINESS_PO_V_TMP2 WHERE 1 = 1 """)

#   conn.commit()

#   cursor.execute("""INSERT INTO OE_SO_READINESS_PO_V_TMP2 
#                  SELECT BU_CODE, SO_YEAR, SO_NO, SO_NO_DOC, PO_NO, PO_YEAR, PO_NO_DOC, SUPPLIER_NAME, DEL_DATE
#                  , CONFIRM_DATE, RM_TYPE, ITEM_NAME, ITEM_COLOR, ITEM_SIZE, PO_QTY, REC_QTY, PURCHASER_NAME, PO_STATUS, REC_STATUS, PO_VI, MOQ
#                  FROM OE_SO_READINESS_PO_V M 
#                  WHERE  rec_status <> 'CLOSDED' 
#                  AND PO_NO_DOC  NOT LIKE 'KL%' 
#                  AND SO_YEAR > 19 """)


#   conn.commit()

  
#   conn.close()
#   printttime('OE_SO_READINESS_PO_V')


# def OE_SO_READINESS_PO_PR_STATUS_V():
#   my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
#   conn = cx_Oracle.connect(user="nygm", password="nygm",
#                            dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
#   cursor = conn.cursor()
#   cursor.execute("""DELETE FROM CONTROL_PR_PO WHERE 1 = 1 """)

#   conn.commit()

#   cursor.execute("""INSERT INTO CONTROL_PR_PO
#                     SELECT * FROM OE_SO_READINESS_PO_PR_STATUS_V """)
# # FIRST_CUT_DATE IS NULL

#   conn.commit()

#   conn.close()
#   printttime('CONTROL_PR_PO')


# def OE_SO_READINESS_PO_V_00():
#   my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
#   conn = cx_Oracle.connect(user="nygm", password="nygm",
#                            dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
#   cursor = conn.cursor()
#   cursor.execute("""DELETE FROM OE_SO_READINESS_PO_V_TMP2 WHERE 1 = 1 """)

#   conn.commit()
  
#   conn.close()
#   printttime('OE_SO_READINESS_PO_V_00')


# class CLS_OE_SO_READINESS_PO_V_01(threading.Thread):
#   def __init__(self):
#     threading.Thread.__init__(self)

#   def run(self):
#     OE_SO_READINESS_PO_V_01()

# def OE_SO_READINESS_PO_V_01():
#   my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
#   conn = cx_Oracle.connect(user="nygm", password="nygm",
#                            dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
#   cursor = conn.cursor()

#   cursor.execute("""INSERT INTO OE_SO_READINESS_PO_V_TMP2 
#                   SELECT BU_CODE, SO_YEAR, SO_NO, SO_NO_DOC, PO_NO, PO_YEAR, PO_NO_DOC, SUPPLIER_NAME, 
#                   DEL_DATE, CONFIRM_DATE, RM_TYPE, ITEM_NAME, 
#                   ITEM_COLOR, ITEM_SIZE, PO_QTY, REC_QTY, PURCHASER_NAME, DECODE(PO_STATUS, 'A', 'APPROVED', '9', 'CLOSED', 'N', 'NORMAL') PO_STATUS,
#                   REC_STATUS, PO_VI, MOQ
#                   FROM OE_SO_READINESS_PO
#                   WHERE  REC_STATUS <> 'CLOSDED' 
#                   AND PO_NO_DOC  NOT LIKE 'KL%' 
#                   AND SO_YEAR > 19 """)


#   conn.commit()

#   conn.close()
#   printttime('OE_SO_READINESS_PO_V_01')


# class CLS_OE_SO_READINESS_PO_V_02(threading.Thread):
#   def __init__(self):
#     threading.Thread.__init__(self)
#   def run(self):
#     OE_SO_READINESS_PO_V_02()

# def OE_SO_READINESS_PO_V_02():
#   my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
#   conn = cx_Oracle.connect(user="nygm", password="nygm",
#                            dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
#   cursor = conn.cursor()

#   cursor.execute("""INSERT INTO OE_SO_READINESS_PO_V_TMP2 
#                   SELECT BU_CODE, SO_YEAR, SO_NO, SO_NO_DOC, PO_NO, PO_YEAR, PO_NO_DOC, SUPPLIER_NAME, 
#                   DEL_DATE, CONFIRM_DATE, RM_TYPE, ITEM_NAME, 
#                   ITEM_COLOR, ITEM_SIZE, PO_QTY, REC_QTY, PURCHASER_NAME, DECODE(PO_STATUS, 'A', 'APPROVED', '9', 'CLOSED', 'N', 'NORMAL') PO_STATUS,
#                   REC_STATUS, PO_VI, MOQ
#                   FROM TRM.OE_SO_READINESS_PO@TRM.WORLD
#                   WHERE  REC_STATUS <> 'CLOSDED' 
#                   AND PO_NO_DOC  NOT LIKE 'KL%' 
#                   AND SO_YEAR > 19 """)


#   conn.commit()

#   conn.close()
#   printttime('OE_SO_READINESS_PO_V_02')


# class CLS_OE_SO_READINESS_PO_V_03(threading.Thread):
#   def __init__(self):
#     threading.Thread.__init__(self)

#   def run(self):
#     OE_SO_READINESS_PO_V_03()

# def OE_SO_READINESS_PO_V_03():
#   my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
#   conn = cx_Oracle.connect(user="nygm", password="nygm",
#                            dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
#   cursor = conn.cursor()

#   cursor.execute("""INSERT INTO OE_SO_READINESS_PO_V_TMP2 
#                   SELECT BU_CODE, SO_YEAR, SO_NO, SO_NO_DOC, PO_NO, PO_YEAR, PO_NO_DOC, SUPPLIER_NAME, 
#                   DEL_DATE, CONFIRM_DATE, RM_TYPE, ITEM_NAME, 
#                   ITEM_COLOR, ITEM_SIZE, PO_QTY, REC_QTY, PURCHASER_NAME, DECODE(PO_STATUS, 'A', 'APPROVED', '9', 'CLOSED', 'N', 'NORMAL') PO_STATUS,
#                   REC_STATUS, PO_VI, MOQ
#                   FROM NYGM.OE_SO_READINESS_PO@NGWSP.WORLD
#                   WHERE  REC_STATUS <> 'CLOSDED' 
#                   AND PO_NO_DOC  NOT LIKE 'KL%' 
#                   AND SO_YEAR > 19 """)


#   conn.commit()

#   conn.close()
#   printttime('OE_SO_READINESS_PO_V_03')


# class CLS_OE_SO_READINESS_PO_V_04(threading.Thread):
#   def __init__(self):
#     threading.Thread.__init__(self)

#   def run(self):
#     OE_SO_READINESS_PO_V_04()

# def OE_SO_READINESS_PO_V_04():
#   my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
#   conn = cx_Oracle.connect(user="nygm", password="nygm",
#                            dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

#   try:
#     cursor = conn.cursor()

#     cursor.execute("""INSERT INTO OE_SO_READINESS_PO_V_TMP2 
#                   SELECT BU_CODE, SO_YEAR, SO_NO, SO_NO_DOC, PO_NO, PO_YEAR, PO_NO_DOC, SUPPLIER_NAME, 
#                   DEL_DATE, CONFIRM_DATE, RM_TYPE, ITEM_NAME, 
#                   ITEM_COLOR, ITEM_SIZE, PO_QTY, REC_QTY, PURCHASER_NAME, DECODE(PO_STATUS, 'A', 'APPROVED', '9', 'CLOSED', 'N', 'NORMAL') PO_STATUS,
#                   REC_STATUS, PO_VI, MOQ
#                   FROM VN.OE_SO_READINESS_PO@VNSQPROD.WORLD
#                   WHERE  REC_STATUS <> 'CLOSDED' 
#                   AND PO_NO_DOC  NOT LIKE 'KL%' 
#                   AND SO_YEAR > 19 """)


#     conn.commit()

#     conn.close()

#   except:
#     print('Error')

#   printttime('OE_SO_READINESS_PO_V_04')


# def OE_SO_READINESS_PO_PR_STATUS_V_01():
#   my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
#   conn = cx_Oracle.connect(user="nygm", password="nygm",
#                            dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
#   cursor = conn.cursor()
#   cursor.execute("""DELETE FROM CONTROL_PR_PO WHERE 1 = 1 """)

#   conn.commit()

#   cursor.execute("""INSERT INTO CONTROL_PR_PO
#                       SELECT "BU","SO_NO","SO_YEAR","SO_NO_DOC","RM_TYPE","PO_FLAG","PR_FLAG"
#                       FROM OE_SO_READINESS_PO_PR_STATUS
#                       UNION ALL
#                       SELECT "BU","SO_NO","SO_YEAR","SO_NO_DOC","RM_TYPE","PO_FLAG","PR_FLAG"
#                       FROM NYGM.OE_SO_READINESS_PO_PR_STATUS@NGWSP.WORLD
#                       UNION ALL
#                       SELECT "BU","SO_NO","SO_YEAR","SO_NO_DOC","RM_TYPE","PO_FLAG","PR_FLAG"
#                       FROM TRM.OE_SO_READINESS_PO_PR_STATUS@TRM.WORLD """)


#   conn.commit()

#   conn.close()

#   OE_SO_READINESS_PO_PR_STATUS_V_02()

#   printttime('OE_SO_READINESS_PO_PR_STATUS_V_01')


# def OE_SO_READINESS_PO_PR_STATUS_V_02():
#   my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
#   conn = cx_Oracle.connect(user="nygm", password="nygm",
#                            dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

#   try:
#     cursor = conn.cursor()

#     cursor.execute("""INSERT INTO CONTROL_PR_PO
#                       SELECT "BU","SO_NO","SO_YEAR","SO_NO_DOC","RM_TYPE","PO_FLAG","PR_FLAG"
#                       FROM VN.OE_SO_READINESS_PO_PR_STATUS@VNSQPROD.WORLD """)

#     conn.commit()

#     conn.close()


#   except:
#     print('Error')

#   printttime('OE_SO_READINESS_PO_PR_STATUS_V_02')

class CLS_UPDATE_ACTCUT_PPR(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      UPDATE_ACTCUT_PPR()


def UPDATE_ACTCUT_PPR():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  cursor2 = conn.cursor()
  cursor.execute("""SELECT BU, SO_NO, SO_YEAR, SUM(ORDER_QTY), MAX(LOADED_QTY)
                    FROM CONTROL_WIP_READINESS_ALL m
                    WHERE BU = 'NYG'
                    GROUP BY BU, SO_NO, SO_YEAR
                    HAVING MAX(LOADED_QTY) < SUM(ORDER_QTY)
                 """)

  # , CTR_GET_CUT_QTY(BU, SO_NO, SO_YEAR, NULL, NULL, NULL) ACT_CUT
  # , GET_SCM_LOAD_CUT_ALL_BU(BU, SO_NO, SO_YEAR) LOADED_QTY

  for row in cursor:
    bu = "NULL" if row[0] is None else "'{}'".format(row[0])
    so = "NULL" if row[1] is None else "{}".format(row[1])
    so_year = "NULL" if row[2] is None else "{}".format(row[2])
    # act_cut = "NULL" if row[3] is None else "{}".format(row[3])
    # loaded = "NULL" if row[4] is None else "{}".format(row[4])
    # print(row)

    sql = """
         UPDATE CONTROL_WIP_READINESS_ALL SET 
         ACT_CUT = CTR_GET_CUT_QTY(BU, SO_NO, SO_YEAR, NULL, NULL, NULL)  
        , LOADED_QTY = GET_SCM_LOAD_CUT_ALL_BU(BU, SO_NO, SO_YEAR)
         WHERE BU = {} 
         AND SO_NO = {}
         AND SO_YEAR = {}
     """.format(bu, so, so_year)
    print(sql)
    cursor2.execute(sql)
    conn.commit()

  conn.close()
  printttime('UPDATE_PPR')







printttime('Start 2 files')

threads = []

thread0 = CLS_IMPORT_READINESS_RESULT()
thread0.start()
threads.append(thread0)

# 2020-10-27






# ข้างล่าง ไม่่ใช้

# thread1 = CLS_CONTROL_WIP_READINESS()
# thread1.start()
# threads.append(thread1)

# thread3 = CLS_UPDATE_ACTCUT_PPR()
# thread3.start()
# threads.append(thread3)


for t in threads:
    t.join()



print (allTxt)

sendLine(allTxt,allHtml)
# 
#update 2020-08-12
