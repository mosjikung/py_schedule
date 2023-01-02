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
os.environ["ORACLE_HOME"]=oracle_client
os.environ["PATH"]=oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"]="AMERICAN_AMERICA.TH8TISASCII"

################### 
#Send Line
###################
def sendLine(txt):
    url = 'https://notify-api.line.me/api/notify'
    token = 'ZE6d4wFQO2qQiSMAMqecrPbj6R3nhj0y1STqOJ6xQ1s'
    headers = {'content-type':'application/x-www-form-urlencoded','Authorization':'Bearer ' + token}
    msg = txt
    requests.post(url,headers=headers,data = {'message':msg})
###################

################### 
#Write Log
###################
def WriteLog(txt):
    f = open("log\PHET_production_output.txt", "a")
    now = datetime.now() # current date and time
    date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
    f.write(date_time +" - "+ txt +"\n")
    f.close()
###################

###########################################
########
# Create 26/05/2022
# Request Move By Chonlada Suksamer
# SQL By 
# Create By Krisada.R
# Remark Move from PHET/production.py
########
class CLS_PROD_OUTPUT(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        PROD_OUTPUT()
        
def PROD_OUTPUT():
  my_dsn = cx_Oracle.makedsn("172.16.6.75",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYF", password="NYF", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_PROD_OUTPUT")
#   WriteLog("START CLS_PROD_OUTPUT")
  sql=""" SELECT DISTINCT to_char(A.EFF_DATE,'YYYY') YEAR,
           to_char(A.EFF_DATE,'IW') WEEK,
           A.EFF_DATE ,  
           TO_CHAR(A.ENTRY_DATE,'DD/MM/YYYY HH24:MI:SS') ENTRY_DATE,  
           A.KP_NO,
           A.SCHEDULE_NO,
           A.ITEM_CODE,
           A.YARN_LOT,
           (SELECT F.MACHINE_GROUP FROM FMIT_MACHINE F WHERE F.MACHINE_NO=A.MACHINE_NO) MACHINE_GROUP,
             A.MACHINE_NO,
           A.BARCODE_ID,
             A.FABRIC_ID,
             DECODE(SUBSTR(A.KP_NO,1,1),'C',A.SCH_ITEM_QTY,'K',A.FABRIC_WEIGHT) FABRIC_WEIGHT,
             A.QC_GRADE,
             (SELECT B.SL_TNAME  FROM HRIS_EMP_ACTIVE B WHERE B.EM_CODE= A.PROD_EMP ) MODULE,
             A.PROD_EMP KNIT_EMP_CODE,                     
             (SELECT B.EM_TNAME  FROM HRIS_EMP_ACTIVE B WHERE B.EM_CODE= A.PROD_EMP ) KNIT_EMP_TNAME,
             (SELECT B.EM_TSURNAME  FROM HRIS_EMP_ACTIVE B WHERE B.EM_CODE= A.PROD_EMP ) KNIT_EMP_TSURNAME,  
            ( SELECT C.EMP_QC FROM PROD_EMP_QC C WHERE C.FABRIC_ID=A.FABRIC_ID ) QC_EMP_CODE,                    
             (SELECT C.EM_TNAME  FROM PROD_EMP_QC C WHERE C.FABRIC_ID=A.FABRIC_ID ) QC_EMP_TNAME,        
             (SELECT C.EM_TSURNAME  FROM PROD_EMP_QC C WHERE C.FABRIC_ID=A.FABRIC_ID) QC_EMP_TSURNAME,   
             (SELECT D.EMP_WH  FROM PROD_EMP_WH D WHERE D.FABRIC_ID=A.FABRIC_ID ) WH_EMP_CODE,                   
             (SELECT D.EM_TNAME  FROM PROD_EMP_WH D WHERE D.FABRIC_ID=A.FABRIC_ID ) WH_EMP_TNAME,         
             (SELECT D.EM_TSURNAME  FROM PROD_EMP_WH D WHERE D.FABRIC_ID=A.FABRIC_ID ) WH_EMP_TSURNAME ,  
             DECODE(A.KP_PALLET_NO,NULL,'Wait','Transfer WH') STATUS_FABRIC,  
             ( SELECT TRUNC(E.ENTRY_DATE) FROM FMIT_KP_PALLET E WHERE A.KP_PALLET_RUNNING = E.RUNNING_PALLET_ID) TRANSFER_WHDATE,
             A.FABRIC_WEIGHT PRODUCTION_WEIGHT ,
           (SELECT MK.KG_PERDAY 
           FROM DFIT_KNIT_MC_ITEM MK ,FMIT_MACHINE MC,FMIT_KP_DETAIL KP
             WHERE MK.KNIT_MC_GROUP = MC.MACHINE_GROUP
             AND MK.KNIT_ITEM_CODE =A.ITEM_CODE
               AND MC.MACHINE_NO = A.MACHINE_NO
             AND MK.KNIT_ITEM_CODE IS NOT NULL
             AND KP.ITEM_CODE = MK.KNIT_ITEM_CODE
             AND KP.KP_NO = A.KP_NO  
             AND ROWNUM=1) CAP100,
           A.MODULE_NAME ,
          (SELECT MK.MACHINE_EFF_PRD--,MK.KG_PERDAY_EFF
           FROM DFIT_KNIT_MC_ITEM MK ,FMIT_MACHINE MC,FMIT_KP_DETAIL KP
             WHERE MK.KNIT_MC_GROUP = MC.MACHINE_GROUP
             AND MK.KNIT_ITEM_CODE =A.ITEM_CODE
               AND MC.MACHINE_NO = A.MACHINE_NO
             AND MK.KNIT_ITEM_CODE IS NOT NULL
             AND KP.ITEM_CODE = MK.KNIT_ITEM_CODE
             AND KP.KP_NO = A.KP_NO  
             AND ROWNUM=1) PPERCENT,
           (SELECT MK.KG_PERDAY_EFF
           FROM DFIT_KNIT_MC_ITEM MK ,FMIT_MACHINE MC,FMIT_KP_DETAIL KP
             WHERE MK.KNIT_MC_GROUP = MC.MACHINE_GROUP
             AND MK.KNIT_ITEM_CODE =A.ITEM_CODE
               AND MC.MACHINE_NO = A.MACHINE_NO
             AND MK.KNIT_ITEM_CODE IS NOT NULL
             AND KP.ITEM_CODE = MK.KNIT_ITEM_CODE
             AND KP.KP_NO = A.KP_NO  
             AND ROWNUM=1) KG_PERDAY_EFF   
     FROM FMIT_KP_FABRIC A
     WHERE EFF_DATE >= TO_DATE('01/01/2021','DD/MM/RRRR') + 0.000000 

    """
  # sql="""    SELECT DISTINCT to_char(A.EFF_DATE,'YYYY') YEAR,
  #          to_char(A.EFF_DATE,'IW') WEEK,
  #          A.EFF_DATE ,  
	#          TO_CHAR(A.ENTRY_DATE,'DD/MM/YYYY HH24:MI:SS') ENTRY_DATE,  
  #          A.KP_NO,
  #          A.SCHEDULE_NO,
  #          A.ITEM_CODE,
	#          A.YARN_LOT,
  #          F.MACHINE_GROUP,
	#          A.MACHINE_NO,
  #          A.BARCODE_ID,
	#          A.FABRIC_ID,
	#          DECODE(SUBSTR(A.KP_NO,1,1),'C',A.SCH_ITEM_QTY,'K',A.FABRIC_WEIGHT) FABRIC_WEIGHT,
	#          A.QC_GRADE,
	#          B.SL_TNAME MODULE,
	#          A.PROD_EMP KNIT_EMP_CODE,                
	#          B.EM_TNAME KNIT_EMP_TNAME,               
	#          B.EM_TSURNAME KNIT_EMP_TSURNAME,          
	#          C.EMP_QC QC_EMP_CODE,                    
	#          C.EM_TNAME QC_EMP_TNAME,        
  #          C.EM_TSURNAME QC_EMP_TSURNAME,   
  #     	   D.EMP_WH WH_EMP_CODE,                   
	#          D.EM_TNAME WH_EMP_TNAME,         
	#          D.EM_TSURNAME WH_EMP_TSURNAME ,  
	#          DECODE(A.KP_PALLET_NO,NULL,'Wait','Transfer WH') STATUS_FABRIC,  
  #          TRUNC(E.ENTRY_DATE)TRANSFER_WHDATE,
  #          A.FABRIC_WEIGHT PRODUCTION_WEIGHT,
	#          (SELECT MK.KG_PERDAY 
  #          FROM DFIT_KNIT_MC_ITEM MK ,FMIT_MACHINE MC,FMIT_KP_DETAIL KP
	#          WHERE MK.KNIT_MC_GROUP = MC.MACHINE_GROUP
  #            AND MK.KNIT_ITEM_CODE =A.ITEM_CODE
	#            AND MC.MACHINE_NO = A.MACHINE_NO
  #            AND MK.KNIT_ITEM_CODE IS NOT NULL
  #            AND KP.ITEM_CODE = MK.KNIT_ITEM_CODE
  #            AND KP.KP_NO = A.KP_NO  
  #            AND ROWNUM=1) CAP100,
  #          A.MODULE_NAME,
  #          (SELECT MK.MACHINE_EFF_PRD--,MK.KG_PERDAY_EFF
  #          FROM DFIT_KNIT_MC_ITEM MK ,FMIT_MACHINE MC,FMIT_KP_DETAIL KP
	#          WHERE MK.KNIT_MC_GROUP = MC.MACHINE_GROUP
  #            AND MK.KNIT_ITEM_CODE =A.ITEM_CODE
	#            AND MC.MACHINE_NO = A.MACHINE_NO
  #            AND MK.KNIT_ITEM_CODE IS NOT NULL
  #            AND KP.ITEM_CODE = MK.KNIT_ITEM_CODE
  #            AND KP.KP_NO = A.KP_NO  
  #            AND ROWNUM=1) PPERCENT,
  #          (SELECT MK.KG_PERDAY_EFF
  #          FROM DFIT_KNIT_MC_ITEM MK ,FMIT_MACHINE MC,FMIT_KP_DETAIL KP
	#          WHERE MK.KNIT_MC_GROUP = MC.MACHINE_GROUP
  #            AND MK.KNIT_ITEM_CODE =A.ITEM_CODE
	#            AND MC.MACHINE_NO = A.MACHINE_NO
  #            AND MK.KNIT_ITEM_CODE IS NOT NULL
  #            AND KP.ITEM_CODE = MK.KNIT_ITEM_CODE
  #            AND KP.KP_NO = A.KP_NO  
  #            AND ROWNUM=1) KG_PERDAY_EFF
  #    FROM FMIT_KP_FABRIC A,
  #         HRIS_EMP_ACTIVE B,
  #         PROD_EMP_QC C,
  #         PROD_EMP_WH D,
  #         FMIT_KP_PALLET E,
  #         FMIT_MACHINE F
  #    WHERE A.PROD_EMP = B.EM_CODE(+) 	
  #      AND A.FABRIC_ID = C.FABRIC_ID
  #      AND A.FABRIC_ID = D.FABRIC_ID	 
  #      AND A.KP_PALLET_NO = E.PALLET_ID(+)
  #      AND A.KP_PALLET_RUNNING = E.RUNNING_PALLET_ID(+)
  #      AND A.MACHINE_NO = F.MACHINE_NO(+)
  #      AND TRUNC(A.EFF_DATE) >= TO_date('01/01/2020','DD/MM/YYYY')
       
  #     """

  df = pd.read_sql_query(sql, conn)
  # _filename = r"C:\QVD_DATA\PRO_NYK\PROD_OUTPUT_2021.xlsx"0
  # df.to_excel(_filename, index=False)
  _filename = r"C:\QVD_DATA\PRO_NYK\PROD_OUTPUT_2021.csv"
  df.to_csv(_filename, index=False, encoding='utf-8-sig')

  conn.close()
  print ("COMPLETE PROD_OUTPUT_2021.csv")
  sendLine("COMPLETE PROD_OUTPUT_2021.csv")
#   WriteLog("COMPLETE PROD_OUTPUT_2021.csv")

#############################################


threads = []

thread1 = CLS_PROD_OUTPUT();thread1.start();threads.append(thread1) 


for t in threads:
    t.join()
print ("Run production_output.py COMPLETE")