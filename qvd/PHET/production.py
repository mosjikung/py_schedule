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


def sendLine(txt):
  url = 'https://notify-api.line.me/api/notify'
  token = 'ZE6d4wFQO2qQiSMAMqecrPbj6R3nhj0y1STqOJ6xQ1s'
  headers = {'content-type':'application/x-www-form-urlencoded','Authorization':'Bearer ' + token}
  msg = txt
  requests.post(url,headers=headers,data = {'message':msg})

###########################################
# class CLS_PROD_OUTPUT(threading.Thread):
#       def __init__(self):
#         threading.Thread.__init__(self)
#       def run(self):
#         PROD_OUTPUT()
        
# def PROD_OUTPUT():
#   my_dsn = cx_Oracle.makedsn("172.16.6.75",port=1521,sid="NYTG")
#   conn = cx_Oracle.connect(user="NYF", password="NYF", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
#   cursor = conn.cursor()
#   sendLine("START CLS_PROD_OUTPUT")
#   sql=""" SELECT DISTINCT to_char(A.EFF_DATE,'YYYY') YEAR,
#            to_char(A.EFF_DATE,'IW') WEEK,
#            A.EFF_DATE ,  
#            TO_CHAR(A.ENTRY_DATE,'DD/MM/YYYY HH24:MI:SS') ENTRY_DATE,  
#            A.KP_NO,
#            A.SCHEDULE_NO,
#            A.ITEM_CODE,
#            A.YARN_LOT,
#            (SELECT F.MACHINE_GROUP FROM FMIT_MACHINE F WHERE F.MACHINE_NO=A.MACHINE_NO) MACHINE_GROUP,
#              A.MACHINE_NO,
#            A.BARCODE_ID,
#              A.FABRIC_ID,
#              DECODE(SUBSTR(A.KP_NO,1,1),'C',A.SCH_ITEM_QTY,'K',A.FABRIC_WEIGHT) FABRIC_WEIGHT,
#              A.QC_GRADE,
#              (SELECT B.SL_TNAME  FROM HRIS_EMP_ACTIVE B WHERE B.EM_CODE= A.PROD_EMP ) MODULE,
#              A.PROD_EMP KNIT_EMP_CODE,                     
#              (SELECT B.EM_TNAME  FROM HRIS_EMP_ACTIVE B WHERE B.EM_CODE= A.PROD_EMP ) KNIT_EMP_TNAME,
#              (SELECT B.EM_TSURNAME  FROM HRIS_EMP_ACTIVE B WHERE B.EM_CODE= A.PROD_EMP ) KNIT_EMP_TSURNAME,  
#             ( SELECT C.EMP_QC FROM PROD_EMP_QC C WHERE C.FABRIC_ID=A.FABRIC_ID ) QC_EMP_CODE,                    
#              (SELECT C.EM_TNAME  FROM PROD_EMP_QC C WHERE C.FABRIC_ID=A.FABRIC_ID ) QC_EMP_TNAME,        
#              (SELECT C.EM_TSURNAME  FROM PROD_EMP_QC C WHERE C.FABRIC_ID=A.FABRIC_ID) QC_EMP_TSURNAME,   
#              (SELECT D.EMP_WH  FROM PROD_EMP_WH D WHERE D.FABRIC_ID=A.FABRIC_ID ) WH_EMP_CODE,                   
#              (SELECT D.EM_TNAME  FROM PROD_EMP_WH D WHERE D.FABRIC_ID=A.FABRIC_ID ) WH_EMP_TNAME,         
#              (SELECT D.EM_TSURNAME  FROM PROD_EMP_WH D WHERE D.FABRIC_ID=A.FABRIC_ID ) WH_EMP_TSURNAME ,  
#              DECODE(A.KP_PALLET_NO,NULL,'Wait','Transfer WH') STATUS_FABRIC,  
#              ( SELECT TRUNC(E.ENTRY_DATE) FROM FMIT_KP_PALLET E WHERE A.KP_PALLET_RUNNING = E.RUNNING_PALLET_ID) TRANSFER_WHDATE,
#              A.FABRIC_WEIGHT PRODUCTION_WEIGHT ,
#            (SELECT MK.KG_PERDAY 
#            FROM DFIT_KNIT_MC_ITEM MK ,FMIT_MACHINE MC,FMIT_KP_DETAIL KP
#              WHERE MK.KNIT_MC_GROUP = MC.MACHINE_GROUP
#              AND MK.KNIT_ITEM_CODE =A.ITEM_CODE
#                AND MC.MACHINE_NO = A.MACHINE_NO
#              AND MK.KNIT_ITEM_CODE IS NOT NULL
#              AND KP.ITEM_CODE = MK.KNIT_ITEM_CODE
#              AND KP.KP_NO = A.KP_NO  
#              AND ROWNUM=1) CAP100,
#            A.MODULE_NAME ,
#           (SELECT MK.MACHINE_EFF_PRD--,MK.KG_PERDAY_EFF
#            FROM DFIT_KNIT_MC_ITEM MK ,FMIT_MACHINE MC,FMIT_KP_DETAIL KP
#              WHERE MK.KNIT_MC_GROUP = MC.MACHINE_GROUP
#              AND MK.KNIT_ITEM_CODE =A.ITEM_CODE
#                AND MC.MACHINE_NO = A.MACHINE_NO
#              AND MK.KNIT_ITEM_CODE IS NOT NULL
#              AND KP.ITEM_CODE = MK.KNIT_ITEM_CODE
#              AND KP.KP_NO = A.KP_NO  
#              AND ROWNUM=1) PPERCENT,
#            (SELECT MK.KG_PERDAY_EFF
#            FROM DFIT_KNIT_MC_ITEM MK ,FMIT_MACHINE MC,FMIT_KP_DETAIL KP
#              WHERE MK.KNIT_MC_GROUP = MC.MACHINE_GROUP
#              AND MK.KNIT_ITEM_CODE =A.ITEM_CODE
#                AND MC.MACHINE_NO = A.MACHINE_NO
#              AND MK.KNIT_ITEM_CODE IS NOT NULL
#              AND KP.ITEM_CODE = MK.KNIT_ITEM_CODE
#              AND KP.KP_NO = A.KP_NO  
#              AND ROWNUM=1) KG_PERDAY_EFF   
#      FROM FMIT_KP_FABRIC A
#      WHERE EFF_DATE >= TO_DATE('01/01/2021','DD/MM/RRRR') + 0.000000 

#     """
#   # sql="""    SELECT DISTINCT to_char(A.EFF_DATE,'YYYY') YEAR,
#   #          to_char(A.EFF_DATE,'IW') WEEK,
#   #          A.EFF_DATE ,  
# 	#          TO_CHAR(A.ENTRY_DATE,'DD/MM/YYYY HH24:MI:SS') ENTRY_DATE,  
#   #          A.KP_NO,
#   #          A.SCHEDULE_NO,
#   #          A.ITEM_CODE,
# 	#          A.YARN_LOT,
#   #          F.MACHINE_GROUP,
# 	#          A.MACHINE_NO,
#   #          A.BARCODE_ID,
# 	#          A.FABRIC_ID,
# 	#          DECODE(SUBSTR(A.KP_NO,1,1),'C',A.SCH_ITEM_QTY,'K',A.FABRIC_WEIGHT) FABRIC_WEIGHT,
# 	#          A.QC_GRADE,
# 	#          B.SL_TNAME MODULE,
# 	#          A.PROD_EMP KNIT_EMP_CODE,                
# 	#          B.EM_TNAME KNIT_EMP_TNAME,               
# 	#          B.EM_TSURNAME KNIT_EMP_TSURNAME,          
# 	#          C.EMP_QC QC_EMP_CODE,                    
# 	#          C.EM_TNAME QC_EMP_TNAME,        
#   #          C.EM_TSURNAME QC_EMP_TSURNAME,   
#   #     	   D.EMP_WH WH_EMP_CODE,                   
# 	#          D.EM_TNAME WH_EMP_TNAME,         
# 	#          D.EM_TSURNAME WH_EMP_TSURNAME ,  
# 	#          DECODE(A.KP_PALLET_NO,NULL,'Wait','Transfer WH') STATUS_FABRIC,  
#   #          TRUNC(E.ENTRY_DATE)TRANSFER_WHDATE,
#   #          A.FABRIC_WEIGHT PRODUCTION_WEIGHT,
# 	#          (SELECT MK.KG_PERDAY 
#   #          FROM DFIT_KNIT_MC_ITEM MK ,FMIT_MACHINE MC,FMIT_KP_DETAIL KP
# 	#          WHERE MK.KNIT_MC_GROUP = MC.MACHINE_GROUP
#   #            AND MK.KNIT_ITEM_CODE =A.ITEM_CODE
# 	#            AND MC.MACHINE_NO = A.MACHINE_NO
#   #            AND MK.KNIT_ITEM_CODE IS NOT NULL
#   #            AND KP.ITEM_CODE = MK.KNIT_ITEM_CODE
#   #            AND KP.KP_NO = A.KP_NO  
#   #            AND ROWNUM=1) CAP100,
#   #          A.MODULE_NAME,
#   #          (SELECT MK.MACHINE_EFF_PRD--,MK.KG_PERDAY_EFF
#   #          FROM DFIT_KNIT_MC_ITEM MK ,FMIT_MACHINE MC,FMIT_KP_DETAIL KP
# 	#          WHERE MK.KNIT_MC_GROUP = MC.MACHINE_GROUP
#   #            AND MK.KNIT_ITEM_CODE =A.ITEM_CODE
# 	#            AND MC.MACHINE_NO = A.MACHINE_NO
#   #            AND MK.KNIT_ITEM_CODE IS NOT NULL
#   #            AND KP.ITEM_CODE = MK.KNIT_ITEM_CODE
#   #            AND KP.KP_NO = A.KP_NO  
#   #            AND ROWNUM=1) PPERCENT,
#   #          (SELECT MK.KG_PERDAY_EFF
#   #          FROM DFIT_KNIT_MC_ITEM MK ,FMIT_MACHINE MC,FMIT_KP_DETAIL KP
# 	#          WHERE MK.KNIT_MC_GROUP = MC.MACHINE_GROUP
#   #            AND MK.KNIT_ITEM_CODE =A.ITEM_CODE
# 	#            AND MC.MACHINE_NO = A.MACHINE_NO
#   #            AND MK.KNIT_ITEM_CODE IS NOT NULL
#   #            AND KP.ITEM_CODE = MK.KNIT_ITEM_CODE
#   #            AND KP.KP_NO = A.KP_NO  
#   #            AND ROWNUM=1) KG_PERDAY_EFF
#   #    FROM FMIT_KP_FABRIC A,
#   #         HRIS_EMP_ACTIVE B,
#   #         PROD_EMP_QC C,
#   #         PROD_EMP_WH D,
#   #         FMIT_KP_PALLET E,
#   #         FMIT_MACHINE F
#   #    WHERE A.PROD_EMP = B.EM_CODE(+) 	
#   #      AND A.FABRIC_ID = C.FABRIC_ID
#   #      AND A.FABRIC_ID = D.FABRIC_ID	 
#   #      AND A.KP_PALLET_NO = E.PALLET_ID(+)
#   #      AND A.KP_PALLET_RUNNING = E.RUNNING_PALLET_ID(+)
#   #      AND A.MACHINE_NO = F.MACHINE_NO(+)
#   #      AND TRUNC(A.EFF_DATE) >= TO_date('01/01/2020','DD/MM/YYYY')
       
#   #     """

#   df = pd.read_sql_query(sql, conn)
#   # _filename = r"C:\QVD_DATA\PRO_NYK\PROD_OUTPUT_2021.xlsx"
#   # df.to_excel(_filename, index=False)
#   _filename = r"C:\QVD_DATA\PRO_NYK\PROD_OUTPUT_2021.csv"
#   df.to_csv(_filename, index=False, encoding='utf-8-sig')

#   conn.close()
#   print ("COMPLETE PROD_OUTPUT_2021.csv")
#   sendLine("COMPLETE PROD_OUTPUT_2021.csv")

#############################################

###########################################
class CLS_PROD_PLANING(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        PROD_PLANING()
        
def PROD_PLANING():
  my_dsn = cx_Oracle.makedsn("172.16.6.75",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYF", password="NYF", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_PROD_PLANING")
  sql=""" select AA.*,
               BUYYER END_BUYER,
               CUSTOMER_YEAR FG_YEAR,
               CUSTOMER_FG FG_WEEk,
              (SELECT min(sk.KNIT_MC_WW)  FROM DFIT_KNIT_SOKP sk
                   WHERE sk.KNIT_MC_CAT NOT IN ( 'COMKN','COMCL','GFSTOCK','NO-FAB','COM')
                   AND sk.KNIT_MC_SO = aa.SO_NO) KNIT_MC_WW,
          ( select count(FF.kp_no) roll 
            from FMIT_KP_FABRIC FF
            where FF.kp_no = AA.KP_NO
              and FF.schedule_no = AA.schedule_no
              and FF.MACHINE_NO = AA.MACHINE_NO
              and FF.fabric_id not in 
                                       (select mm.fabric_id  from FMIT_GF_WAREHOUSE MM
                                        where mm.gf_locked = 'N' 
                                        and mm.fabric_id in (select nn.FABRIC_ID from FMIT_KP_FABRIC nn where nn.kp_no = AA.KP_NO 
                                        and nn.schedule_no = AA.schedule_no and nn.MACHINE_NO = AA.MACHINE_NO) 
              )) FABRIC_WAIT_ROLL,
            (select nvl(sum(fabric_weight),0) fabric_weight
            from FMIT_KP_FABRIC FF
            where FF.kp_no = AA.KP_NO
              and FF.schedule_no = AA.schedule_no
              and FF.MACHINE_NO = AA.MACHINE_NO
              and FF.fabric_id not in 
                                       (select mm.fabric_id  from FMIT_GF_WAREHOUSE MM
                                        where mm.gf_locked = 'N' 
                                        and mm.fabric_id in (select nn.FABRIC_ID from FMIT_KP_FABRIC nn where nn.kp_no = AA.KP_NO 
                                        and nn.schedule_no = AA.schedule_no and nn.MACHINE_NO = AA.MACHINE_NO) 
              )) FABRIC_WAIT_WEIGHT,
         (select count(*) from FMIT_GF_WAREHOUSE BB where bb.gf_locked = 'N' 
          and bb.fabric_id in (select cc.FABRIC_ID from FMIT_KP_FABRIC cc where cc.kp_no = aa.kp_no and cc.schedule_no = aa.schedule_no and cc.MACHINE_NO = aa.MACHINE_NO)
          and bb.QC_GRADE = 'X') FABRIC_GRADE_X ,
          GET_KNIT_FG(AA.KP_SO, AA.WEEK) KNIT_FG_WEEK
        from(
        SELECT DISTINCT A.KNIT_MC_CAT CAT,
          A.KNIT_MC_GROUP MC_GROUP,
	        A.KNIT_MC_GUAGE GUAGE,
	        A.KNIT_MC_YEAR YEAR,
	        A.KNIT_MC_WW WEEK,
	        A.KP_NO,          
	        A.KNIT_ITEM_CODE ITEM_CODE,
          A.KNIT_ITEM_DESC DESCRIPTION,
  	    	C.KG_PERDAY CAP100,
	        SUM(A.KNIT_MC_KG) KP_WEIGHT,
	        A.KNITTED_TYPE TYPE,			
	        B.SCHEDULE_NO,
	        B.MACHINE_NO,
	        B.YARN_LOT,
	        B.SCHEDULE_ROLL,
	        B.SCHEDULE_WEIGHT,
	        B.CURRENT_ROLL,
	        DECODE(SUBSTR(B.KP_NO,1,1),'K',B.CURRENT_WEIGHT,'C',B.SCH_ITEM_QTY) CURRENT_WEIGHT,
	       	B.LAST_UPDATE_DATE EFF_DATE,
          TO_CHAR(LAST_WEIGHTING_TIME,'DD/MM/YYYY HH24:MI:SS')LAST_WEIGHTING_TIME,
	        B.PIN,
	        B.JMP,
	        B.SL,
	        B.EMP_SETUP SETUP_NEME,
	        B.EMP_APPROVED APPROVED_NAME,
          TO_CHAR(B.TARGET_START,'DD/MM/YYYY HH24:MI:SS')SCHEDULE_START,
          TO_CHAR(B.TARGET_END,'DD/MM/YYYY HH24:MI:SS')SCHEDULE_END,
          MIN(TO_CHAR(D.ENTRY_DATE,'DD/MM/YYYY HH24:MI:SS'))FIRST_ROLL,
          MAX(TO_CHAR(D.ENTRY_DATE,'DD/MM/YYYY HH24:MI:SS'))END_ROLL,
	       	DECODE(B.RM_ROLL,'1','Knitting Schedule','2','SetUp Machine')SCHEDULE_TYPE,
          TO_CHAR(B.ENG_ADJUST_START,'DD/MM/YYYY HH24:MI:SS')SETUP_START,      
          TO_CHAR(B.ENG_ADJUST_END,'DD/MM/YYYY HH24:MI:SS')SETUP_END,
          B.FANI_ROLL SETUP_TIME,
          B.MODULE_NAME,
	      	C.MACHINE_EFF_PRD PERCENT,
		      C.KG_PERDAY_EFF CAP_PERCENT ,
		      TO_CHAR(B.QC_APPROVED_DATE,'DD/MM/YYYY HH24:MI:SS')QC_APPROVED_FIRST_YARD,
	      	TO_CHAR(B.WORK_TRANS_DATE,'DD/MM/YYYY HH24:MI:SS')QC_APPROVED_FIRST_ROLL,
          (SELECT distinct s.SO_NO FROM FMIT_KP_PO_DETAIL_SF5 s WHERE s.KP_NO = A.KP_NO) SO_NO,
          GET_YARN_USED(A.KNIT_ITEM_CODE,A.KP_NO) YARN_USERD,
          (SELECT i.ITEM_STRUCTURE FROM FMIT_ITEM i WHERE i.ITEM_CODE = A.KNIT_ITEM_CODE) STRUCTURE,
          GET_BASIC_PREMIUM (A.KNIT_ITEM_CODE,A.KP_NO) BASIC_PREMIUM ,
          (SELECT DISTINCT KNIT_MC_SO FROM DFIT_KNIT_SOKP KP WHERE KP.KP_NO=A.KP_NO AND ROWNUM=1) KP_SO
       FROM DFIT_KNIT_SOKP_V A,
            FMIT_KP_SCHEDULE B,
            DFIT_KNIT_MC_ITEM C,
            FMIT_KP_FABRIC D,
            FMIT_MACHINE E
       WHERE A.KP_NO = B.KP_NO(+)
	     AND B.MACHINE_NO = E.MACHINE_NO
	     AND E.MACHINE_GROUP = C.KNIT_MC_GROUP	
	     AND A.KNIT_ITEM_CODE = C.KNIT_ITEM_CODE(+)
	     AND B.SCHEDULE_NO = D.SCHEDULE_NO(+)
       AND B.KP_NO IS NOT NULL
       AND A.KP_NO IS NOT NULL
       AND A.KNIT_MC_GROUP NOT IN ('NO-FAB','STOCK')
       AND NVL(A.KNIT_MC_YEAR,9999) >= 2019
        GROUP BY A.KNIT_MC_CAT,
          A.KNIT_MC_GROUP,
	        A.KNIT_MC_GUAGE,
	        A.KNIT_MC_YEAR,
	        A.KNIT_MC_WW,
	        A.KNIT_ITEM_CODE,
		      C.KG_PERDAY,
	        A.KNIT_ITEM_DESC,
	        A.KP_NO,
	        A.KNITTED_TYPE,			
	        B.SCHEDULE_NO,
	        B.MACHINE_NO,
	        B.YARN_LOT,
	        B.SCHEDULE_ROLL,
	        B.SCHEDULE_WEIGHT,
	        B.CURRENT_ROLL,
	        DECODE(SUBSTR(B.KP_NO,1,1),'K',B.CURRENT_WEIGHT,'C',B.SCH_ITEM_QTY),
	      	B.LAST_UPDATE_DATE,
          TO_CHAR(LAST_WEIGHTING_TIME,'DD/MM/YYYY HH24:MI:SS'),
	        B.PIN,
	        B.JMP,
	        B.SL,
	        B.EMP_SETUP,
	        B.EMP_APPROVED,
          TO_CHAR(B.TARGET_START,'DD/MM/YYYY HH24:MI:SS'),
          TO_CHAR(B.TARGET_END,'DD/MM/YYYY HH24:MI:SS'),
	       	DECODE(B.RM_ROLL,'1','Knitting Schedule','2','SetUp Machine'),
          TO_CHAR(B.ENG_ADJUST_START,'DD/MM/YYYY HH24:MI:SS'),      
          TO_CHAR(B.ENG_ADJUST_END,'DD/MM/YYYY HH24:MI:SS'),
          B.FANI_ROLL,
          B.MODULE_NAME,
	      	C.MACHINE_EFF_PRD,
	      	C.KG_PERDAY_EFF,
	      	TO_CHAR(B.QC_APPROVED_DATE,'DD/MM/YYYY HH24:MI:SS'),
          TO_CHAR(B.WORK_TRANS_DATE,'DD/MM/YYYY HH24:MI:SS')    
        ) AA,
        SMIT_SO_HEADER SS
        where AA.SO_NO = SS.SO_NO(+)
        """
  
  _filename = r"C:\QVD_DATA\PRO_NYK\PROD_PLANING.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print ("COMPLETE PROD_PLANING.xlsx")
  sendLine("COMPLETE PROD_PLANING.xlsx")


###########################################
########
# Create 18/08/2022
# Request Move By Chonlada Suksamer
# SQL By 
# Create By Krisada.R
# Remark cut data KP 2019 - 2021
########
class CLS_DATA_KP_2021(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_KP_2021()


def DATA_KP_2021():
  my_dsn = cx_Oracle.makedsn("172.16.6.75", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYF", password="NYF", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  print ("START CLS_DATA_KP_2021")
  sendLine("START CLS_DATA_KP_2021")
  sql =""" SELECT CAT,ITEM_CATEGORY, MC_GROUP, GUAGE, YEAR,
	         WEEK, KP_NO, ITEM_CODE, DESCRIPTION, KP_WEIGHT, COLOR,
	         TYPE, SO_NO, YARN_USED, ITEM_STRUCTURE, BASIC_Premium,
           END_BUYYER, FG_YEAR, FG_WEEK, Customer, SALE_NAME,
           TEAM_NAME, REMARK, Round_Minute, Revolution_Roll,
           Revolution_Weight, SO_Type, Key_In_DATE, Last_Adj_DATE,
           case when SUBSTR(SO_NO,4,2) in ('16','18','91','13','28') then 'Salesman' else
           case when SUBSTR(SO_NO,4,2) not in ('16','18','91','13','28') then 'Normal' else SO_TYPED end end SO_TYPED,
           rs_number, Yarn_Lot, New_Item,New_Lot,
           ORDER_WEIGHT, SCHEDULE_WEIGHT, NVL(CUR,0)-NVL(DEL,0) Knit_Weight,
           NVL(SCHEDULE_WEIGHT,0)-(NVL(CUR,0)-NVL(DEL,0)) OutStanding_weight,EDD_YEAR,EDD_WEEK
          FROM(
          SELECT DISTINCT A.KNIT_MC_CAT CAT,
               (SELECT max(i.ITEM_CATEGORY) FROM FMIT_ITEM I WHERE i.ITEM_CODE = A.KNIT_ITEM_CODE) ITEM_CATEGORY,
                A.KNIT_MC_GROUP MC_GROUP,
	              A.KNIT_MC_GUAGE GUAGE,
	              A.KNIT_MC_YEAR YEAR,
	              A.KNIT_MC_WW WEEK,
	              A.KP_NO,                
	              A.KNIT_ITEM_CODE ITEM_CODE,
	              A.KNIT_ITEM_DESC DESCRIPTION,
	              SUM(A.KNIT_MC_KG) KP_WEIGHT,
	              A.KNITTED_COLOR COLOR,
	              A.KNITTED_TYPE TYPE,
                A.KNIT_MC_SO SO_NO,
                GET_YARN_USED(A.KNIT_ITEM_CODE,A.KP_NO) YARN_USED,
                (SELECT max(i.ITEM_STRUCTURE) FROM FMIT_ITEM I WHERE i.ITEM_CODE = A.KNIT_ITEM_CODE) ITEM_STRUCTURE,
                GET_BASIC_PREMIUM(A.KNIT_ITEM_CODE,A.KP_NO) BASIC_Premium,
                (SELECT MAX(S.BUYYER) FROM SMIT_SO_HEADER S WHERE S.SO_NO = A.KNIT_MC_SO) END_BUYYER,
                (SELECT MAX(S.CUSTOMER_YEAR) FROM SMIT_SO_HEADER S WHERE S.SO_NO = A.KNIT_MC_SO) FG_YEAR,
                (SELECT MAX(S.CUSTOMER_FG) FROM SMIT_SO_HEADER S WHERE S.SO_NO = A.KNIT_MC_SO) FG_WEEK,
                A.KNIT_CUS_NAME Customer,
                B.SALE_NAME SALE_NAME,
                B.TEAM_NAME TEAM_NAME,
                REPLACE(REPLACE(REPLACE(REPLACE(A.KNITTED_REMARK,CHR(10)||CHR(13),' '), CHR(9), ' '), CHR(10), ' '), CHR(13), ' ') REMARK,
                (SELECT max(i.MC_RPM) FROM FMIT_ITEM I WHERE i.ITEM_CODE = A.KNIT_ITEM_CODE) Round_Minute,
                (SELECT max(i.MC_ROTATE) FROM FMIT_ITEM I WHERE i.ITEM_CODE = A.KNIT_ITEM_CODE) Revolution_Roll,
                (SELECT max(i.PRODUCTION_WEIGHT) FROM FMIT_ITEM I WHERE i.ITEM_CODE = A.KNIT_ITEM_CODE) Revolution_Weight,
                case when B.TEAM_NAME like '%MS%' THEN 'RUNNING' else ( SELECT MAX(ss.SO_TYPE) FROM SF5.DUMMY_SO_HEADERS@SALESF5.WORLD ss 
                                                                        WHERE ss.ORA_ORDER_NUMBER = A.KNIT_MC_SO) end  SO_Type,
                (SELECT max(to_date(entry_date,'dd/mm/yyyy')) FROM SF5.DUMMY_RUCONFIRM_STOCK@SALESF5.WORLD
                  WHERE Order_number = A.rs_number  and CONF_RU_TYPE ='CONFIRM_WEEK') Key_In_DATE ,                                                                    
                A.ENTRY_DATE Last_Adj_DATE,
                (SELECT max(FOB_POINT_CODE) FROM SF5.DUMMY_SO_HEADERS@SALESF5.WORLD WHERE Order_number = A.rs_number) SO_TYPED ,
                A.rs_number,
                GET_LOT_KP(A.KP_NO) Yarn_Lot,
                KNIT_NEW_ITEM(A.KP_NO,A.KNIT_ITEM_CODE) New_Item,
                GET_NEW_LOT_KP(A.KP_NO) New_Lot,
                SUM(A.KNIT_MC_KG) ORDER_WEIGHT,
                (SELECT SUM(sc.SCHEDULE_WEIGHT) FROM FMIT_KP_SCHEDULE sc WHERE sc.KP_NO=A.KP_NO AND sc.ITEM_CODE = A.KNIT_ITEM_CODE) SCHEDULE_WEIGHT,
                (SELECT SUM(f.FABRIC_WEIGHT) FROM FMIT_KP_FABRIC F WHERE F.KP_NO=A.KP_NO AND F.ITEM_CODE=A.KNIT_ITEM_CODE AND NVL(STATUS_FABRIC,'N')<>'D')  CUR,
                (SELECT SUM(D.FABRIC_WEIGHT) FROM FMIT_PK_DETAIL D WHERE D.KP_NO=A.KP_NO AND D.ITEM_CODE = A.KNIT_ITEM_CODE AND D.PACKING_NO LIKE '3%') DEL,
                (SELECT MAX(ss.EDD_YEAR2) FROM SF5.DUMMY_SO_HEADERS@SALESF5.WORLD ss WHERE ss.ORA_ORDER_NUMBER = A.KNIT_MC_SO) EDD_YEAR,
                (SELECT MAX(ss.EDD_WEEK2) FROM SF5.DUMMY_SO_HEADERS@SALESF5.WORLD ss WHERE ss.ORA_ORDER_NUMBER = A.KNIT_MC_SO) EDD_WEEK
            --    DECODE(SUBSTR(A.KNIT_MC_SO,4,2),'16','Salesman','18','Salesman','91','Salesman','13','Salesman','28','Salesman','Normal') SO_TYPE3
            FROM DFIT_KNIT_SOKP A,
                 sf5.DFORA_SALE@SALESF5.WORLD B 
            WHERE trunc(A.ENTRY_DATE) between To_date('01/01/2019','dd/mm/yyyy') and To_date('31/12/2021','dd/mm/yyyy')
                 and  A.KNIT_SALE_ID = b.sale_id(+)    
            GROUP BY A.KNIT_MC_CAT,
                 A.KNIT_MC_GROUP,
	               A.KNIT_MC_GUAGE,
	               A.KNIT_MC_YEAR,
	               A.KNIT_MC_WW,
                 A.KNIT_ITEM_CODE,
	               A.KNIT_ITEM_DESC,
	               A.KP_NO,
	               A.KNITTED_COLOR,
	               A.KNITTED_TYPE,
                 A.KNIT_MC_SO,A.KNIT_CUS_NAME,
                 B.SALE_NAME,
                 B.TEAM_NAME,
                 REPLACE(REPLACE(REPLACE(REPLACE(A.KNITTED_REMARK,CHR(10)||CHR(13),' '), CHR(9), ' '), CHR(10), ' '), CHR(13), ' '),
                 A.ENTRY_DATE,
          	     A.RS_NUMBER)                 
             ORDER BY KP_NO  """

  _filename = r"C:\QVD_DATA\PRO_NYK\DATA_KP_2021.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE DATA_KP_2021.xlsx")
  sendLine("COMPLETE DATA_KP_2021.xlsx")

#############################################

###########################################
class CLS_DATA_KP(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_KP()


def DATA_KP():
  my_dsn = cx_Oracle.makedsn("172.16.6.75", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYF", password="NYF", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_KP")
  sql =""" SELECT CAT,ITEM_CATEGORY, MC_GROUP, GUAGE, YEAR,
	         WEEK, KP_NO, ITEM_CODE, DESCRIPTION, KP_WEIGHT, COLOR,
	         TYPE, SO_NO, YARN_USED, ITEM_STRUCTURE, BASIC_Premium,
           END_BUYYER, FG_YEAR, FG_WEEK, Customer, SALE_NAME,
           TEAM_NAME, REMARK, Round_Minute, Revolution_Roll,
           Revolution_Weight, SO_Type, Key_In_DATE, Last_Adj_DATE,
           case when SUBSTR(SO_NO,4,2) in ('16','18','91','13','28') then 'Salesman' else
           case when SUBSTR(SO_NO,4,2) not in ('16','18','91','13','28') then 'Normal' else SO_TYPED end end SO_TYPED,
           rs_number, Yarn_Lot, New_Item,New_Lot,
           ORDER_WEIGHT, SCHEDULE_WEIGHT, NVL(CUR,0)-NVL(DEL,0) Knit_Weight,
           NVL(SCHEDULE_WEIGHT,0)-(NVL(CUR,0)-NVL(DEL,0)) OutStanding_weight,EDD_YEAR,EDD_WEEK
          FROM(
          SELECT DISTINCT A.KNIT_MC_CAT CAT,
               (SELECT max(i.ITEM_CATEGORY) FROM FMIT_ITEM I WHERE i.ITEM_CODE = A.KNIT_ITEM_CODE) ITEM_CATEGORY,
                A.KNIT_MC_GROUP MC_GROUP,
	              A.KNIT_MC_GUAGE GUAGE,
	              A.KNIT_MC_YEAR YEAR,
	              A.KNIT_MC_WW WEEK,
	              A.KP_NO,                
	              A.KNIT_ITEM_CODE ITEM_CODE,
	              A.KNIT_ITEM_DESC DESCRIPTION,
	              SUM(A.KNIT_MC_KG) KP_WEIGHT,
	              A.KNITTED_COLOR COLOR,
	              A.KNITTED_TYPE TYPE,
                A.KNIT_MC_SO SO_NO,
                GET_YARN_USED(A.KNIT_ITEM_CODE,A.KP_NO) YARN_USED,
                (SELECT max(i.ITEM_STRUCTURE) FROM FMIT_ITEM I WHERE i.ITEM_CODE = A.KNIT_ITEM_CODE) ITEM_STRUCTURE,
                GET_BASIC_PREMIUM(A.KNIT_ITEM_CODE,A.KP_NO) BASIC_Premium,
                (SELECT MAX(S.BUYYER) FROM SMIT_SO_HEADER S WHERE S.SO_NO = A.KNIT_MC_SO) END_BUYYER,
                (SELECT MAX(S.CUSTOMER_YEAR) FROM SMIT_SO_HEADER S WHERE S.SO_NO = A.KNIT_MC_SO) FG_YEAR,
                (SELECT MAX(S.CUSTOMER_FG) FROM SMIT_SO_HEADER S WHERE S.SO_NO = A.KNIT_MC_SO) FG_WEEK,
                A.KNIT_CUS_NAME Customer,
                B.SALE_NAME SALE_NAME,
                B.TEAM_NAME TEAM_NAME,
                REPLACE(REPLACE(REPLACE(REPLACE(A.KNITTED_REMARK,CHR(10)||CHR(13),' '), CHR(9), ' '), CHR(10), ' '), CHR(13), ' ') REMARK,
                (SELECT max(i.MC_RPM) FROM FMIT_ITEM I WHERE i.ITEM_CODE = A.KNIT_ITEM_CODE) Round_Minute,
                (SELECT max(i.MC_ROTATE) FROM FMIT_ITEM I WHERE i.ITEM_CODE = A.KNIT_ITEM_CODE) Revolution_Roll,
                (SELECT max(i.PRODUCTION_WEIGHT) FROM FMIT_ITEM I WHERE i.ITEM_CODE = A.KNIT_ITEM_CODE) Revolution_Weight,
                case when B.TEAM_NAME like '%MS%' THEN 'RUNNING' else ( SELECT MAX(ss.SO_TYPE) FROM SF5.DUMMY_SO_HEADERS@SALESF5.WORLD ss 
                                                                        WHERE ss.ORA_ORDER_NUMBER = A.KNIT_MC_SO) end  SO_Type,
                (SELECT max(to_date(entry_date,'dd/mm/yyyy')) FROM SF5.DUMMY_RUCONFIRM_STOCK@SALESF5.WORLD
                  WHERE Order_number = A.rs_number  and CONF_RU_TYPE ='CONFIRM_WEEK') Key_In_DATE ,                                                                    
                A.ENTRY_DATE Last_Adj_DATE,
                (SELECT max(FOB_POINT_CODE) FROM SF5.DUMMY_SO_HEADERS@SALESF5.WORLD WHERE Order_number = A.rs_number) SO_TYPED ,
                A.rs_number,
                GET_LOT_KP(A.KP_NO) Yarn_Lot,
                KNIT_NEW_ITEM(A.KP_NO,A.KNIT_ITEM_CODE) New_Item,
                GET_NEW_LOT_KP(A.KP_NO) New_Lot,
                SUM(A.KNIT_MC_KG) ORDER_WEIGHT,
                (SELECT SUM(sc.SCHEDULE_WEIGHT) FROM FMIT_KP_SCHEDULE sc WHERE sc.KP_NO=A.KP_NO AND sc.ITEM_CODE = A.KNIT_ITEM_CODE) SCHEDULE_WEIGHT,
                (SELECT SUM(f.FABRIC_WEIGHT) FROM FMIT_KP_FABRIC F WHERE F.KP_NO=A.KP_NO AND F.ITEM_CODE=A.KNIT_ITEM_CODE AND NVL(STATUS_FABRIC,'N')<>'D')  CUR,
                (SELECT SUM(D.FABRIC_WEIGHT) FROM FMIT_PK_DETAIL D WHERE D.KP_NO=A.KP_NO AND D.ITEM_CODE = A.KNIT_ITEM_CODE AND D.PACKING_NO LIKE '3%') DEL,
                (SELECT MAX(ss.EDD_YEAR2) FROM SF5.DUMMY_SO_HEADERS@SALESF5.WORLD ss WHERE ss.ORA_ORDER_NUMBER = A.KNIT_MC_SO) EDD_YEAR,
                (SELECT MAX(ss.EDD_WEEK2) FROM SF5.DUMMY_SO_HEADERS@SALESF5.WORLD ss WHERE ss.ORA_ORDER_NUMBER = A.KNIT_MC_SO) EDD_WEEK
            --    DECODE(SUBSTR(A.KNIT_MC_SO,4,2),'16','Salesman','18','Salesman','91','Salesman','13','Salesman','28','Salesman','Normal') SO_TYPE3
            FROM DFIT_KNIT_SOKP A,
                 sf5.DFORA_SALE@SALESF5.WORLD B 
            WHERE trunc(A.ENTRY_DATE) >= To_date('01/01/2022','dd/mm/yyyy')
                 and  A.KNIT_SALE_ID = b.sale_id(+)    
            GROUP BY A.KNIT_MC_CAT,
                 A.KNIT_MC_GROUP,
	               A.KNIT_MC_GUAGE,
	               A.KNIT_MC_YEAR,
	               A.KNIT_MC_WW,
                 A.KNIT_ITEM_CODE,
	               A.KNIT_ITEM_DESC,
	               A.KP_NO,
	               A.KNITTED_COLOR,
	               A.KNITTED_TYPE,
                 A.KNIT_MC_SO,A.KNIT_CUS_NAME,
                 B.SALE_NAME,
                 B.TEAM_NAME,
                 REPLACE(REPLACE(REPLACE(REPLACE(A.KNITTED_REMARK,CHR(10)||CHR(13),' '), CHR(9), ' '), CHR(10), ' '), CHR(13), ' '),
                 A.ENTRY_DATE,
          	     A.RS_NUMBER)                 
             ORDER BY KP_NO  """

  _filename = r"C:\QVD_DATA\PRO_NYK\DATA_KP.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE DATA_KP.xlsx")
  sendLine("COMPLETE DATA_KP.xlsx")

#############################################

###########################################
class CLS_DATA_RCCP(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_RCCP()


def DATA_RCCP():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_RCCP")
  sql ="""  select GET_GROUP_PLANTS(ou_code , TEAM_NAME, ITEM_CODE, COLOR_CODE ) GROUP_PLANTS ,
                 ou_code,
                 FOB_POINT_CODE,  
                 END_BUYER,
                 TEAM_NAME,
                 SO_NO,
                 FG_YEAR Edd_year1,
                 FG_WEEK EDD_WEEK1,
                 Edd_year2,
                 EDD_WEEK2,
                 RS_NUMBER,
                 SO_RESERVE,
                   (SELECT sum(CC.ORDER_PROD_QTY)  FROM sf5.DUMMY_SO_LINES LL, 
                     sf5.DUMMY_COLOR_LINES CC, 
                     sf5.DUMMY_SO_HEADERS MM
                   WHERE MM.ORDER_NUMBER=LL.ORDER_NUMBER 
                   AND LL.ORDER_NUMBER=CC.ORDER_NUMBER 
                   AND LL.LINE_NUMBER=CC.LINE_NUMBER      
                   and mm.ora_order_number = D.SO_RESERVE )  SO_RESERVE_QTY,
                 CUSTOMER_NAME,
                 SALE_NAME,
                 ITEM_CODE,
                 COLOR_CODE,
                 COLOR_DESC,
                 decode(SPIT_BATCH_STATUS, 'SPIT-BATCH',COLOR_QTY,0) PROD_QTY,
                 decode(SPIT_BATCH_STATUS, 'SPIT-BATCH',0,COLOR_QTY)  COLOR_QTY,
                 sch_fabric_qty
      from(      
      SELECT  M.FOB_POINT_CODE, 
            M.ATTRIBUTE2 END_BUYER,
            TEAM_NAME,
            M.ORA_ORDER_NUMBER SO_NO, 
            FG_YEAR,FG_WEEK,
            M.Edd_year2, M.EDD_WEEK2,
            M.ORDER_NUMBER RS_NUMBER, 
            M.SO_RESERVE,
            (SELECT CC.CUSTOMER_NAME FROM sf5.DFORA_CUSTOMER CC WHERE M.CUSTOMER_NUMBER = CC.CUSTOMER_ID) CUSTOMER_NAME ,
            SALE_NAME,
            A.ITEM_CODE,
            A.COLOR_CODE, A.COLOR_DESC,
            (select 'D0'||max(substr(machine_no,3,1)) from sf5.dfit_mc_schedule         
               where so_no = M.ORA_ORDER_NUMBER) ou_code,
             A.PORDER_QTY COLOR_QTY,
              (select sum( FABRIC_QUANTITY) from sf5.dfit_mc_schedule         
               where so_no = M.ORA_ORDER_NUMBER and ITEM_CODE = a.ITEM_CODE and color_code = A.COLOR_CODE and nyk_cancle_sch is null) sch_fabric_qty   , a.LINE_NUMBER,
            nvl( ( SELECT 'SPIT-BATCH' FROM sf5.DFIT_MC_SCHEDULE D WHERE D.SO_NO=M.ORA_ORDER_NUMBER AND ROWNUM=1),'NON SPIT') SPIT_BATCH_STATUS  
       FROM sf5.DUMMY_SO_HEADERS M  , sf5.DUMMY_CONFIRM_ORDER C, sf5.DFORA_SALE S,
       (SELECT  L.ORDER_NUMBER, L.ORDERED_ITEM ITEM_CODE,
                   C.COMM_PLCOLOR COLOR_CODE ,C.COMM_DESC COLOR_DESC, 
                   C.ORDERED_KGS PORDER_QTY, c.LINE_NUMBER
        FROM sf5.DUMMY_SO_LINES L, sf5.DUMMY_COLOR_LINES C
        WHERE L.ORDER_NUMBER=C.ORDER_NUMBER  AND L.LINE_NUMBER=C.LINE_NUMBER            
        ) A
   WHERE C.ORDER_NUMBER=M.ORDER_NUMBER
       AND M.ORDER_NUMBER=A.ORDER_NUMBER
       AND M.SALESREP_NUMBER = S.SALE_ID
       AND C.CONFIRM_TYPE='KNIT-FG CONFIRM DUMMY SO'
       AND M.FLOW_STATUS_CODE <> 'CANCEL'
       and nvl(M.CLOSE_FLAG,'N') <> 'C'
       AND DATA_REC_TYPE = 'ORDERED'
   ORDER BY M.ORDER_NUMBER
   ) D
   where (nvl(EDD_year2,FG_YEAR), nvl(edd_week2,FG_WEEK)) IN (SELECT NYEAR,NWEEK
                                          FROM NYTG.BAL_PERIOD_WW@BIS.WORLD
                                          WHERE PERIOD_TIME >= NVL('201701', PERIOD_TIME)
                                                 AND PERIOD_TIME <=  NVL('204001', PERIOD_TIME))  """

  _filename = r"C:\QVD_DATA\PRO_NYK\DATA_RCCP.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE DATA_RCCP.xlsx")
  sendLine("COMPLETE DATA_RCCP.xlsx")

#############################################

#############################################
class CLS_DATA_NonSpit_Batch(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_NonSpit_Batch()


def DATA_NonSpit_Batch():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_NonSpit_Batch")
  sql =""" select  a.*,
                   sf5.cal_Mps_dye_hr_new(a.ITEM_CODE,a.COLOR_CODE) Syd_Dye_HR,
                   sf5.CAL_KG_PER_HR(sf5.cal_Mps_dye_hr_new(a.ITEM_CODE,a.COLOR_CODE),a.COLOR_QTY) KG_HOUR
      from(SELECT C.CONFIRM_DATE,FG_YEAR,FG_WEEK,
            M.ORDER_NUMBER RS_NUMBER, M.ORA_ORDER_NUMBER SO_NO, 
            (SELECT CC.CUSTOMER_NAME FROM sf5.DFORA_CUSTOMER CC WHERE M.CUSTOMER_NUMBER = CC.CUSTOMER_ID) CUSTOMER_NAME ,
            TEAM_NAME,
            SALE_NAME,
            A.ITEM_CODE,
            M.ATTRIBUTE2 END_BUYER,
            A.COLOR_CODE, A.COLOR_DESC,
            ( SELECT  SUM(ORDERED_KGS) 
              FROM sf5.DUMMY_COLOR_LINES L
              WHERE L.ORDER_NUMBER=M.ORDER_NUMBER) PROD_QTY,
              A.PORDER_QTY COLOR_QTY,
             (SELECT  SUM(ORDERED_QUANTITY) 
              FROM sf5.smit_so_line SO
              WHERE SO.so_no =M.ORA_ORDER_NUMBER and so.ITEM_CODE = A.ITEM_CODE and so.COLOR_CODE = A.COLOR_CODE
              AND SO.TUBULAR_TYPE=A.TUBULAR_TYPE) REVISED_ORDER_QTY,
             nvl( ( SELECT 'SPIT-BATCH' FROM sf5.DFIT_MC_SCHEDULE D WHERE D.SO_NO=M.ORA_ORDER_NUMBER AND ROWNUM=1),'NON SPIT') SPIT_BATCH_STATUS,
              M.FOB_POINT_CODE,
             GET_STEP_PENDD (M.ORDER_NUMBER) STEP_PENDING,
             M.SO_RESERVE,M.SO_TYPE
       FROM sf5.DUMMY_SO_HEADERS M  , sf5.DUMMY_CONFIRM_ORDER C, sf5.DFORA_SALE S,
        (SELECT  L.ORDER_NUMBER, L.ORDERED_ITEM ITEM_CODE,
                   C.COMM_PLCOLOR COLOR_CODE ,C.COMM_DESC COLOR_DESC, 
                   C.ORDERED_KGS PORDER_QTY,L.TUBULAR_TYPE
        FROM sf5.DUMMY_SO_LINES L, sf5.DUMMY_COLOR_LINES C
        WHERE L.ORDER_NUMBER=C.ORDER_NUMBER  AND L.LINE_NUMBER=C.LINE_NUMBER            
        ) A
   WHERE C.ORDER_NUMBER=M.ORDER_NUMBER
       AND M.ORDER_NUMBER=A.ORDER_NUMBER
       AND M.SALESREP_NUMBER = S.SALE_ID
       AND C.CONFIRM_TYPE='KNIT-FG CONFIRM DUMMY SO'
       AND M.FLOW_STATUS_CODE <> 'CANCEL'
       and nvl(M.CLOSE_FLAG,'N') <> 'C'
       AND DATA_REC_TYPE = 'ORDERED'
   ORDER BY M.ORDER_NUMBER) A
   WHERE spit_batch_status ='NON SPIT'
      and (FG_YEAR, FG_WEEK) IN (SELECT NYEAR,NWEEK
                                 FROM NYTG.BAL_PERIOD_WW@BIS.WORLD
                                 WHERE PERIOD_TIME >= NVL('201701', PERIOD_TIME)
                                  AND  PERIOD_TIME <=  NVL('204001', PERIOD_TIME))  """

  _filename = r"C:\QVD_DATA\PRO_NYK\DATA_NonSpit_Batch.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE DATA_NonSpit_Batch.xlsx")
  sendLine("COMPLETE DATA_NonSpit_Batch.xlsx")

#############################################

#############################################
class CLS_DATA_Status_Batch(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_Status_Batch()


def DATA_Status_Batch():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_Status_Batch")
  sql ="""  SELECT C.CONFIRM_DATE,FG_YEAR,FG_WEEK, 
            M.FOB_POINT_CODE FOB_CODE, 
            sh.CUSTOMER_YEAR SO_FG_YEAR, 
            sh.CUSTOMER_FG SO_FG_WW, 
            M.ORDER_NUMBER RS_NUMBER, M.ORA_ORDER_NUMBER SO_NO, 
            (SELECT CC.CUSTOMER_NAME FROM sf5.DFORA_CUSTOMER CC WHERE M.CUSTOMER_NUMBER = CC.CUSTOMER_ID) CUSTOMER_NAME ,
             M.ATTRIBUTE2 END_BUYER,
            TEAM_NAME,
            SALE_NAME,
            A.TUBULAR_TYPE,
            A.ITEM_CODE,
            I.MATERIAL_YARN,
            I.O_GAUGE,
            I.O_DIAMETER,
            A.COLOR_CODE, A.COLOR_DESC,
            ( SELECT  SUM(ORDERED_KGS)  
              FROM sf5.DUMMY_COLOR_LINES L
              WHERE L.ORDER_NUMBER=M.ORDER_NUMBER) PROD_QTY,
            A.PORDER_QTY COLOR_QTY,
             (SELECT  SUM(ORDERED_QUANTITY) 
              FROM sf5.smit_so_line SO
              WHERE SO.so_no =M.ORA_ORDER_NUMBER and so.ITEM_CODE = A.ITEM_CODE and so.COLOR_CODE = A.COLOR_CODE) REVISED_ORDER_QTY,
            ( SELECT 'SPIT-BATCH' FROM sf5.DFIT_MC_SCHEDULE D WHERE D.SO_NO=M.ORA_ORDER_NUMBER AND ROWNUM=1) SPIT_BATCH_STATUS,
            NVL(SH.SO_STATUS,'99.Pending Order') SO_STATUS, 
            A.O_MACHINE_GROUP,
           (select distinct  dc.RU_CONFIRM_TYPE from  sf5.DUMMY_CONFIRM_ORDER dc where DC.ORDER_NUMBER=M.ORDER_NUMBER 
            and dc.steps_id = 6 and dc.RU_CONFIRM_TYPE is not null)  RU_CONFIRM_TYPE,M.SO_TYPE,
           (select max(cc.CONFIRM_DATE) from sf5.DUMMY_CONFIRM_ORDER cc where cc.ORDER_NUMBER =  m.ORDER_NUMBER  and cc.steps_id = 2)  Issue_dummy_date,
           (select b.BOOKED_DATE from rapps.OE_ORDER_HEADERS_ALL@R12INTERFACE.WORLD b where b.order_number = M.ORA_ORDER_NUMBER) BOOK_DATE,
           M.CLOSE_DATE OPEN_SO_DATE,
           PORDER_QTY ORDERED_KGS
        FROM sf5.DUMMY_SO_HEADERS M  , sf5.DUMMY_CONFIRM_ORDER C, sf5.DFORA_SALE S,
              (SELECT  L.ORDER_NUMBER, L.ORDERED_ITEM ITEM_CODE,
               C.COMM_PLCOLOR COLOR_CODE ,C.COMM_DESC COLOR_DESC, 
               C.ORDERED_KGS PORDER_QTY, decode(L.TUBULAR_TYPE,1,'อบกลม',2,'อบผ่า',null) TUBULAR_TYPE,
                L.O_MACHINE_GROUP
        FROM sf5.DUMMY_SO_LINES L, sf5.DUMMY_COLOR_LINES C
        WHERE L.ORDER_NUMBER=C.ORDER_NUMBER  AND L.LINE_NUMBER=C.LINE_NUMBER            
              ) A,
            SF5.SMIT_SO_HEADER sh,
            SF5.FMIT_ITEM I
        WHERE C.ORDER_NUMBER=M.ORDER_NUMBER
          AND M.ORDER_NUMBER=A.ORDER_NUMBER
          AND M.SALESREP_NUMBER = S.SALE_ID
          AND C.CONFIRM_TYPE='KNIT-FG CONFIRM DUMMY SO'
          AND M.FLOW_STATUS_CODE <> 'CANCEL'
          and nvl(M.CLOSE_FLAG,'N') <> 'C'
          AND M.ORA_ORDER_NUMBER = sh.so_no(+)
          AND I.ITEM_CODE = A.ITEM_CODE
          AND trunc(C.CONFIRM_DATE) >= TO_DATE('01/01/2019','DD/MM/RRRR')
        ORDER BY M.ORDER_NUMBER """

  _filename = r"C:\QVD_DATA\PRO_NYK\DATA_Status_Batch.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE DATA_Status_Batch.xlsx")
  sendLine("COMPLETE DATA_Status_Batch.xlsx")

#############################################

###########################################
class CLS_DATA_NYK_DEADNON(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_NYK_DEADNON()


def DATA_NYK_DEADNON():
  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="DEMO", password="DEMO", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_NYK_DEADNON")
  sql ="""      select RECEIVE_DATE RECEIVE_WH_DATE,
           trunc(REASON_UPDATE) PL_REASON_DATE,
           ISSUE_DATE ISSUE_WH_DATE,
           PL_NO,
           OU_CODE,
           BATCH_NO,GRADE,
           SO_NO,
           LINE_ID,
           so.packing_instructions edd_week1,
           (select customer_year from sf5.smit_so_header@SALESF5.WORLD where so_no = a.so_no) edd_year1,
           so.attribute18 edd_week2,
           so.attribute19 edd_year2,
           CUSTOMER_NAME,
           BUYER,
           TEAM_NAME,
           a.SALE_NAME,
           ITEM_CODE,
           ITEM_DESC,
           COLOR_ID,
           COLOR_DESC,
           PL_COLORLAB,
           TUBULAR_TYPE,
           MATERIAL_GROUP,
           DYE_TYPE,
           STRUCTURE_GROUP,
           WIDTH,
           WEIGHT_G,
           WEIGHT_Y,
           WEIGHT QTY,
           DOZ,
           ROLL,
           PL_REASON_DESC,
           REASON_STATUS,
           FQC_NO,
           FQC_DATE,
           FQC_PROBLEM,
           FQC_CAUSE,
           FQC_REF_OWER,
           a.inactive_grade, a.LOC_NO,
           a.bal_reason_rec Balance_Rec_Inactive
      from demo.v_fg_deadnon a,
             sf5.dfora_sale@SALESF5.WORLD s,
             rapps.oe_order_headers_all@R12INTERFACE.WORLD so
      where a.sale_id = s.sale_id
            and s.active = 'Y'
            and a.so_no = so.order_number(+)
            and trunc(a.RECEIVE_DATE) >= TO_DATE('01/01/2005','DD/MM/YYYY')
      order by RECEIVE_DATE  """

  _filename = r"C:\QVD_DATA\PRO_NYK\DATA_NYK_DEADNON.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_NYK_DEADNON")
  sendLine("COMPLETE CLS_DATA_NYK_DEADNON")

#############################################

###########################################
class CLS_NYK_DATA_MINING(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    NYK_DATA_MINING()


def NYK_DATA_MINING():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_NYK_DATA_MINING")
  sql =""" select SIZE_UNIT, DH, MC_GROUP, Mps_Mc_No, SO_NO, LINE_ID, PO_NO, CUSTOMER_PO
       ,to_char(SO_REQUEST_DATE,'DD/MM/YYYY') SO_REQUESTED_DATE, EDD_YEAR2, EDD_WEEK2, STYLE, FG_YEAR, FG_WEEK, DH_YEAR, DH_WEEK
       ,CUSTOMER_ID, CUSTOMER_NAME, BUYER, SALE_ID, SALE_NAME, TEAM_NAME, SCHEDULE_ID
       ,SCHEDULE_ID_REF, MPS_CYCLE, BATCH_NO, TOTAL_ROLL, TOTAL_QTY, TUBULAR_TYPE_DESC
       ,ITEM_TYPE, ITEM_CODE, ITEM_DESC, COLOR_CODE, COLOR_DESC, NYK_ITEM_PROCESS
       ,WIDTH, WEIGHT, PACK_TYPE, FABRIC_MER, FABRIC_SCR, FABRIC_SET, FABRIC_SIGYN
       ,PREP, YARN_LOT, KP_NO, STATUS, PRODUCT_LINE, trunc(SO_NO_DATE) SO_NO_DATE
       ,trunc(NYK_TRANSFER_DATE) NYK_TRANSFER_DATE ,trunc(NYF_TRANS_DATE) NYF_TRANS_DATE
       ,trunc(SPLIT_DATE) SPLIT_DATE, KNIT_MC_YEAR Knit_Year, KNIT_MC_WW Knit_WEEK, trunc(CONFIRM_FAB_DATE) CONFIRM_FAB_DATE
       ,trunc(STOCK_KNIT_RECEIVED_DATE) STOCK_KNIT_RECEIVED_DATE, trunc(GREY_IN_DATE) GREY_IN_DATE
       ,trunc(NYK_REC_DATE) NYK_REC_DATE, trunc(MPS_DYE_DATE) MPS_DYE_DATE, trunc(BATCH_DATE) BATCH_DATE
       ,trunc(FAB_DATE) FAB_DATE, trunc(DYE_DATE) DYE_DATE, trunc(PL_DATE) PL_DATE
       ,trunc(QT_DATE) QT_DATE, trunc(SHIP_DATE) SHIP_DATE, P_TIME, F_TIME, PRELAB_STATUS
       ,FIRST_BATCH, trunc(SCH_CLOSED_DATE) SCH_CLOSED_DATE, SO_FOB_CODE,trunc(ENTRY_DATE) LAST_UPDATE, Status_WIP, Mps_Condition
       ,LAB_REM LAB_Remark,REF_SCHEDULE_ID Ref_Schedule,TOTAL_QTY Schedule_Qty,  trunc(REC_FLATKN_DATE) REC_FLATKN_DATE --วันรับ_Flatknit
       ,Ref_FQC, WW_Entry_FQC ,Reason_Confirm_so, ACTUAL_WIP,trunc(FQC_Date) FQC_Date,  trunc(MER_Confirm_Date) MER_Confirm_Date
       ,SO_Type, Order_Type ,LOAD_DYE, SCH_Close, PRODUCT_DH Product,Chk_SO,SIZE_MC Sizes, A_Time
       ,P_Time, F_Time, MPS_Status ,Index_p1, Item, Proc_PD, Proc_set, Proc_sin
       ,Proc_PF, SP_Proc, TUBE, Job_Type, Shade, C_Time, Index_p2, Jacuard Jacquard, F_Type
       ,trunc(dye_date) dye_date, Type_F, KNIT_CF, REASON_UNLOAD, QRD, ANTI_BAC
       ,REASON_KNIT SO_REASON_KNIT, KNIT_WEEK SO_KNIT_WEEK,  KNIT_REMARK SO_KNIT_REMARK
       ,nytg.GET_FQC_CAUSE(SCHEDULE_ID) FQC_CAUSE
       ,nytg.GET_FQC_PROBLEM(SCHEDULE_ID) FQC_PROBLEM
       ,(select sum(NYK_FG_WEIGHT) from sf5.DFIT_MC_SCH_OMNOI B where a.SCHEDULE_ID = B.SCHEDULE_ID) NYK_FG_WEIGHT
       ,sf5.cal_Mps_dye_hr_new(ITEM_CODE,COLOR_CODE) Syd_Dye_HR
       ,sf5.CAL_KG_PER_HR(sf5.cal_Mps_dye_hr_new(ITEM_CODE,COLOR_CODE),a.total_qty) KG_HOUR
      from sf5.DFIT_MPS_DATA_MINING a"""

  _filename = r"C:\QVD_DATA\PRO_NYK\.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE NYK_DATA_MINING.xlsx")
  sendLine("COMPLETE NYK_DATA_MINING.xlsx")

#############################################

###########################################
class CLS_DATA_BILL_OF_MATERIALS(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_BILL_OF_MATERIALS()


def DATA_BILL_OF_MATERIALS():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSf5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_BILL_OF_MATERIALS")
  sql =""" SELECT S.KNIT_MC_YEAR YEAR, S.KNIT_MC_WW WEEK,
                   S.KNIT_MC_SO SO,
                   S.KNIT_MC_PO PO,
                   S.KP_NO,
                   S.KNIT_ITEM_CODE KNIT_ITEM,
                   S.KNITTED_COLOR COLOR_DESC,
                   Y.ITEM_CONTAIN,
                   Y.YARN_ITEM,
                   S.KNIT_SALE_NAME SALE,
                   S.KNIT_CUS_NAME CUSTOMER,
                   S.KNIT_MC_CAT MC_CAT,
                   S.KNIT_MC_GROUP MC_GROUP,
                   DECODE(S.KNIT_MC_GROUP,'CL-OM','ทอปกอ้อมน้อย','COMKN','ส่งจ้างทอ',
                   DECODE(SUBSTR(S.KNIT_MC_GROUP,1,2),'FA','For Nike ทออ้อมน้อย',S.KNIT_MC_GROUP)) MC_DESC,
                   SUM(UNIT_QTY*Y.ITEM_CONTAIN/100) RM_KG,
                   SUM(S.KNIT_MC_KG) KNIT_UNIT,
                   SUM(S.UNIT_QTY) KNIT_KG,
                   (SELECT NYF_BUYER FROM SF5.SMIT_SO_HEADER SO WHERE SO.SO_NO=S.KNIT_MC_SO) END_BUYER
               FROM  SF5.DFIT_KNIT_SOKP S, SF5.DFIT_KNIT_YARN y
               WHERE Y.ITEM_CODE=S.KNIT_ITEM_CODE AND
                  NVL(S.KNIT_MC_KG,0)>0 AND
                  YARN_BOM_ACTIVE='Y' AND
                  S.KNIT_MC_YEAR >= 2020
               GROUP BY S.KNIT_MC_YEAR, S.KNIT_MC_WW,S.KNIT_MC_SO,S.KNIT_MC_PO,S.KP_NO,
                       S.KNIT_ITEM_CODE,S.KNITTED_COLOR,Y.ITEM_CONTAIN,Y.YARN_ITEM,
                       S.KNIT_SALE_NAME,S.KNIT_CUS_NAME,S.KNIT_MC_CAT, S.KNIT_MC_GROUP """

  _filename = r"C:\QVDatacenter\SCM\FABRIC\DATA_BILL_OF_MATERIALS.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE DATA_BILL_OF_MATERIALS.xlsx")
  sendLine("COMPLETE DATA_BILL_OF_MATERIALS.xlsx")

#############################################

###########################################
class CLS_DATA_TRANSFER_GOODS_NYK(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_TRANSFER_GOODS_NYK()


def DATA_TRANSFER_GOODS_NYK():
  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="DEMO", password="DEMO", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_TRANSFER_GOODS_NYK")
  sql =""" SELECT SHIPMENT_DATE, ou_code, PL_NO, Fabric_Type,   
        SO_NO, Line_Id, STD_Lost, APPIONT_DATE, PO_NO,           
        BATCH_NO,CUSTOMER_NAME, BUYYER, ITEM_CODE,
        ITEM_DESC, COLOR_CODE, COLOR_Desc, Shade,      
        OU_CODE, DYEDATE, PL_DATE, TIME_WIP, PL_NO,          
        SHIP_BY, ROLL, QTY_RM, QTY_FG, DOZ,           
        (nvl(QTY_RM,0) - nvl(QTY_FG,0)) LOST,           
        case when nvl(qty_rm,0) <> 0 then round((nvl(qty_rm,0)-nvl(qty_fg,0))/(nvl(qty_rm,0))*100,2) else 0 end PERCENT_LOST,          
        UPD_BY, SLAE_ID, SALE_NAME, TEAM_NAME,    
        NYK_DYE_PRICE, CHE_FIN_KG_PER_FG, CHE_DYE_KG_PER_FG,
        GRADE_NO, PRODUCT_TYPE, PROCESS_LINE, WIDTH,            
        WEIGHT_G, Item_process, Type_OP,Type_Pack,         
        So_No_date, DH_WEEK, YARD, Schedule_Id, STYLE_CODE,       
        Week_Year, Week_Fg, So_Bu, SR_NO, Std_FG_Cost,       
        Act_FG_Cost, Std_CC, Std_OH, Std_Energy,        
        Std_DP, Std_GF_Cost, Act_GF_Cost, Type_Fabric, Job_Type,         
        Reason_Stock, Status, Remark, Week_Reason,      
        OPEN_DATE, DUE_DATE, Fqc_Roll, Fqc_Qty, Check_FG,         
        Fqc_Problem, Fqc_Cause, Pl_Remark, APP_Remark         
        FROM(
             SELECT trunc(ph.shipment_date) SHIPMENT_DATE,    
                    Sh.so_no SO_NO,            
                    Nvl(Ph.SO_LINE_ID,Sh.Line_Id) Line_Id,         
                    Sl.OE_PRICE_LOST STD_Lost,        
                    Sh.appoint_date APPIONT_DATE,    
                    sh.po_no PO_NO,           
                    Sh.batch_no BATCH_NO,        
                    sh.customer_name CUSTOMER_NAME,    
                    sh.buyyer BUYYER,          
                    sh.item_code ITEM_CODE,
                    (select i.item_desc from Dfora_Item i where i.item_code = sh.item_code) ITEM_DESC,        
                    sh.color_code COLOR_CODE,
                    sh.color_desc COLOR_Desc,       
                    Sh.Color_Shade Shade,      
                    Sh.ou_code OU_CODE,          
                    trunc(sh.dye_sdate) DYEDATE,        
                    ph.pl_date PL_DATE,        
                    (case when sh.qt_date is null then trunc(ph.pl_date)-trunc(sh.dye_sdate) else
                    case when ph.pl_date >= sh.qt_date then trunc(ph.pl_date)-trunc(sh.dye_sdate) else
                    case when ph.pl_date <= sh.qt_date then trunc(sh.qt_date)-trunc(sh.dye_sdate)  end end end) TIME_WIP,       
                    ph.pl_no PL_NO,          
                    DECODE(ph.ship_to_wh,'NN','P','L') SHIP_BY,       
                    get_pl_tot_roll(ph.Ou_Code,ph.Pl_No,ph.Batch_No) ROLL,           
                    Decode(Substr(ph.pl_no,3,1),'S',0,get_pl_tot_sum_rm(ph.Ou_Code,ph.Pl_No,ph.Batch_No)) QTY_RM,        
                    get_pl_tot_sum(ph.Ou_Code,ph.Pl_No,ph.Batch_No) QTY_FG,        
                    Ph.Unit_Fg DOZ,          
                    ph.UPD_BY UPD_BY,         
                    sh.sale_id SLAE_ID,       
                    sh.sale_name SALE_NAME,   
                    e.team_name TEAM_NAME,    
                    SL.OE_PRICE_SELL NYK_DYE_PRICE, 
                    GET_FIN_KG_PER_FG (Sh.ou_code, Sh.batch_no, SH.CHE_FIN_COST, SH.TOTAL_COST,'COST') CHE_FIN_KG_PER_FG,
                    GET_FIN_KG_PER_FG (Sh.ou_code, Sh.batch_no, SH.CHE_FIN_COST, SH.TOTAL_COST,'TOTAL') CHE_DYE_KG_PER_FG,
                    PH.GRADE_NO,          
                    SH.PRODUCT_TYPE,     
                    SH.PROCESS_LINE,      
                    SH.WIDTH,            
                    SH.WEIGHT_G,          
                    SH.Item_process,     
                    Sl.Attribute1 Type_OP,         
                    Sl.Attribute2 Type_Pack,       
                    SH.SO_NO_DATE,       
                    sh.MPS_WEEK_NO DH_WEEK,          
                    get_yard_Dfpl_Detail (Sh.ou_code, Sh.batch_no, ph.pl_no) YARD,              
                    SH.Schedule_Id,       
                    sh.STYLE STYLE_CODE,       
                    (Select So.Year_Week_Fg From Dfit_So_Header So Where So.So_No = SH.So_No) Week_Year,         
                    (Select So.Week_Fg From Dfit_So_Header So Where So.So_No = SH.So_No) Week_Fg,           
                    Decode(Substr(Sh.So_No,1,1),'2','NYK',Decode(Substr(Nvl(Sh.Org_Id,0),1,1),'2','NYK','NYF')) SO_BU,             
                    (Select dh.so_no_r From Dfbt_Header Dh Where Dh.Ou_Code = Sh.Ou_Code And Dh.Batch_No = Sh.Batch_No) SR_NO,             
                    SL.Std_Fg_Cost Std_FG_Cost,       
                    (round(nvl(sh.total_cost,0) + CAL_moniter_btcost(sh.ou_code,sh.batch_no,'5')+(sh.total_qty*decode(chk_act_scc_bt (sh.ou_code,sh.batch_no),1,nvl(Sl.SCC_Cost,0),0)),2)) Act_FG_Cost,      
                    (nvl(sl.SDC_Cost,0) + nvl(sl.SCC_Cost,0) + nvl(sl.scf_cost,0)) Std_CC,            
                    (nvl(sl.OHC_PRE_COST,0) + nvl(sl.OHC_dye_COST,0) + nvl(sl.OHC_Fin_COST,0) + nvl(sl.OHC_ins_COST,0) + nvl(sl.OHC_ship_COST,0) + nvl(sl.ipc_step_cost,0)) Std_OH,           
                    (nvl(sl.SED_Cost,0) + nvl(sl.ipc_mc_cost,0)) Std_Energy,        
                    nvl(sl.ipc_depre_cost,0) Std_DP,            
                    STD_Gfcost_item(sh.item_code) Std_GF_Cost,       
                    cal_gfcost_kg_sch(sh.schedule_id) Act_GF_Cost,      
                    Decode(Substr(sh.item_code,1,1),'F','FABRIC','COLLAR') Type_Fabric,       
                    Decode(NVL(Sh.job_type,'@'),'N','ปกติ','D','ซ่อมภายใน','R','ซ่อมภายนอก') Job_Type,         
                    get_reason_desc(Sh.ou_code, ph.PL_NO, ph.Fabric_Type)  Reason_Stock,  ph.Fabric_Type,  
                    get_REC_STATUS_DEADNON (Sh.ou_code, ph.PL_NO, ph.Fabric_Type) Status,         
                    (SELECT  MAX(PR.pl_remark) FROM SF5_DFWH_PL_REA PR WHERE PR.OU_CODE=SH.OU_CODE  AND PR.PL_NO = PH.PL_NO  AND PR.FG_TYPE = ph.Fabric_Type) Remark,           
                    (select to_char(max(DR.REASON_UPDATE),'IW') from SF5_DFWH_PL_REA DR WHERE DR.pl_no = PH.PL_NO AND DR.OU_CODE = SH.OU_CODE AND DR.FG_TYPE = ph.Fabric_Type) Week_Reason,      
                    null OPEN_DATE,        
                    null DUE_DATE,         
                    get_FQC_BY_TYPE (Sh.Ou_Code,Sh.Batch_No, Ph.PL_no,'ROLL') Fqc_Roll,          
                    get_FQC_BY_TYPE (Sh.Ou_Code,Sh.Batch_No, Ph.PL_no,'QTY') Fqc_Qty,          
                    get_ROLL_PL_DEL_WH (Sh.Ou_Code,Sh.Batch_No, Ph.PL_no) Check_FG,         
                    get_FQC_BY_TYPE (Sh.Ou_Code,Sh.Batch_No, Ph.PL_no,'PROBLEM') Fqc_Problem,       
                    get_FQC_BY_TYPE (Sh.Ou_Code,Sh.Batch_No, Ph.PL_no,'CAUSE') Fqc_Cause,        
                    Ph.Remark Pl_Remark,        
                    Ph.APPROVED_REMARK APP_Remark        
               FROM DFPL_HEADER ph, 
                    dfit_btdata sh, 
                    dfora_sale E,
                    DFIT_SO_LINE SL
               WHERE ph.ou_code = Sh.ou_code
                AND  ph.batch_no = Sh.batch_no
                AND  sh.sale_id = e.sale_id
                AND  Sh.SO_NO = Sl.SO_NO(+)
                AND  Sh.LINE_ID = Sl.LINE_ID(+)
                and  Sh.status <> '9' 
                and  ph.status not in ('x','X','9')
                AND trunc(ph.shipment_date) >= to_date('01/01/2021','dd/mm/yyyy')) """

  _filename = r"C:\QVD_DATA\PRO_NYK\DATA_TRANSFER_GOODS_NYK.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE DATA_TRANSFER_GOODS_NYK.xlsx")
  sendLine("COMPLETE DATA_TRANSFER_GOODS_NYK.xlsx")

#############################################

###########################################
class CLS_DATA_SOColor_Ref_SODummy(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_SOColor_Ref_SODummy()


def DATA_SOColor_Ref_SODummy():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_SOColor_Ref_SODummy")
  sql =""" select SO_MAIN, RS_MAIN, SO_MAIN_ITEM, KNIT_YEAR, KNIT_WEEK, KNIT_YYWW, RS_COLOR, SO_COLOR, 
                  ITEM_CODE, USED_QTY, UOM_CODE, ORDERED_KGS
           from sf5.smit_ref_so """

  _filename = r"C:\QVD_DATA\PRO_NYK\DATA_SOColor_Ref_SODummy.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE DATA_SOColor_Ref_SODummy.xlsx")
  sendLine("COMPLETE DATA_SOColor_Ref_SODummy.xlsx")

#############################################

###########################################
class CLS_DATA_DUMMY_App_Time_Step(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_DUMMY_App_Time_Step()


def DATA_DUMMY_App_Time_Step():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_DUMMY_App_Time_Step")
  sql =""" select *
           from nyis.DUMMY_APP_STEPS_V """

  _filename = r"C:\QVD_DATA\PRO_NYK\DATA_DUMMY_App_Time_Step.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE DATA_DUMMY_App_Time_Step.xlsx")
  sendLine("COMPLETE DATA_DUMMY_App_Time_Step.xlsx")

#############################################

###########################################
class CLS_DATA_DUMMY_App_Time_Step_DETAIL(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_DUMMY_App_Time_Step_DETAIL()


def DATA_DUMMY_App_Time_Step_DETAIL():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_DUMMY_App_Time_Step_DETAIL")
  sql =""" SELECT a.*,
                  SF5.GET_WK_MRD(ORDER_NUMBER,NULL) SCMG,
                  SF5.GET_MRD_NYK(NULL,ORDER_NUMBER,NULL) SCMK
           FROM nyis.DUMMY_APP_STEPS_BOM_V A """

  _filename = r"C:\QVD_DATA\PRO_NYK\DATA_DUMMY_App_Time_Step_DETAIL.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE DATA_DUMMY_App_Time_Step_DETAIL.xlsx")
  sendLine("COMPLETE DATA_DUMMY_App_Time_Step_DETAIL.xlsx")

#############################################

###########################################
class CLS_DATA_CUSTOMER_PO_TRACK(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_CUSTOMER_PO_TRACK()


def DATA_CUSTOMER_PO_TRACK():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_CUSTOMER_PO_TRACK")
  sql =""" SELECT *
           FROM CUSTOMER_PO_TRACK_EXP_V 
           WHERE PO_NO like 'FL2%' """

  _filename = r"C:\QVD_DATA\PRO_NYK\DATA_CUSTOMER_PO_TRACK.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE DATA_CUSTOMER_PO_TRACK.xlsx")
  sendLine("COMPLETE DATA_CUSTOMER_PO_TRACK.xlsx")

#############################################


threads = []

#thread1 = CLS_PROD_OUTPUT();thread1.start();threads.append(thread1) # Move to production_output.py
thread2 = CLS_PROD_PLANING();thread2.start();threads.append(thread2)
#thread3_2021 = CLS_DATA_KP_2021();thread3_2021.start();threads.append(thread3_2021) #Cut Data Create KP 2019 -2021
thread3 = CLS_DATA_KP();thread3.start();threads.append(thread3)
thread4 = CLS_DATA_RCCP();thread4.start();threads.append(thread4)
thread5 = CLS_DATA_NonSpit_Batch();thread5.start();threads.append(thread5)
thread6 = CLS_DATA_Status_Batch();thread6.start();threads.append(thread6)
#thread7 = CLS_DATA_NYK_DEADNON();thread7.start();threads.append(thread7) #NO USED
#thread8 = CLS_NYK_DATA_MINING();thread8.start();threads.append(thread8) # Move To production2.py Request Move By Chonlada Suksamer
thread9 = CLS_DATA_BILL_OF_MATERIALS();thread9.start();threads.append(thread9)
thread10 = CLS_DATA_TRANSFER_GOODS_NYK();thread10.start();threads.append(thread10)
thread11 = CLS_DATA_SOColor_Ref_SODummy();thread11.start();threads.append(thread11)
thread12 = CLS_DATA_DUMMY_App_Time_Step();thread12.start();threads.append(thread12)
thread13 = CLS_DATA_DUMMY_App_Time_Step_DETAIL();thread13.start();threads.append(thread13)
thread14 = CLS_DATA_CUSTOMER_PO_TRACK();thread14.start();threads.append(thread14)


for t in threads:
    t.join()
print ("COMPLETE")

