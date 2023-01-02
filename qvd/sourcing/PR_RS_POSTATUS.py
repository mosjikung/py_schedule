#-*-coding: utf-8 -*-
import cx_Oracle
import csv
import os
from pathlib import Path
import threading

# oracle_client = "C:\instantclient_12_2"


class CLS_call1(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      call1()

def call1():
  oracle_client = r"C:\instantclient_19_5"
  os.environ["ORACLE_HOME"]=oracle_client
  os.environ["PATH"]=oracle_client+os.pathsep+os.environ["PATH"]
  os.environ["NLS_LANG"]="AMERICAN_AMERICA.TH8TISASCII"
  my_dsn = cx_Oracle.makedsn("172.16.6.74",port=1521,sid="NYTG")
  #Replica.world
  conn = cx_Oracle.connect(user="sf5", password="omsf5", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()

  # conn2 = cx_Oracle.connect(user="nyis", password="nyis", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  # cursor2 = conn2.cursor()

  cursor.execute("""SELECT PR.ORG_NAME , 
                PR.PR_NO , 
                PR.PR_DATE , 
                PR.PR_STATUS , 
                PR.PR_APPROVED_DATE,
                PR.LINE_NUM , 
                PR.ITEM_DESCRIPTION , 
                PR.ITEM_CODE ,
                PR.UOM,
                PR.QUANTITY ,
                PR.PR_YARN_IN_DATE  ,
                BYR.LAST_NAME BUYER_NAME ,
                PO.PO_YARN_IN_DATE ,
                PO.PROMISED_DATE ,
                PO.PO_NUMBER ,
                PO.PO_DATE ,
                PO.PO_LINE_NO ,
                PO.PO_STATUS ,
                PO.PO_APPROVED_DATE ,
                PO.VENDOR_NAME ,
                TRUNC(PR.PR_YARN_IN_DATE)-NVL(TRUNC(PO.PO_YARN_IN_DATE),TRUNC(SYSDATE)) DIF  
        FROM NY_PR_LINE_DISTRIBUTIONS_V  PR ,
                      NY_PO_LINE_DISTRIBUTIONS_V  PO ,
                      PER_PEOPLE_F  BYR
        WHERE PO.REQ_DISTRIBUTION_ID(+) = PR.DISTRIBUTION_ID 
        AND BYR.PERSON_ID(+) = PR.BUYER_ID
        AND EXTRACT(YEAR FROM PR.PR_APPROVED_DATE) >= (to_number(to_char(sysdate,'YYYY')) - 5) """)


  with open(r"\\172.16.0.182\yarn_center\MT Team\PR&RS&PO\PRvsPO_1077.csv", "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
    
  conn.close()
  print('Success Call 1')
  

class CLS_call2(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      call2()

def call2():
  oracle_client = r"C:\instantclient_19_5"
  os.environ["ORACLE_HOME"] = oracle_client
  os.environ["PATH"]=oracle_client+os.pathsep+os.environ["PATH"]
  os.environ["NLS_LANG"]="AMERICAN_AMERICA.TH8TISASCII"
  my_dsn = cx_Oracle.makedsn("172.16.6.74",port=1521,sid="NYTG")
  #Replica.world
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
    
    
  cursor.execute(""" SELECT FG_WEEK,KNIT_WEEK_END KNIT_WEEK,  LTRIM(TO_CHAR(RS_NO)) RS_NO ,SO_NO ,
                DUMMY_ITEM_TYPE ORDER_TYPE, SALE_NAME, TEAM_NAME, CUSTOMER_NAME, END_BUYER,
                MS_APPROVED_DATE,  YARN_ITEM, YARN_COLOR, PLANIN_WW, NVL(PO_SHIPPMENT,PLANIN_DATE)  YARN_IN_DATE, YARN_QTY, 
                CONFIRM_PLANIN,--CONFIRM_PLANIN_BY,
                YARN_PONO, PO_LINE,
                  LTRIM(TO_CHAR(PO_IN_DATE,'IYYYIW')) PO_IN_WEEK, PO_IN_DATE,--,CONFIRM_DATE
                TRUNC(NVL(PO_SHIPPMENT,PLANIN_DATE))-NVL(TRUNC(PO_IN_DATE),TRUNC(SYSDATE)) DIF,
                (SELECT FLOW_STATUS_CODE FROM SF5.DUMMY_SO_HEADERS DH  WHERE DH.ORDER_NUMBER=V.RS_NO) RS_STATUS,
                (SELECT CREATION_DATE  FROM SF5.DUMMY_SO_HEADERS  WHERE ORDER_NUMBER=V.RS_NO) CREATE_DATE,
                (SELECT DISTINCT  PO_STATUS  FROM SF5.NY_PO_LINE_DISTRIBUTIONS_V NPO WHERE NPO.PO_NUMBER =V.YARN_PONO
                                    AND NPO.PO_LINE_NO =V.PO_LINE) ORA_PO_STATUS
          FROM SF5.DUMMY_YARN_PLANIN_V V
          WHERE PO_FLAG='PO' 
            AND EXTRACT(YEAR FROM MS_APPROVED_DATE) >= (to_number(to_char(sysdate,'YYYY')) - 5)
          UNION
          SELECT LTRIM(TO_CHAR(FG_WEEK)) FG_WEEK, LTRIM(TO_CHAR(KNIT_WEEK)) KNIT_WEEK, RS_NO, SO_NO, 
                ORDER_TYPE, SALE_NAME, TEAM_NAME, CUSTOMER_NAME,END_BUYER,
                MS_APPROVED_DATE, YARN_ITEM, YARN_COLOR, PLANIN_WW, YARN_IN_DATE,YARN_QTY, CONFIRM_PLANIN,
                PO_NO, PO_LINE, PO_IN_WEEK, PO_IN_DATE,
                TRUNC(NVL(YARN_IN_DATE,CONFIRM_PLANIN))-NVL(TRUNC(PO_IN_DATE),TRUNC(SYSDATE))  DIF,
                ( CASE WHEN PO_NO IS NULL AND SC_REJECT IS NULL THEN 'OPEN' ELSE
                    CASE WHEN PO_NO IS NOT NULL  AND PO_STATUS  IS NULL THEN 'IN-PROCESS' ELSE 
                        CASE WHEN PO_NO IS NOT NULL  AND PO_STATUS  IS NOT NULL THEN 'APPROVED' ELSE  SC_REJECT END END END ) DOC_REJECT,
                        (SELECT  OPEN_DATE  FROM SF5.DUMMY_PURCHASEYARN_H  WHERE YP_ORDER=RS_NO) CREATE_DATE,
                        (SELECT DISTINCT  PO_STATUS  FROM SF5.NY_PO_LINE_DISTRIBUTIONS_V NPO WHERE NPO.PO_NUMBER =w.po_no
                                    AND NPO.PO_LINE_NO =w.po_line )ORA_PO_STATUS
              -- NVL(SC_REJECT,
              --  (SELECT 'REJECT' FROM SF5.DUMMY_PURCHASEYARN_H  YH WHERE YH.YP_ORDER= W.RS_NO  AND YH.REJECT_DATE IS NOT NULL ))  END END END  DOC_REJECT
        FROM (              
                SELECT  H.FG_WEEK, H.KNIT_WEEK, H.YP_ORDER RS_NO ,
                                (SELECT SO_NO FROM SF5.SF5_FQC_CUSTOMER_H WHERE FQC_NO=H.YP_ORDER) SO_NO,
                                (CASE WHEN D.YARN_COLOR ='NO-COLOR' THEN 'PIECE-DYE' ELSE 'TOP-DYE' END) ORDER_TYPE,
                                (SELECT SALE_NAME FROM SF5.DFORA_SALE S WHERE S.SALE_ID=H.SALE_ID)  SALE_NAME,
                                  (SELECT  TEAM_NAME FROM SF5.DFORA_SALE S WHERE S.SALE_ID=H.SALE_ID) TEAM_NAME,
                                  (SELECT  CUSTOMER_NAME FROM SF5.DFORA_CUSTOMER S WHERE S.CUSTOMER_ID=H.CUSTOMER_ID)  CUSTOMER_NAME, H.BUYYER END_BUYER,
                                  H.SCM_APP_DATE MS_APPROVED_DATE, 
                                  D.YARN_ITEM, D.YARN_COLOR, TO_NUMBER(LTRIM(TO_CHAR(D.NEED_DATE,'YYYYIW'))) PLANIN_WW,
                                    (SELECT yarn_in_date
                                      FROM NYF.fmit_po_detail@NYFPHET.WORLD
                                    WHERE po_no = d.po_no
                                      AND line_id = d.po_line) YARN_IN_DATE,
                                    D.YARN_QTY,  H.SCM_APP_DATE CONFIRM_PLANIN,
                                  D.PO_NO,D.PO_LINE,LTRIM(TO_CHAR(D.PO_IN_DATE,'IYYYIW')) PO_IN_WEEK,
                                  D.PO_IN_DATE , 
                                  (CASE WHEN D.SOURCING_REJ_DATE IS NOT NULL THEN 'REJECT'  ELSE  CASE WHEN H.REJECT_DATE IS NOT NULL THEN 'REJECT' END END)     SC_REJECT,
                                  ( SELECT PO_STATUS  FROM SF5.NY_PO_LINE_DISTRIBUTIONS_V NPO
                                    WHERE NPO.PO_NUMBER =d.po_no
                                    AND NPO.PO_LINE_NO =d.po_line  AND PO_STATUS = 'APPROVED') PO_STATUS
                      FROM SF5.DUMMY_PURCHASEYARN_H H, SF5.DUMMY_PURCHASEYARN_D D
                      WHERE H.YP_ORDER=D.YP_ORDER
                      AND SCM_QTY>0
          ) W   WHERE  EXTRACT(YEAR FROM MS_APPROVED_DATE) >= (to_number(to_char(sysdate,'YYYY')) - 5)
              ORDER BY CONFIRM_PLANIN,RS_NO,YARN_ITEM, YARN_COLOR, PLANIN_WW """)


  with open(r"\\172.16.0.182\yarn_center\MT Team\PR&RS&PO\RMOvsPO_967.csv", "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
    
  conn.close()
  print('Success Call 2')


class CLS_call3(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      call3()

def call3():
  oracle_client = r"C:\instantclient_19_5"
  os.environ["ORACLE_HOME"] = oracle_client
  os.environ["PATH"] = oracle_client+os.pathsep+os.environ["PATH"]
  os.environ["NLS_LANG"] = "AMERICAN_AMERICA.TH8TISASCII"
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521, sid="NYTG")
  #Replica.world
  conn = cx_Oracle.connect(user="NYIS", password="NYIS",
                          dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()


  cursor.execute(""" SELECT  ORDER_NUMBER, SO_NO, FLOW_STATUS_CODE,ORDERED_DATE, DATA_REC_TYPE, FOB_POINT_CODE, ITEM_TYPE, 
                            CUSTOMER_NUMBER,
                            ( SELECT CUSTOMER_NAME FROM SF5.DFORA_CUSTOMER C WHERE C.CUSTOMER_ID=A.CUSTOMER_NUMBER) CUSTOMER_NAME,
                            END_BUYER   ,
                            SALE_ID,
                            (SELECT SALE_NAME FROM SF5.DFORA_SALE SL WHERE SL.SALE_ID=A.SALE_ID) SALE_NAME,
                            TEAM_NAME,
                            ORDER_TYPE,
                            SALES_SEND_DATE,            
                            NVL(MANAGER_APPROVED,APPROVED_PRICE_DATE) MANAGER_APPROVED,
                            APPROVED_PRICE_DATE,
                            Get_Time_Minutes(APPROVED_PRICE_DATE,NVL(MANAGER_APPROVED,APPROVED_PRICE_DATE)) USED_TIME,
                            FLOOR(Get_Time_Minutes(APPROVED_PRICE_DATE,NVL(MANAGER_APPROVED,APPROVED_PRICE_DATE))/60) USED_HH,
                            MOD(Get_Time_Minutes(APPROVED_PRICE_DATE,NVL(MANAGER_APPROVED,APPROVED_PRICE_DATE)),60) USED_MM,
                            CREDIT_CONTROL_APPROVED,
                            Get_Time_Minutes(CREDIT_CONTROL_APPROVED,NVL(MANAGER_APPROVED,CREDIT_CONTROL_APPROVED)) CC_USED_TIME,
                            FLOOR(Get_Time_Minutes(CREDIT_CONTROL_APPROVED,NVL(MANAGER_APPROVED,CREDIT_CONTROL_APPROVED))/60) CC_USED_HH,
                            MOD(Get_Time_Minutes(CREDIT_CONTROL_APPROVED,NVL(MANAGER_APPROVED,CREDIT_CONTROL_APPROVED)),60) CC_USED_MM,
                            SPEC_APPROVED,            
                            Get_Time_Minutes(SPEC_APPROVED,NVL(MANAGER_APPROVED,SPEC_APPROVED)) SPEC_USED_TIME,
                            FLOOR(Get_Time_Minutes(SPEC_APPROVED,NVL(MANAGER_APPROVED,SPEC_APPROVED))/60) SPEC_USED_HH,
                            MOD(Get_Time_Minutes(SPEC_APPROVED,NVL(MANAGER_APPROVED,SPEC_APPROVED)),60) SPEC_USED_MM,
                            COLLAR_PRICE_APPROVED,
                            Get_Time_Minutes(COLLAR_PRICE_APPROVED,NVL(MANAGER_APPROVED,COLLAR_PRICE_APPROVED)) COLLAR_USED_TIME,
                            FLOOR(Get_Time_Minutes(COLLAR_PRICE_APPROVED,NVL(MANAGER_APPROVED,COLLAR_PRICE_APPROVED))/60) COLLAR_USED_HH,
                            MOD(Get_Time_Minutes(COLLAR_PRICE_APPROVED,NVL(MANAGER_APPROVED,COLLAR_PRICE_APPROVED)),60) COLLAR_USED_MM,
                            YARN_PO_APPROVED,
                            Get_Time_Minutes(CONFIRM_OPENPO_YARN,NVL(YARN_PO_APPROVED,NVL(MANAGER_APPROVED,YARN_PO_APPROVED))) YARN_PO_USED_TIME,
                            FLOOR(Get_Time_Minutes(CONFIRM_OPENPO_YARN,NVL(YARN_PO_APPROVED,NVL(MANAGER_APPROVED,YARN_PO_APPROVED)))/60) YARN_PO_USED_HH,
                            MOD(Get_Time_Minutes(CONFIRM_OPENPO_YARN,NVL(YARN_PO_APPROVED,NVL(MANAGER_APPROVED,YARN_PO_APPROVED))),60) YARN_PO_USED_MM,                           
                            CONFIRM_OPENPO_YARN,
                            Get_Time_Minutes(CONFIRM_OPENPO_YARN,NVL(MANAGER_APPROVED,CONFIRM_OPENPO_YARN)) CONFIRM_OPENPO_YARN_USED_TIME,
                            FLOOR(Get_Time_Minutes(CONFIRM_OPENPO_YARN,NVL(MANAGER_APPROVED,CONFIRM_OPENPO_YARN))/60) CONFIRM_OPENPO_YARN_USED_HH,
                            MOD(Get_Time_Minutes(CONFIRM_OPENPO_YARN,NVL(MANAGER_APPROVED,CONFIRM_OPENPO_YARN)),60) CONFIRM_OPENPO_YARN_USED_MM,     
                            Get_Time_Minutes(STOCK_APPROVED,NVL(MANAGER_APPROVED,STOCK_APPROVED)) STOCK_USED_TIME,
                            FLOOR(Get_Time_Minutes(STOCK_APPROVED,NVL(MANAGER_APPROVED,STOCK_APPROVED))/60) STOCK_USED_HH,
                            MOD(Get_Time_Minutes(STOCK_APPROVED,NVL(MANAGER_APPROVED,STOCK_APPROVED)),60) STOCK_USED_MM,
                            FLAT_APPROVED,
                            Get_Time_Minutes(FLAT_APPROVED,NVL(MAX_STAGE_2,FLAT_APPROVED)) FLAT_USED_TIME,
                            FLOOR(Get_Time_Minutes(FLAT_APPROVED,NVL(MAX_STAGE_2,FLAT_APPROVED))/60) FLAT_USED_HH,
                            MOD(Get_Time_Minutes(FLAT_APPROVED,NVL(MAX_STAGE_2,FLAT_APPROVED)),60) FLAT_USED_MM,            
                            KNIT_FG_APPROVED,
                            Get_Time_Minutes(KNIT_FG_APPROVED,NVL(NVL(MAX_STAGE_3,MAX_STAGE_2),KNIT_FG_APPROVED)) KNIT_USED_TIME,
                            FLOOR(Get_Time_Minutes(KNIT_FG_APPROVED, NVL(NVL(MAX_STAGE_3,MAX_STAGE_2),KNIT_FG_APPROVED))/60) KNIT_USED_HH,
                            MOD(Get_Time_Minutes(KNIT_FG_APPROVED,NVL(NVL(MAX_STAGE_3,MAX_STAGE_2),KNIT_FG_APPROVED)),60) KNIT_USED_MM,            
                            OE_APPROVED,
                            Get_Time_Minutes(OE_APPROVED,NVL(MAX_STAGE_4,OE_APPROVED)) OE_USED_TIME,
                            FLOOR(Get_Time_Minutes(OE_APPROVED,NVL(MAX_STAGE_4,OE_APPROVED))/60) OE_USED_HH,
                            MOD(Get_Time_Minutes(OE_APPROVED,NVL(MAX_STAGE_4,OE_APPROVED)),60) OE_USED_MM,
                            FG_YEAR, FG_WEEK, SALES_FG_YEAR, SALES_FG_WEEK, OE_STATUS, PRINT_SO,
                            BOOK_DATE,
                            Get_Time_Minutes(BOOK_DATE,NVL(OE_APPROVED,BOOK_DATE)) BOOK_USED_TIME,
                            FLOOR(Get_Time_Minutes(BOOK_DATE,NVL(OE_APPROVED,BOOK_DATE))/60) BOOK_USED_HH,
                            MOD(Get_Time_Minutes(BOOK_DATE,NVL(OE_APPROVED,BOOK_DATE)),60) BOOK_USED_MM ,
                            TRUNC(SYSDATE)-TRUNC(ORDERED_DATE)  AGEING_DAYS,
                            ORDER_QTY,ORDER_UOM,ORDER_QTY_KGS,
                            RU_REMARK ,EDD_YEAR2,EDD_WEEK2 , 
                            ORACLE_FG_YEAR,ORACLE_FG_WEEK,
                            KNIT_YEAR_ST, KNIT_WEEK_ST, KNIT_YEAR, KNIT_WEEK
                  FROM ( SELECT H.ORDER_NUMBER, H.ORA_ORDER_NUMBER SO_NO,H.FLOW_STATUS_CODE, H.ORDERED_DATE, H.DATA_REC_TYPE, 
                                          H.FOB_POINT_CODE, DECODE(H.ITEM_TYPE,'F','FABRIC','COLLAR') ITEM_TYPE,
                                          H.CUSTOMER_NUMBER,H.SALESREP_NUMBER SALE_ID,'TEAM_NAME', H.SALES_SEND_DATE,DUMMY_ITEM_TYPE ORDER_TYPE,
                                          (SELECT CONFIRM_DATE FROM SF5.DUMMY_CONFIRM_ORDER C WHERE C.ORDER_NUMBER=H.ORDER_NUMBER
                                          AND CONFIRM_TYPE='MANAGER APPROVED DUMMY SO' AND STEPS_ID=2) MANAGER_APPROVED,
                                          (SELECT CONFIRM_DATE FROM SF5.DUMMY_CONFIRM_ORDER C WHERE C.ORDER_NUMBER=H.ORDER_NUMBER
                                          AND CONFIRM_TYPE='C/C APPROVED DUMMY SO' AND STEPS_ID=3) CREDIT_CONTROL_APPROVED,
                                          (SELECT CONFIRM_DATE FROM SF5.DUMMY_CONFIRM_ORDER C WHERE C.ORDER_NUMBER=H.ORDER_NUMBER
                                          AND CONFIRM_TYPE='SPEC APPROVED DUMMY SO' AND STEPS_ID=4) SPEC_APPROVED,
                                          (SELECT CONFIRM_DATE FROM SF5.DUMMY_CONFIRM_ORDER C WHERE C.ORDER_NUMBER=H.ORDER_NUMBER
                                          AND STEPS_ID=5) YARN_PO_APPROVED,
                                          ( SELECT  MAX(CF.PO_IN_DATE)
                                            FROM SF5.DUMMY_YARN_PLANIN CF,SF5.DUMMY_CONFIRM_MRP MC
                                            WHERE CF.ORDER_NUMBER=MC.ORDER_NUMBER
                                            AND CF.YARN_ITEM=MC.YARN_ITEM
                                            AND CF.PL_COLOR=MC.YARN_COLOR
                                            AND NVL(MC.PO_FLAG,'N')='Y'
                                            AND MC.ORDER_NUMBER=H.ORDER_NUMBER) CONFIRM_OPENPO_YARN,
                                          (SELECT CONFIRM_DATE FROM SF5.DUMMY_CONFIRM_ORDER C WHERE C.ORDER_NUMBER=H.ORDER_NUMBER
                                            AND STEPS_ID=5.1) COLLAR_PRICE_APPROVED,            
                                          (SELECT CONFIRM_DATE FROM SF5.DUMMY_CONFIRM_ORDER C WHERE C.ORDER_NUMBER=H.ORDER_NUMBER
                                          AND STEPS_ID=6) STOCK_APPROVED,                                                            
                                          (SELECT CONFIRM_DATE FROM SF5.DUMMY_CONFIRM_ORDER C WHERE C.ORDER_NUMBER=H.ORDER_NUMBER
                                          AND STEPS_ID=7) FLAT_APPROVED,                                                            
                                          (SELECT CONFIRM_DATE FROM SF5.DUMMY_CONFIRM_ORDER C WHERE C.ORDER_NUMBER=H.ORDER_NUMBER
                                          AND STEPS_ID=8) KNIT_FG_APPROVED,                                                            
                                          (SELECT CONFIRM_DATE FROM SF5.DUMMY_CONFIRM_ORDER C WHERE C.ORDER_NUMBER=H.ORDER_NUMBER
                                          AND STEPS_ID=9) OE_APPROVED,                                                                                                                        
                                          H.APPROVED_PRICE_DATE,
                                          (SELECT MAX(CONFIRM_DATE) FROM SF5.DUMMY_CONFIRM_ORDER C WHERE C.ORDER_NUMBER=H.ORDER_NUMBER
                                          AND STEPS_STAGE='STAGE-1') MAX_STAGE_1,
                                          (SELECT MAX(CONFIRM_DATE) FROM SF5.DUMMY_CONFIRM_ORDER C WHERE C.ORDER_NUMBER=H.ORDER_NUMBER
                                          AND STEPS_STAGE='STAGE-2') MAX_STAGE_2,
                                          (SELECT MAX(CONFIRM_DATE) FROM SF5.DUMMY_CONFIRM_ORDER C WHERE C.ORDER_NUMBER=H.ORDER_NUMBER
                                          AND STEPS_STAGE='STAGE-3') MAX_STAGE_3,
                                          (SELECT MAX(CONFIRM_DATE) FROM SF5.DUMMY_CONFIRM_ORDER C WHERE C.ORDER_NUMBER=H.ORDER_NUMBER
                                          AND STEPS_STAGE='STAGE-4') MAX_STAGE_4,
                                          (SELECT TEAM_NAME FROM SF5.DFORA_SALE SL WHERE SL.SALE_ID=H.SALESREP_NUMBER)  TEAM_NAME,
                                            H.FG_YEAR, H.FG_WEEK, H.SALES_FG_YEAR, H.SALES_FG_WEEK, 
                                          (select q.oe_status from sf5.dummy_so_headers_yq q where q.order_number = h.order_number ) oe_status,
                                          (select q.print_so from sf5.dummy_so_headers_yq q where q.order_number = h.order_number ) print_so,
                                          GET_BOOK_DATE ( H.ORA_ORDER_NUMBER) BOOK_DATE ,
                                          ATTRIBUTE2 END_BUYER,
                                          ( SELECT  SUM(ORDER_PROD_QTY) FROM SF5.DUMMY_COLOR_LINES  l where l.order_number = h.order_number ) ORDER_QTY,        
                                          ( SELECT  DISTINCT (ORDER_UOM) FROM SF5.DUMMY_COLOR_LINES  l where l.order_number = h.order_number AND ROWNUM=1) ORDER_UOM,  
                                          ( SELECT  SUM(ORDERED_KGS) FROM SF5.DUMMY_COLOR_LINES  l where l.order_number = h.order_number ) ORDER_QTY_KGS,  
                                          (select max(c.CONFIRM_REF) FROM sf5.DUMMY_CONFIRM_ORDER C  where c.STEPS_ID =6 and C.ORDER_NUMBER=H.ORDER_NUMBER ) RU_REMARK  ,
                                          EDD_YEAR2,EDD_WEEK2,
                                          (SELECT CUSTOMER_YEAR FROM SF5.SMIT_SO_HEADER SO WHERE SO.SO_NO= H.ORA_ORDER_NUMBER) ORACLE_FG_YEAR,
                                          (SELECT CUSTOMER_FG FROM SF5.SMIT_SO_HEADER SO WHERE SO.SO_NO= H.ORA_ORDER_NUMBER) ORACLE_FG_WEEK,
                                              H.KNIT_YEAR_ST, H.KNIT_WEEK_ST, H.KNIT_YEAR, H.KNIT_WEEK
                            FROM SF5.DUMMY_SO_HEADERS H
                            WHERE FLOW_STATUS_CODE NOT IN ('CANCEL')
                            AND ORDERED_DATE >=add_months(TRUNC(sysdate,'YYYY'),-60) 
                            -- AND ORDERED_DATE >= TO_DATE('01/01/2015','DD/MM/RRRR')
                        ) A """)


  with open(r"\\172.16.0.182\yarn_center\MT Team\PR&RS&PO\dummyApproveTimeStep.csv", "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)

  conn.close()
  print('Success Call 3')


class CLS_call4(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      call4()
  
def call4():
  oracle_client = r"C:\instantclient_19_5"
  os.environ["ORACLE_HOME"]=oracle_client
  os.environ["PATH"]=oracle_client+os.pathsep+os.environ["PATH"]
  os.environ["NLS_LANG"]="AMERICAN_AMERICA.TH8TISASCII"
  my_dsn = cx_Oracle.makedsn("172.16.6.75",port=1521,sid="NYTG")
  #Replica.world
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()

  cursor.execute(""" SELECT PO_DATE,WORK_WEEK,YARN_SUPPLIER,PO_NO, LINE_ID, ITEM_CODE, ITEM_DESC,
                          CASE WHEN PR_DATE IS NOT NULL THEN PR_DATE ELSE
                            (SELECT MAX(CONF_POFLAG) FROM SF5.DUMMY_CONFIRM_MRP@SALESF5.WORLD
                            WHERE ORDER_NUMBER=PPA.RS_NO
                              AND YARN_ITEM= PPA.ITEM_CODE    
                              AND PO_FLAG='Y' ) END  REQUEST_DATE,
                    ORDER_QTY, REC_QTY,DECODE(NVL(ORDER_QTY,0),0,0,((REC_QTY/ORDER_QTY)*100)) PERCENT_REC, --ROLL,
                    MIN_REC_DATE,decode(nvl(YARN_PLAN_QTY,0),0,PR_QUANTITY,YARN_PLAN_QTY) YARN_PLAN_QTY, YARN_IN_DATE,
                    (NVL(TRUNC(MIN_REC_DATE),TRUNC(SYSDATE))-TRUNC(YARN_IN_DATE)) DELAY_DAYS_YARN_IN,
                    (NVL(TRUNC(MIN_REC_DATE),TRUNC(SYSDATE))-TRUNC(PO_IN_DATE)) DELAY_DAYS_PO_IN,
                    PO_IN_DATE,NEED_DATE,SO_NO,CUSTOMER_NAME,SALE_NAME ,PO_STATUS,CURRENCY_CODE,PR_NO,RS_NO,
                    UNIT_PRICE, UOM, RATE,
                        FUNC_PRDATE(PR_NO,RS_NO,ITEM_CODE) PR_START_DATE,
                        FUNC_TRDATE(PR_NO,RS_NO,ITEM_CODE) PO_TARGET_DATE,
                        FUNC_OTP(PR_NO,RS_NO,ITEM_CODE,PO_DATE) OTP_TYPE,
                        Color,Sale_man,Team,PLAN_ETD, PLAN_ETA, ESTIMATED_ETD, ESTIMATED_ETA, ACTUAL_ETA_DATE, SCM_CONFIRM_DTE
                          ,NYF.GET_YARN_REMARK (PO_NO,LINE_ID) YARN_REMARK
                          ,NYF.GET_YARN_CTNS (PO_NO,LINE_ID) CTNS
                          ,NYF.GET_YARN_SENT (PO_NO,LINE_ID) SEA_AIR
                          ,NYF.GET_YARN_INV (PO_NO,LINE_ID) INVOICE_NO
                          ,RS_REMARK
                          ,NYK_BUYER@R12INTERFACE.WORLD(PO_NO) CREATE_PO_BY , UPD_YARN_IN ,  NOTE, CONTAINER_NO
              FROM( SELECT TRUNC(P.PO_DATE) PO_DATE,P.PO_NO,D.LINE_ID,D.ITEM_CODE,D.ITEM_DESC, 
                      P.vendor_name YARN_SUPPLIER,SUM(NVL(D.TOTAL_QTY_K,0))  ORDER_QTY,
                      nvl((select SUM(NVL(B.TOTAL_WEIGHT,0)) FROM nyf.FMIT_YARN_RECEIVED B
                          where B.PO_NO = P.PO_NO
                          AND B.LINE_ID = D.LINE_ID                        
                          and not exists (SELECT NULL FROM nyf.FMIT_YARN_ISSUEDH aa
                                          WHERE aa.Issue_No = b.Iss_Iss_No
                                          and SUBSTR( aa.Requested_Type, 1, 2 ) IN ( '01', '02', '03' )) ),0)  REC_QTY, 
                    (SELECT MIN(TRUNC(RECEIVED_DATE)) 
                      FROM NYF.FMIT_YARN_RECEIVED B WHERE B.PO_NO=P.PO_NO AND B.LINE_ID=D.LINE_ID
                      and not exists (SELECT NULL FROM nyf.FMIT_YARN_ISSUEDH aa
                                      WHERE aa.Issue_No = b.Iss_Iss_No
                                      and SUBSTR( aa.Requested_Type, 1, 2 ) IN ( '01', '02', '03' ))) MIN_REC_DATE,                     
                      D.PO_IN_DATE ,D.NEED_DATE,P.ATTRIBUTE11 SO_NO,P.SALE_NAME,P.CUSTOMER_NAME ,
                      D.YARN_IN_DATE,
                      DECODE(NVL(P.CLOSED_CODE,'OPEN'),'OPEN','OPEN','ERP APPROVED','OPEN','CLOSED','CLOSED' ,'FINALLY CLOSED','CLOSED') PO_STATUS,
                      ( SELECT SUM(YARN_QTY)
                        FROM SF5.DUMMY_YARN_PLANIN_V@SALESF5.WORLD
                        WHERE PO_FLAG='PO' AND YARN_PONO=P.PO_NO AND PO_LINE=D.LINE_ID) YARN_PLAN_QTY,
                        P.CURRENCY_CODE,D.PR_NO,
                        D.ITEM_PRICE_K UNIT_PRICE, D.ITEM_UNIT_K UOM, P.RATE,
                      (SELECT MIN(RS_NO)  FROM SF5.DUMMY_YARN_PLANIN_V@SALESF5.WORLD 
                      WHERE YARN_PONO=P.PO_NO AND PO_LINE=D.LINE_ID) RS_NO,
                      ( SELECT max(CREATION_DATE) FROM PO_REQUISITION_HEADERS_ALL
                        WHERE SEGMENT1 =D.PR_NO) PR_DATE,
                        ( SELECT max(RL.ATTRIBUTE3) FROM PO_REQUISITION_LINES_ALL@R12INTERFACE.WORLD RL,
                                                                                      PO_REQUISITION_HEADERS_ALL RH
                        WHERE RH.REQUISITION_HEADER_ID = RL.REQUISITION_HEADER_ID and RH.SEGMENT1 =D.PR_NO and RL.LINE_NUM=D.LINE_ID) Color,
                      ( SELECT max(RL.ATTRIBUTE4) FROM PO_REQUISITION_LINES_ALL@R12INTERFACE.WORLD RL,
                                                                                      PO_REQUISITION_HEADERS_ALL RH
                        WHERE RH.REQUISITION_HEADER_ID = RL.REQUISITION_HEADER_ID and RH.SEGMENT1 =D.PR_NO and RL.LINE_NUM=D.LINE_ID) Sale_man,
                      ( SELECT max(RL.ATTRIBUTE5) FROM PO_REQUISITION_LINES_ALL@R12INTERFACE.WORLD RL,
                                                                                      PO_REQUISITION_HEADERS_ALL RH
                        WHERE RH.REQUISITION_HEADER_ID = RL.REQUISITION_HEADER_ID and RH.SEGMENT1 =D.PR_NO and RL.LINE_NUM=D.LINE_ID) Team    
                        ,TRUNC(PLAN_ETD) PLAN_ETD, TRUNC(PLAN_ETA) PLAN_ETA, TRUNC(ESTIMATED_ETD) ESTIMATED_ETD, TRUNC(ESTIMATED_ETA) ESTIMATED_ETA, TRUNC(ACTUAL_ETA_DATE)ACTUAL_ETA_DATE, TRUNC(SCM_CONFIRM_DTE)SCM_CONFIRM_DTE
                        ,NYIS.GET_WORKWEEK_YARN (TRUNC(P.PO_DATE)) WORK_WEEK, P.ATTRIBUTE2 RS_REMARK,
                      (SELECT sum(pr.QUANTITY)  FROM SF5.NY_PR_LINE_DISTRIBUTIONS_V@SALESF5.WORLD pr
                      WHERE pr.PR_NO = d.PR_NO) PR_QUANTITY , UPD_YARN_IN ,  NOTE, D.CONTAINER_NO                      
              FROM nyf.FMIT_PO_HEADER P,
                    nyf.FMIT_PO_DETAIL D                     
              WHERE P.PO_NO = D.PO_NO            
                and p.attribute10 like 'สั่งซื้อเส้นด้าย%'
              and EXTRACT(YEAR FROM P.PO_DATE) >= (to_number(to_char(sysdate,'YYYY')) - 5)
            GROUP BY TRUNC(P.PO_DATE),P.PO_NO,P.ATTRIBUTE11,P.SALE_NAME,P.CUSTOMER_NAME,
                    D.LINE_ID,D.ITEM_CODE,P.VENDOR_NAME,D.ITEM_DESC,D.YARN_IN_DATE,D.PO_IN_DATE,D.NEED_DATE, UPD_YARN_IN ,  NOTE,
                    P.CURRENCY_CODE,D.PR_NO,
                      D.ITEM_PRICE_K , D.ITEM_UNIT_K , P.RATE,P.ATTRIBUTE2,
                    TRUNC(PLAN_ETD) , TRUNC(PLAN_ETA) , TRUNC(ESTIMATED_ETD) , TRUNC(ESTIMATED_ETA) , TRUNC(ACTUAL_ETA_DATE), TRUNC(SCM_CONFIRM_DTE),
                    NYIS.GET_WORKWEEK_YARN (TRUNC(P.PO_DATE)),D.CONTAINER_NO,
                    DECODE(NVL(P.CLOSED_CODE,'OPEN'),'OPEN','OPEN','ERP APPROVED','OPEN','CLOSED','CLOSED' ,'FINALLY CLOSED','CLOSED')) PPA
            WHERE 1 = 1
              ORDER BY 1,2,3 """)


  with open(r"\\172.16.0.182\yarn_center\MT Team\PR&RS&PO\YarnReceive_830.csv", "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
    
  conn.close()
  
  print('Success Call 4')


class CLS_call5(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      call5()


def call5():
  oracle_client = r"C:\instantclient_19_5"
  os.environ["ORACLE_HOME"] = oracle_client
  os.environ["PATH"] = oracle_client+os.pathsep+os.environ["PATH"]
  os.environ["NLS_LANG"] = "AMERICAN_AMERICA.TH8TISASCII"
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521, sid="NYTG")
  #Replica.world
  conn = cx_Oracle.connect(user="NYIS", password="NYIS",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  cursor.execute(""" SELECT FG_WEEK,KNIT_WEEK_END KNIT_WEEK,  LTRIM(TO_CHAR(RS_NO)) RS_NO ,SO_NO ,
               DUMMY_ITEM_TYPE ORDER_TYPE, SALE_NAME, TEAM_NAME, CUSTOMER_NAME, END_BUYER,
               MS_APPROVED_DATE,  
               V.ITEM_CODE, V.YARN_CONTAIN,
               YARN_ITEM, YARN_COLOR, PLANIN_WW, NVL(PO_SHIPPMENT,PLANIN_DATE)  YARN_IN_DATE, YARN_QTY, 
               CONFIRM_PLANIN,--CONFIRM_PLANIN_BY,
               YARN_PONO, PO_LINE,
                LTRIM(TO_CHAR(PO_IN_DATE,'IYYYIW')) PO_IN_WEEK, PO_IN_DATE,--,CONFIRM_DATE
               TRUNC(NVL(PO_SHIPPMENT,PLANIN_DATE))-NVL(TRUNC(PO_IN_DATE),TRUNC(SYSDATE)) DIF,
               (SELECT FLOW_STATUS_CODE FROM SF5.DUMMY_SO_HEADERS DH  WHERE DH.ORDER_NUMBER=V.RS_NO) RS_STATUS,
               (SELECT CREATION_DATE  FROM SF5.DUMMY_SO_HEADERS  WHERE ORDER_NUMBER=V.RS_NO) CREATE_DATE,
               (SELECT DISTINCT  PO_STATUS  FROM SF5.NY_PO_LINE_DISTRIBUTIONS_V NPO WHERE NPO.PO_NUMBER =V.YARN_PONO
                                   AND NPO.PO_LINE_NO =V.PO_LINE) ORA_PO_STATUS,V.MRP_QTY, V.YARN_COMMENT
        FROM SF5.DUMMY_YARN_PLANIN_VITM V
        WHERE PO_FLAG='PO' 
        AND MS_APPROVED_DATE >= add_months(TRUNC(sysdate,'YYYY'),-60) 
        -- AND MS_APPROVED_DATE >= NVL(TO_DATE('01/01/2015','DD/MM/RRRR'),MS_APPROVED_DATE)+0.00000
        UNION
        SELECT LTRIM(TO_CHAR(FG_WEEK)) FG_WEEK, LTRIM(TO_CHAR(KNIT_WEEK)) KNIT_WEEK, RS_NO, SO_NO, 
               ORDER_TYPE, SALE_NAME, TEAM_NAME, CUSTOMER_NAME,END_BUYER,
              MS_APPROVED_DATE, 
              ITEM_CODE, 0 YARN_CONTAIN,
              YARN_ITEM, YARN_COLOR, PLANIN_WW, YARN_IN_DATE,YARN_QTY, CONFIRM_PLANIN,
              PO_NO, PO_LINE, PO_IN_WEEK, PO_IN_DATE,
              TRUNC(NVL(YARN_IN_DATE,CONFIRM_PLANIN))-NVL(TRUNC(PO_IN_DATE),TRUNC(SYSDATE))  DIF,
              ( CASE WHEN PO_NO IS NULL AND SC_REJECT IS NULL THEN 'OPEN' ELSE
                  CASE WHEN PO_NO IS NOT NULL  AND PO_STATUS  IS NULL THEN 'IN-PROCESS' ELSE 
                      CASE WHEN PO_NO IS NOT NULL  AND PO_STATUS  IS NOT NULL THEN 'APPROVED' ELSE  SC_REJECT END END END ) DOC_REJECT,
                      (SELECT  OPEN_DATE  FROM SF5.DUMMY_PURCHASEYARN_H  WHERE YP_ORDER=RS_NO) CREATE_DATE,
                      (SELECT DISTINCT  PO_STATUS  FROM SF5.NY_PO_LINE_DISTRIBUTIONS_V NPO WHERE NPO.PO_NUMBER =w.po_no
                                   AND NPO.PO_LINE_NO =w.po_line )ORA_PO_STATUS,MRP_QTY, null YARN_COMMENT
       FROM (              
               SELECT  H.FG_WEEK, H.KNIT_WEEK, H.YP_ORDER RS_NO ,
                              (SELECT SO_NO FROM SF5.SF5_FQC_CUSTOMER_H WHERE FQC_NO=H.YP_ORDER) SO_NO,
                              (CASE WHEN D.YARN_COLOR ='NO-COLOR' THEN 'PIECE-DYE' ELSE 'TOP-DYE' END) ORDER_TYPE,
                               (SELECT SALE_NAME FROM SF5.DFORA_SALE S WHERE S.SALE_ID=H.SALE_ID)  SALE_NAME,
                                (SELECT  TEAM_NAME FROM SF5.DFORA_SALE S WHERE S.SALE_ID=H.SALE_ID) TEAM_NAME,
                                (SELECT  CUSTOMER_NAME FROM SF5.DFORA_CUSTOMER S WHERE S.CUSTOMER_ID=H.CUSTOMER_ID)  CUSTOMER_NAME, H.BUYYER END_BUYER,
                                 H.SCM_APP_DATE MS_APPROVED_DATE, D.ITEM_CODE,
                                 D.YARN_ITEM, D.YARN_COLOR, TO_NUMBER(LTRIM(TO_CHAR(D.NEED_DATE,'YYYYIW'))) PLANIN_WW,
                                  (SELECT yarn_in_date
                                    FROM NYF.fmit_po_detail@NYFPHET.WORLD
                                   WHERE po_no = d.po_no
                                     AND line_id = d.po_line) YARN_IN_DATE,
                                  D.YARN_QTY,  H.SCM_APP_DATE CONFIRM_PLANIN,
                                 D.PO_NO,D.PO_LINE,LTRIM(TO_CHAR(D.PO_IN_DATE,'IYYYIW')) PO_IN_WEEK,
                                 D.PO_IN_DATE , 
                                 (CASE WHEN D.SOURCING_REJ_DATE IS NOT NULL THEN 'REJECT'  ELSE  CASE WHEN H.REJECT_DATE IS NOT NULL THEN 'REJECT' END END)     SC_REJECT,
                                 ( SELECT PO_STATUS  FROM SF5.NY_PO_LINE_DISTRIBUTIONS_V NPO
                                   WHERE NPO.PO_NUMBER =d.po_no
                                   AND NPO.PO_LINE_NO =d.po_line  AND PO_STATUS = 'APPROVED') PO_STATUS, D.SCM_QTY MRP_QTY
                    FROM SF5.DUMMY_PURCHASEYARN_H H, SF5.DUMMY_PURCHASEYARN_D D
                    WHERE H.YP_ORDER=D.YP_ORDER
                    AND SCM_QTY>0
        ) W  
        WHERE  MS_APPROVED_DATE >= add_months(TRUNC(sysdate,'YYYY'),-60) 
        -- WHERE  MS_APPROVED_DATE >= NVL(TO_DATE('01/01/2015','DD/MM/RRRR'),MS_APPROVED_DATE)+0.00000
        ORDER BY CONFIRM_PLANIN,RS_NO,YARN_ITEM, YARN_COLOR, PLANIN_WW """)

  with open(r"\\172.16.0.182\yarn_center\MT Team\PR&RS&PO\VerifyOpenRMOvsOpenPOItem_1829.csv", "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)

  conn.close()

  print('Success Call 5')



class CLS_call6(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      call6()


def call6():
  oracle_client = r"C:\instantclient_19_5"
  os.environ["ORACLE_HOME"] = oracle_client
  os.environ["PATH"] = oracle_client+os.pathsep+os.environ["PATH"]
  os.environ["NLS_LANG"] = "AMERICAN_AMERICA.TH8TISASCII"
  my_dsn = cx_Oracle.makedsn("172.16.6.75", port=1521, sid="NYTG")
  #PHET.world
  conn = cx_Oracle.connect(user="NYIS", password="NYIS",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  cursor.execute("""  SELECT PO_DATE,YARN_SUPPLIER,PO_NO, LINE_ID, ITEM_CODE, ITEM_DESC,
                 CASE WHEN PR_DATE IS NOT NULL THEN PR_DATE ELSE
                    (SELECT MAX(CONF_POFLAG) FROM SF5.DUMMY_CONFIRM_MRP@SALESF5.WORLD
                     WHERE ORDER_NUMBER=PPA.RS_NO
                     AND YARN_ITEM= PPA.ITEM_CODE    
                     AND PO_FLAG='Y' ) END  REQUEST_DATE,
                   ORDER_QTY, REC_QTY,DECODE(NVL(ORDER_QTY,0),0,0,((REC_QTY/ORDER_QTY)*100)) PERCENT_REC, --ROLL,
                   MIN_REC_DATE,YARN_PLAN_QTY, YARN_IN_DATE,
                   (NVL(TRUNC(MIN_REC_DATE),TRUNC(SYSDATE))-TRUNC(YARN_IN_DATE)) DELAY_DAYS_YARN_IN,
                   (NVL(TRUNC(MIN_REC_DATE),TRUNC(SYSDATE))-TRUNC(PO_IN_DATE)) DELAY_DAYS_PO_IN,
                   PO_IN_DATE,NEED_DATE,SO_NO,CUSTOMER_NAME,SALE_NAME ,PO_STATUS,CURRENCY_CODE,PR_NO,RS_NO,
                      FUNC_PRDATE(PR_NO,RS_NO,ITEM_CODE) PR_START_DATE,
                      FUNC_TRDATE(PR_NO,RS_NO,ITEM_CODE) PO_TARGET_DATE,
                      FUNC_OTP(PR_NO,RS_NO,ITEM_CODE,PO_DATE) OTP_TYPE, CONTAINER_NO
            FROM( SELECT TRUNC(P.PO_DATE) PO_DATE,P.PO_NO,D.LINE_ID,D.ITEM_CODE,D.ITEM_DESC, 
                    P.vendor_name YARN_SUPPLIER,SUM(NVL(D.TOTAL_QTY_K,0))  ORDER_QTY,
                    nvl( (SELECT SUM(DECODE(SUBSTR(ITEM_CODE,1,1),'F',TOTAL_WEIGHT,TOTAL_DOZ)) 
                           FROM SF5.GFCV_WAREHOUSE@SALESF5.WORLD GF 
                           WHERE GF.PO_NO=P.PO_NO AND ITEM_CODE=D.ITEM_CODE AND GF.PO_LINE=D.LINE_ID),0)  REC_QTY, 
                   (SELECT MIN(TRUNC(RECEIVE_DATE)) FROM SF5.GFCV_WAREHOUSE@SALESF5.WORLD GF 
                    WHERE GF.PO_NO=P.PO_NO AND ITEM_CODE=D.ITEM_CODE) MIN_REC_DATE,                     
                    D.PO_IN_DATE ,D.NEED_DATE,P.ATTRIBUTE11 SO_NO,P.SALE_NAME,P.CUSTOMER_NAME ,
                    D.YARN_IN_DATE,
                    DECODE(NVL(P.CLOSED_CODE,'OPEN'),'OPEN','OPEN','ERP APPROVED','OPEN','CLOSED','CLOSED' ,'FINALLY CLOSED','CLOSED') PO_STATUS,
                    ( SELECT SUM(YARN_QTY)
                      FROM SF5.DUMMY_YARN_PLANIN_V@SALESF5.WORLD
                      WHERE PO_FLAG='PO' AND YARN_PONO=P.PO_NO AND PO_LINE=D.LINE_ID) YARN_PLAN_QTY,
                      P.CURRENCY_CODE,D.PR_NO,
                    (SELECT MIN(RS_NO)  FROM SF5.DUMMY_YARN_PLANIN_V@SALESF5.WORLD 
                     WHERE YARN_PONO=P.PO_NO AND PO_LINE=D.LINE_ID) RS_NO,
                     ( SELECT CREATION_DATE FROM PO_REQUISITION_HEADERS_ALL
                      WHERE SEGMENT1 =D.PR_NO) PR_DATE, D.CONTAINER_NO
             FROM nyf.FMIT_PO_HEADER P,
                  nyf.FMIT_PO_DETAIL D                     
             WHERE P.PO_NO = D.PO_NO            
             and p.attribute10 like 'สั่งซื้อผ้า%'
                 and TRUNC(P.PO_DATE) >= nvl(to_date('01/01/2018','DD/MM/RRRR'),TRUNC(P.PO_DATE))
            GROUP BY TRUNC(P.PO_DATE),P.PO_NO,P.ATTRIBUTE11,P.SALE_NAME,P.CUSTOMER_NAME,
                   D.LINE_ID,D.ITEM_CODE,P.VENDOR_NAME,D.ITEM_DESC,D.YARN_IN_DATE,D.PO_IN_DATE,D.NEED_DATE,
                   P.CURRENCY_CODE,D.PR_NO,D.CONTAINER_NO,
                   DECODE(NVL(P.CLOSED_CODE,'OPEN'),'OPEN','OPEN','ERP APPROVED','OPEN','CLOSED','CLOSED' ,'FINALLY CLOSED','CLOSED')) PPA
          WHERE 1=1
       ORDER BY 1,2,3 """)

  with open(r"\\172.16.0.182\yarn_center\MT Team\PR&RS&PO\VerifyFabricReceivePO_1193.csv", "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)

  conn.close()

  print('Success Call 6')

#############################################
threads = []

thread1 = CLS_call1()
thread1.start()
threads.append(thread1)

thread2 = CLS_call2()
thread2.start()
threads.append(thread2)

thread3 = CLS_call3()
thread3.start()
threads.append(thread3)

thread4 = CLS_call4()
thread4.start()
threads.append(thread4)

thread5 = CLS_call5()
thread5.start()
threads.append(thread5)

thread6 = CLS_call6()
thread6.start()
threads.append(thread6)


for t in threads:
    t.join()
print("COMPLETE")
