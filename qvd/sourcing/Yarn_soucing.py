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
class CLS_DATA_Yarn_Received_PO_830(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_Yarn_Received_PO_830()


def DATA_Yarn_Received_PO_830():
  my_dsn = cx_Oracle.makedsn("172.16.6.75", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_Yarn_Received_PO_830")
  sql =""" select PO_DATE,PO_NO,LINE_ID,YARN_SUPPLIER,ITEM_CODE,ITEM_DESC,REQUEST_DATE,YARN_IN_DATE,YARN_PLAN_QTY,
                  PO_IN_DATE,NEED_DATE,ORDER_QTY,REC_QTY,UNIT_PRICE,UOM,CURRENCY_CODE,RATE,MIN_REC_DATE,(nvl(ORDER_QTY,0) - nvl(REC_QTY,0)) PENDING_PO,
                  (nvl(ORDER_QTY,0) - nvl(YARN_PLAN_QTY,0)) PENDDING_YARNINREC,
                  PERCENT_REC,DELAY_DAYS_YARN_IN,DELAY_DAYS_PO_IN,SO_NO,CUSTOMER_NAME,SALE_NAME,PR_NO,RS_NO,PO_STATUS,PR_START_DATE,
                  PO_TARGET_DATE,OTP_TYPE,COLOR,SALE_MAN,TEAM,ORACLE_PO_STATUS,CREATE_PO_BY,UPD_YARN_IN,NOTE,WORK_WEEK,PLAN_ETD,PLAN_ETA,
                  ESTIMATED_ETD,ESTIMATED_ETA,ACTUAL_ETA_DATE,SCM_CONFIRM_DTE,YARN_REMARK,CTNS,SEA_AIR,INVOICE_NO,RS_REMARK,BUYYER BUYER,CONTAINER_NO,COLOR_TD,
                  sf5.get_po_customer@salesf5.world(so_no,rs_no,RS_REMARK) customer_po        
            from po_daily_830
        """

  _filename = r"C:\Qlikview_Report\YARN INVENTORY\DATA_Yarn_Received_PO_830.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_Yarn_Received_PO_830")
  sendLine("COMPLETE CLS_DATA_Yarn_Received_PO_830")

#############################################

###########################################
class CLS_DATA_Yarn_Received_PO_830_detail(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_Yarn_Received_PO_830_detail()


def DATA_Yarn_Received_PO_830_detail():
  my_dsn = cx_Oracle.makedsn("172.16.6.75", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_Yarn_Received_PO_830_detail")
  sql ="""   SELECT PO_DATE,YARN_SUPPLIER,PO_NO, LINE_ID, ITEM_CODE, ITEM_DESC,
                      CASE WHEN PR_DATE IS NOT NULL THEN PR_DATE ELSE
                          (SELECT MAX(CONF_POFLAG) FROM SF5.DUMMY_CONFIRM_MRP@SALESF5.WORLD
                           WHERE ORDER_NUMBER=PPA.RS_NO
                            AND YARN_ITEM= PPA.ITEM_CODE    
                            AND PO_FLAG='Y' ) END  REQUEST_DATE,
                  RECEIVED_DATE,
                  UNIT_PRICE,UOM,CURRENCY,RATE,PACKING_NO,RECEIVED_NO,YARN_LOT,
                  ORDER_QTY,
                  REC_QTY,
                  ROLL,
                  (TRUNC(SYSDATE)-TRUNC(RECEIVED_DATE)) REMAIN,
                  BUYYER, COLOR_TD, LOCATOR_ID
           FROM(
               SELECT TRUNC(P.PO_DATE) PO_DATE,
                   P.PO_NO,
                   TRUNC(B.RECEIVED_DATE) RECEIVED_DATE,
                   D.LINE_ID,
                   D.ITEM_CODE,
                   D.ITEM_DESC,
                   B.PACKING_NO,
                   B.RECEIVED_NO,
                   P.VENDOR_NAME YARN_SUPPLIER,
                   P.CURRENCY_CODE CURRENCY,
                   D.ITEM_UNIT_K UOM,
                   P.RATE,
                   D.ITEM_PRICE_K UNIT_PRICE,
                   B.YARN_LOT, 
                   nvl(D.TOTAL_QTY_K,0)  ORDER_QTY,
                    sum(NVL(B.TOTAL_WEIGHT,0)) REC_QTY,
                    sum(NVL(B.TOTAL_YARN,0)) ROLL,
                    (SELECT MIN(TRUNC(RECEIVED_DATE)) 
                     FROM NYF.FMIT_YARN_RECEIVED B WHERE B.PO_NO=P.PO_NO AND B.LINE_ID=D.LINE_ID) MIN_REC_DATE,
                    ( SELECT SUM(YARN_QTY)
                      FROM SF5.DUMMY_YARN_PLANIN_V@SALESF5.WORLD
                      WHERE PO_FLAG='PO' AND YARN_PONO=P.PO_NO AND PO_LINE=D.LINE_ID) YARN_PLAN_QTY,
                      D.YARN_IN_DATE, D.PO_IN_DATE  ,D.NEED_DATE,P.ATTRIBUTE11 SO_NO,P.SALE_NAME,P.CUSTOMER_NAME ,D.PR_NO ,
                      (SELECT MIN(RS_NO)  FROM SF5.DUMMY_YARN_PLANIN_V@SALESF5.WORLD 
                     WHERE YARN_PONO=P.PO_NO AND PO_LINE=D.LINE_ID) RS_NO,
                     ( SELECT max(CREATION_DATE) FROM PO_REQUISITION_HEADERS_ALL
                      WHERE SEGMENT1 =D.PR_NO) PR_DATE,
                     (SELECT max((case when nvl(LPO.CANCEL_FLAG,'N') <> 'Y' then nvl((decode(LPO.closed_code,'FINALLY CLOSED','CLOSED',LPO.closed_code)),'OPEN') else 'CANCEL' end)) 
                            FROM PO_HEADERS_ALL@R12INTERFACE.WORLD RPO,
                                 PO_LINES_ALL@R12INTERFACE.WORLD LPO 
                            WHERE RPO.SEGMENT1 = P.PO_NO
                              and rpo.po_header_id = lpo.po_header_id
                              and LPO.line_num = D.LINE_ID) PO_STATUS,  
                      P.ATTRIBUTE2 RS_REMARK, P.BUYYER,D.ATTRIBUTE8 COLOR_TD, B.LOCATOR_ID
                 FROM nyf.FMIT_PO_HEADER P,
                      nyf.FMIT_YARN_RECEIVED B, 
                      nyf.FMIT_PO_DETAIL D                      
                 WHERE P.PO_NO = D.PO_NO
                   AND D.PO_NO = B.PO_NO(+)
                   AND D.LINE_ID = B.LINE_ID(+)
                 and not exists (SELECT NULL
                                 FROM nyf.FMIT_YARN_ISSUEDH aa
                                 WHERE aa.Issue_No = b.Iss_Iss_No
                                 and SUBSTR( aa.Requested_Type, 1, 2 ) IN ( '01', '02', '03' ))
                and p.attribute10 like 'สั่งซื้อเส้นด้าย%'
                and nvl(D.CANCEL_FLAG,'N') = 'N'               
           GROUP BY TRUNC(P.PO_DATE),
                   P.PO_NO,
                   TRUNC(B.RECEIVED_DATE),
                   D.LINE_ID,
                   D.ITEM_CODE,
                   D.ITEM_DESC,
                   B.PACKING_NO,
                   B.RECEIVED_NO,
                   P.VENDOR_NAME,
                   P.CURRENCY_CODE,
                   D.ITEM_UNIT_K,
                   P.RATE,
                   D.ITEM_PRICE_K,
                   nvl(D.TOTAL_QTY_K,0), P.BUYYER,
                   B.YARN_LOT,D.YARN_IN_DATE, D.PO_IN_DATE,D.NEED_DATE,P.ATTRIBUTE11,P.SALE_NAME,P.CUSTOMER_NAME  ,D.PR_NO,P.ATTRIBUTE2,
                   D.ATTRIBUTE8,
                   p.org_id,
                   B.LOCATOR_ID
                   ) PPA
          ORDER BY 1,2,3  """

  _filename = r"C:\Qlikview_Report\YARN INVENTORY\DATA_Yarn_Received_PO_830_detail.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_Yarn_Received_PO_830_detail")
  sendLine("COMPLETE CLS_DATA_Yarn_Received_PO_830_detail")

#############################################

###########################################
class CLS_DATA_Yarn_aging_Received_797(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_Yarn_aging_Received_797()


def DATA_Yarn_aging_Received_797():
  my_dsn = cx_Oracle.makedsn("172.16.6.75", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_Yarn_aging_Received_797")
  sql =""" SELECT ITEM_CODE,YARN_LOT,M.TYPE YARN_TYPE, M.YARN_GROUP,M.YARN_MATERIAL,
            LOCATOR_ID,
            RECEIVE_DATE,RECEIVED_NO, 
            A.PO_NO, 
            (SELECT RECEIVED_GROUP FROM NYF_YARN_RECGROUP WHERE RECEIVED_TYPE=A.RECEIVED_TYPE) RECEIVED_GROUP,
                    RECEIVED_TYPE, SUPLIER_NAME, VENDOR_TYPE, 
                    SUM(WEIGHT) TOTAL_WEIGHT,
                    SUM(TOTAL_YARN) TOTAL_YARN, 
                    ROUND(SUM(WEIGHT)/DECODE(SUM(TOTAL_YARN),0,1,SUM(TOTAL_YARN)),2) YARN_WEIGHT,
                    SUM(TOTAL_AMOUNT) TOTAL_AMOUNT,
                    AGEING_DAYS,
                    GROUP_AGEING ,
                    PO.SALE_ID ,PO.SALE_NAME, PO.TEAM_NAME,
                    PO.CUSTOMER_ID, PO.CUSTOMER_NAME, PO.BUYYER,
                    PO.SO_NO,
                    ( CASE WHEN  LOCATOR_ID  IN ( 'DEFECT', 'DEFECT01', 'DEFECT02', 'DEV10', 'NOC', 'NYTY01', 'OM00', 'OM001', 'OM01', 'OM02',
                                                                    'OM15', 'OM20', 'OM30' , 'OM-ทอปก01', 'OM-ทอปก02', 'OM-ทอปก03', 'OM-ทอปก04', 
                                                                    'OM-ทอปก05', 'OM-ทอปก06', 'REDEVELOP', 'Scrap', 'SCRAP1', 'SCRAP2', 'SCRAP3',
                                                                    'scrap4', 'scrap5', 'SCRAP6', 'SCRAP7', 'SCRAP8', 'จ้างทอ', 'รอขาย1', 'รอขาย2', 'รอขาย3',
                                                                    'รอขาย4' , 'รับปก', 'รอขาย5','รอขาย6','A03','A06','AA09') THEN   'Locator Dead' 
                   ELSE
                       (CASE WHEN TO_NUMBER(LTRIM(TO_CHAR(RECEIVE_DATE,'IYYY'))) < TO_NUMBER(LTRIM(TO_CHAR(SYSDATE,'IYYY')))-2 THEN  'Aging > 2 Year' 
                             ELSE
                                  NYF.CHECK_DEAD2( BAR_TYPE,ITEM_CODE, YARN_LOT) END ) END ) STOCK_GROUP,
                  ( SELECT REASON_CODE FROM NYF.FMIT_YARN_STOCK S WHERE S.RECEIVED_NO=A.RECEIVED_NO) DEAD_REASON ,
                  AGING_ACTION ,AGING_ACCOUNT,AGING_REASON ,AGING_ACTION_Y ,AGING_ACTION_W,PACKING_NO ,
                  QA_REMARK,  QA_REMARK_DATE, UNLOCKED_REMARK, UNLOCKED_REMARK_DATE,REMARK_BOOK,
                  max((SELECT PD.ATTRIBUTE8 FROM  nyf.FMIT_PO_DETAIL PD  WHERE PD.PO_NO= A.PO_NO AND PD.item_code = A.ITEM_CODE and pd.LINE_ID = A.LINE_ID)) COLOR_TD 
          FROM
             (                      
               SELECT R.YARN_ITEM ITEM_CODE,R.YARN_LOT,R.UNIT_PRICE_1 UNIT_PRICE,R.RECEIVED_NO,
               DECODE(SUBSTR(R.YARN_BARCODE,1,1),'R','RETURN','NEW') BAR_TYPE,
               R.PO_NO, 
               R.LOCATOR_ID,R.VENDOR_NAME SUPLIER_NAME,
               R.VENDOR_TYPE,
               R.RECEIVED_TYPE,
               TRUNC(R.RECEIVED_DATE) RECEIVE_DATE,
               TRUNC(R.YARN_CLOSED_DATE) YARN_CLOSED_DATE,
               SUM(MASTER_NYF_INVENTORY_ONHAND (TRUNC(sysdate), TRUNC(R.YARN_CLOSED_DATE), TOTAL_WEIGHT)) WEIGHT,
               SUM(MASTER_NYF_INVENTORY_ONHAND (TRUNC(sysdate), TRUNC(R.YARN_CLOSED_DATE), TOTAL_YARN)) TOTAL_YARN,
               SUM(TOTAL_WEIGHT) TOTAL_WEIGHT,
               SUM(MASTER_NYF_INVENTORY_ONHAND (TRUNC(sysdate), TRUNC(R.YARN_CLOSED_DATE), ROUND((R.TOTAL_WEIGHT*R.UNIT_PRICE_1),2) )) TOTAL_AMOUNT,
               TRUNC(sysdate)-TRUNC(R.RECEIVED_DATE)  AGEING_DAYS,
               CASE WHEN TRUNC(sysdate)-TRUNC(R.RECEIVED_DATE) > -1 AND TRUNC(sysdate)-TRUNC(R.RECEIVED_DATE) <= 3 THEN  '<= 3' 
                     WHEN TRUNC(sysdate)-TRUNC(R.RECEIVED_DATE) > 3  AND TRUNC(sysdate)-TRUNC(R.RECEIVED_DATE) <= 7 THEN  '>4 <= 7' 
                     WHEN TRUNC(sysdate)-TRUNC(R.RECEIVED_DATE) > 7  AND TRUNC(sysdate)-TRUNC(R.RECEIVED_DATE) <= 14 THEN '>7 <=14'  
                     WHEN TRUNC(sysdate)-TRUNC(R.RECEIVED_DATE) > 14 AND TRUNC(sysdate)-TRUNC(R.RECEIVED_DATE) <= 21 THEN '>14 <=21'  
                     WHEN TRUNC(sysdate)-TRUNC(R.RECEIVED_DATE) > 21 AND TRUNC(sysdate)-TRUNC(R.RECEIVED_DATE) <= 30 THEN  '>21 <=30'
                     WHEN TRUNC(sysdate)-TRUNC(R.RECEIVED_DATE) > 30 AND TRUNC(sysdate)-TRUNC(R.RECEIVED_DATE) <= 60 THEN  '>30 <=60' 
                     WHEN TRUNC(sysdate)-TRUNC(R.RECEIVED_DATE) > 60 THEN  '>60' 
               END GROUP_AGEING ,AGING_ACTION ,AGING_ACCOUNT,AGING_REASON ,AGING_ACTION_Y ,AGING_ACTION_W, R.PACKING_NO  ,
               R.QA_REASON QA_REMARK,  TRUNC(R.QA_DATE) QA_REMARK_DATE, R.UNLOCKED_REM UNLOCKED_REMARK,
               TRUNC(R.UNLOCKED_DATE) UNLOCKED_REMARK_DATE, R.REMARK_BOOK, R.LINE_ID
          FROM  NYF.FMIT_YARN_RECEIVED R , NYF.MASTER_YARN_ITEM M
          WHERE R.YARN_ITEM=M.YARN_CODE 
            AND R.YARN_CLOSED<>'C' 
            AND nvl(R.YARN_CLOSED,'N') = 'N'    
            AND R.RECEIVED_TYPE IN ( SELECT  RECEIVED_TYPE  FROM NYF_YARN_RECGROUP YG
                                      WHERE YG.RECEIVED_TYPE=R.RECEIVED_TYPE)  
          GROUP BY R.YARN_ITEM ,R.YARN_LOT,R.UNIT_PRICE_1,R.RECEIVED_NO,
                  DECODE(SUBSTR(R.YARN_BARCODE,1,1),'R','RETURN','NEW') ,
                  R.PO_NO,R.PACKING_NO, R.LOCATOR_ID,R.VENDOR_NAME ,
                  R.VENDOR_TYPE,R.RECEIVED_TYPE, TRUNC(R.RECEIVED_DATE) ,TRUNC(R.YARN_CLOSED_DATE),
                  AGING_ACTION ,AGING_ACCOUNT,AGING_REASON ,AGING_ACTION_Y ,AGING_ACTION_W ,
                  R.QA_REASON,  TRUNC(R.QA_DATE), R.UNLOCKED_REM, TRUNC(R.UNLOCKED_DATE),R.REMARK_BOOK, R.LINE_ID
           ) A, NYF.MASTER_YARN_ITEM M ,
           (SELECT DISTINCT PO_NO,
                   SALE,SO_NO,SALE_ID,
                   SALE_NAME,TEAM_NAME, CUSTOMER_ID,CUSTOMER_NAME, BUYYER 
            FROM 
           ( SELECT PO_NO,
                   ATTRIBUTE15 SALE ,ATTRIBUTE11 SO_NO,
                   SALE_ID, SALE_NAME, TEAM_NAME, CUSTOMER_ID, CUSTOMER_NAME, BUYYER
             FROM NYF.FMIT_PO_HEADER
             UNION
             SELECT DISTINCT KP_NO,
                    KNIT_SALE_NAME,  LTRIM(TO_CHAR(KNIT_MC_SO)) SO_NO, 
                    TO_NUMBER(KNIT_SALE_ID) SALES_ID, KNIT_SALE_NAME SALE_NAME,
                    SL.TEAM_NAME, KNIT_CUS_ID CUSTOMER_ID, KNIT_CUS_NAME CUSTOMER_NAME, NULL BUYYER         
             FROM SF5.DFIT_KNIT_SOKP@SALESF5.WORLD K , SF5.DFORA_SALE@SALESF5.WORLD SL
             WHERE TO_NUMBER(KNIT_SALE_ID)=SL.SALE_ID(+)
             UNION
             SELECT DISTINCT RC.PO_NO ,
                    SL.SALE_NAME, RC.PO_NO, SL.SALE_ID,SL.SALE_NAME,SL.TEAM_NAME,
                    CS.CUSTOMER_ID, CS.CUSTOMER_NAME, NULL BUYER
             FROM NYF.FMIT_YARN_STOCK RC, sf5.SMIT_SO_HEADER@SALESF5.WORLD  SO,
                  sf5.dfora_sale@SALESF5.WORLD SL , sf5.dfora_customer@SALESF5.WORLD CS
             WHERE RC.PO_NO=LTRIM(TO_CHAR(SO.SO_NO))
             AND SO.SALE_ID=SL.SALE_ID
             AND SO.CUSTOMER_ID=CS.NYF_CUS_ID
             AND SUBSTR(RC.PO_NO,1,1) IN ('6','7') 
             UNION
             SELECT KP_NO,
                    F.SALE_NAME, LTRIM(TO_CHAR(SO_NO)) SO_NO, 
                    TO_NUMBER(F.SALE_ID), F.SALE_NAME, SL.TEAM_NAME,CUSTOMER_ID, CUSTOMER_NAME, NULL BUYER
             FROM sf5.FMIT_KP_PO_HEADER@SALESF5.WORLD F, sf5.dfora_sale@SALESF5.WORLD SL
             WHERE F.SALE_ID=SL.SALE_ID ) WHERE SALE IS NOT NULL ) PO
      WHERE A.ITEM_CODE=M.YARN_CODE 
         AND A.PO_NO=PO.PO_NO(+)           
      GROUP BY  ITEM_CODE,YARN_LOT,M.TYPE , M.YARN_GROUP,M.YARN_MATERIAL,
              LOCATOR_ID,
              RECEIVE_DATE,RECEIVED_NO, PACKING_NO,
              A.PO_NO,RECEIVED_TYPE, SUPLIER_NAME, VENDOR_TYPE, 
              GROUP_AGEING,
              PO.SALE_ID ,PO.SALE_NAME, PO.TEAM_NAME,
              PO.CUSTOMER_ID, PO.CUSTOMER_NAME, PO.BUYYER,
              PO.SO_NO,BAR_TYPE, AGEING_DAYS,AGING_ACTION ,AGING_ACCOUNT,AGING_REASON ,AGING_ACTION_Y ,AGING_ACTION_W,
              QA_REMARK,  QA_REMARK_DATE, UNLOCKED_REMARK, UNLOCKED_REMARK_DATE,REMARK_BOOK
      ORDER BY 1,2  """

  _filename = r"C:\Qlikview_Report\YARN INVENTORY\DATA_Yarn_aging_Received_797.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_Yarn_aging_Received_797")
  sendLine("COMPLETE CLS_DATA_Yarn_aging_Received_797")

#############################################

###########################################
class CLS_DATA_RM_YARN_ISSUE_DETAIL(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_RM_YARN_ISSUE_DETAIL()


def DATA_RM_YARN_ISSUE_DETAIL():
  my_dsn = cx_Oracle.makedsn("172.16.6.75", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_RM_YARN_ISSUE_DETAIL")

  # nyf.get_vendor_yarn(b.requested_type,b.rm_job_no) Vendor,
  #  nyf.get_kp_no_yarn(b.rm_job_no) KP_NO,
  # 
  # 

  sql =""" SELECT b.requested_type,Get_Grtype(NULL,b.movement_account) gr_type,
                      b.issued_Date, b.rm_job_no,
                      b.yarn_item, b.yarn_lot,b.bu_ASSET,b.BU_ISSUED,
                      SUM(b.issued_roll) yarn_roll,
                 SUM(b.issued_net_qty) yarn_kgs,  
                 b.ISSUE_NO,             
                 sum(b.issued_tar_qty) TARE_QTY,
                 sum(b.issued_gro_qty) GROSS_QTY
          FROM nyf.fmit_yarn_issuedb b
          WHERE EXTRACT(YEAR from b.issued_Date) >= 2019
          GROUP BY b.requested_type,b.movement_account
             ,b.issued_Date, b.rm_job_no,b.yarn_item, b.yarn_lot,b.bu_ASSET,b.BU_ISSUED,
             b.ISSUE_NO                         
         """

  _filename = r"C:\Qlikview_Report\YARN INVENTORY\DATA_RM_YARN_ISSUE_DETAIL.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_RM_YARN_ISSUE_DETAIL")
  sendLine("COMPLETE CLS_DATA_RM_YARN_ISSUE_DETAIL")

#############################################

###########################################
class CLS_DATA_YARN_KP_JOB(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_YARN_KP_JOB()


def DATA_YARN_KP_JOB():
  my_dsn = cx_Oracle.makedsn("172.16.6.75", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START DATA_YARN_KP_JOB")
  sql =""" SELECT distinct RM_JOB_NO, LISTAGG(KP_NO, ', ') WITHIN GROUP (ORDER BY KP_NO)
 OVER (PARTITION BY RM_JOB_NO) as KP_NO_LIST
       FROM  nyf.FMIT_YARN_ISSUEDH
      WHERE  RM_JOB_NO is not null """

  _filename = r"C:\Qlikview_Report\YARN INVENTORY\DATA_YARN_KP_JOB.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_YARN_KP_JOB")
  sendLine("COMPLETE CLS_DATA_YARN_KP_JOB")

#############################################

###########################################
class CLS_DATA_YARN_VENDOR(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_YARN_VENDOR()


def DATA_YARN_VENDOR():
  my_dsn = cx_Oracle.makedsn("172.16.6.75", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_YARN_VENDOR")
  sql =""" select rm_job_no, kp_no,  GET_VENDOR_YARN('ขาย', rm_job_no) SO_VENDOR_NAME ,   GET_VENDOR_YARN('xxx', rm_job_no) PO_VENDOR_NAME
           from 
           (
             SELECT  RM_JOB_NO, min(KP_NO) KP_NO
             FROM  nyf.FMIT_YARN_ISSUEDH
             WHERE  RM_JOB_NO is not null      
                and  EXTRACT(YEAR from issued_Date) >= 2019
             group by RM_JOB_NO) m """

  _filename = r"C:\Qlikview_Report\YARN INVENTORY\DATA_YARN_VENDOR.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_YARN_VENDOR")
  sendLine("COMPLETE CLS_DATA_YARN_VENDOR")

#############################################


###########################################
class CLS_YARN_CONTAIN(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    YARN_CONTAIN()


def YARN_CONTAIN():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_YARN_CONTAIN")
  sql =""" SELECT distinct Y.ITEM_CODE AS ITEM_CODE_SHADE, Y.YARN_ITEM, 
CASE WHEN I.ACTIVE ='Y' THEN Y.ITEM_CONTAIN ELSE 0 END ITEM_CONTAIN,i.ACTIVE
FROM SF5.DFIT_KNIT_YARN Y, SF5.FMIT_YARN_PRICE I
WHERE I.ITEM_CODE = Y.YARN_ITEM
AND Y.ITEM_CODE LIKE 'F%'
AND SUBSTR(Y.ITEM_CODE,LENGTH(Y.ITEM_CODE)-1,1) IN ('A','B')  
ORDER BY Y.ITEM_CODE, Y.YARN_ITEM """

  _filename = r"C:\QVD_DATA\OPE_FABRIC\DATA_YARN_CONTAIN.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_YARN_CONTAIN")
  sendLine("COMPLETE CLS_YARN_CONTAIN")

#############################################

###########################################
class CLS_DATA_OPEN_QN_YARN_1126(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_OPEN_QN_YARN_1126()


def DATA_OPEN_QN_YARN_1126():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_OPEN_QN_YARN_1126")
  sql ="""  select AA.* ,
      (SELECT max(dh.START_STEPS) FROM DUMMY_CONFIRM_ORDER dh WHERE dh.STEPS_ID = 2 and dh.ORDER_NUMBER=aa.RS_NO) rs_send_date
      from( 
            SELECT  B.ORDER_NO QN_NO,
               B.TEAM_NAME,
               B.SALE_EXPORT SALE_NAME,
               B.CUSTOMER_EXPORT CUSTOMER_NAME,
               B.ORDER_DATE QN_DATE,
               C.ITEM_CODE,
               D.ITEM_DESC,
               C.ITEM_WEIGHT ORDER_QTY,
               B.CUSTOMER_END,
               Y.YARN_ITEM,
               c.TOPDYED_COLOR,
               YARN_BOM_KG Std_Kgs,
               REQUESTED_BOM_KG Yarn_Kgs,
               YARN_PRICE_KG Yarn_price,
               c.FABRIC_SHADE,
               c.TARGET_PRICE,
               Z.LMARKET_PPRICE LMARKET_PRICE_LBS ,
               Z.LMARKET_KPRICE LMARKET_PRICE_KG,
               Z.LSUPPLIER ,
               Z.LYARN_GROUP,
               Z.LRISK,
               Z.LMARKET_USDPPRICE LVAT,
               Z.LDISCOUNT,
               Z.EMARKET_USDPPRICE EMARKET_PRICE_USD_LBS,
               Z.EMARKET_USDKPRICE EMARKET_PRICE_USD_KGS,
               Z.ESUPPLIER,
               Z.EYARN_GROUP,
               Z.ERISK,
               Z.ELOGISTIC,
               Z.ECOUNTRY,
               Z.EXCHANGE_RATE,
               Z.ETAX_5,
               Z.EDISCOUNT,
               case when C.ITEM_WEIGHT <> 0 then round((YARN_BOM_KG/ C.ITEM_WEIGHT)*100,2) else 0 end percent_Contain,
               OPEN_QN('C'||B.ORDER_NO,C.ITEM_CODE) OPEN_DUMMY_SO,
               (select max(rs.order_number) from DUMMY_SO_HEADERS rs where rs.qn_no = 'C'||B.ORDER_NO) RS_NO,
               (select QH.ORDER_DATE from sf5.DFIT_QN_REH QH where QH.ORDER_NO = B.ORDER_NO) CREATE_DATE
    FROM   sf5.DFIT_MORDER B,
           sf5.DFIT_DORDER C,
           sf5.FMIT_ITEM D,
           sf5.DFIT_YARN_BOM Y,
           SF5.FMIT_YARN_PRICE Z
    WHERE B.ORDER_NO = C.ORDER_NO
       AND C.ITEM_CODE = D.ITEM_CODE
       AND B.ORDER_NO = Y.ORDER_NO
       AND C.ITEM_CODE = Y.ITEM_CODE
       and c.line_id = y.line_id
       AND z.ITEM_CODE = Y.YARN_ITEM 
       AND trunc(B.ORDER_DATE) >= TO_DATE('01/01/2017','DD/MM/YYYY')) AA """

  _filename1 = r"C:\Qlikview_Report\YARN INVENTORY\DATA_OPEN_QN_YARN_1126.xlsx"
  _filename2 = r"C:\QlikView\QVD\QN\DATA_OPEN_QN_YARN_1126.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename1, index=False)
  df.to_excel(_filename2, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_OPEN_QN_YARN_1126")
  sendLine("COMPLETE CLS_DATA_OPEN_QN_YARN_1126")

#############################################

###########################################
class CLS_DATA_QN_Daily_1289(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_QN_Daily_1289()


def DATA_QN_Daily_1289():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_QN_Daily_1289")
  sql =""" SELECT  B.TEAM_NAME,
               B.SALES_ID,
               B.SALE_EXPORT SALE_NAME,
               B.ORDER_NO,
               B.ORDER_DATE,
               (select EXPIRED_DATE from SF5.DFIC_MORDER o where o.ORDER_NO = B.ORDER_NO) EXPIRED_DATE,
               DECODE(SUBSTR(QUOTATION_TYPE,1,1),'1','New','2','Reopen')   QUOTATION_TYPE,               
               B.CUSTOMER_EXPORT CUSTOMER_NAME,              
               B.CUSTOMER_END BUYER,               
               C.ITEM_CODE,
               D.ITEM_DESC,            
               C.FABRIC_SHADE,
               C.TARGET_PRICE,               
               C.ITEM_PRICE,
               C.ITEM_WEIGHT ORDER_QTY,
               C.PENDING_BF_QTY,
               C.DROPPED_BF_QTY,               
               C.CLOSED_BF_QTY,
                nyis.GET_RESERVED_ORDER_QTY (B.ORDER_NO , C.FABRIC_SHADE , C.ITEM_CODE)  reserve_qty,
               nyis.GET_PROD_ORDER_QTY (B.ORDER_NO , C.FABRIC_SHADE , C.ITEM_CODE) prod_so_qty,
               sf5.OPEN_QN('C'||c.ORDER_NO,c.ITEM_CODE) cust_so_qty,
              (nvl(C.ITEM_WEIGHT,0) -   nvl(sf5.OPEN_QN('C'||c.ORDER_NO,c.ITEM_CODE),0) - nvl(C.DROPPED_BF_QTY,0)) Balance_qty,
                nyis.get_YARN_BY_IEM (c.item_code) yarn_item,
                B.REQUESTED_DATE,
                D.MACHINE_GROUP,
                case when (B.SO_STATUS LIKE '1%' OR B.SO_STATUS LIKE '2%' OR B.SO_STATUS LIKE '4%') and (trunc(sysdate) <= GET_EXPIRED_DATE_QN(B.ORDER_NO))  THEN 'Order Pending'                 
                 when (nvl(C.PENDING_BF_QTY,0) > 0) and (trunc(sysdate) <= GET_EXPIRED_DATE_QN(B.ORDER_NO)) THEN 'Order Pending'          
                 when B.SO_STATUS LIKE '3%' THEN 'Order Completed'
                 when B.SO_STATUS LIKE '5%' THEN 'Order Dropped'
                 when  greatest(GET_EXPIRED_DATE_QN(B.ORDER_NO)-trunc(sysdate),0) <= 1 THEN 'Order Expired'
                 else null end status,
                 (SELECT DISTINCT qn_type  FROM sf5.dfit_qn_reh qn  WHERE qn.order_no = b.order_no) QN_TYPE,
                 itm.CHK_Isegment_support(null,C.ITEM_CODE,'01') Market_type
                 ,( select red.CONFIRM_PLAN   from  sf5.dfit_qn_red red where red.order_no = b.order_no and  red.LINE_ID = c.line_id) CONFIRM_PLAN,
                 OPEN_QN('C'||B.ORDER_NO,C.ITEM_CODE) OPEN_DUMMY_SO,
                 (select QH.ORDER_DATE from sf5.DFIT_QN_REH QH where QH.ORDER_NO = B.ORDER_NO) CREATE_DATE
       FROM   sf5.DFIT_MORDER B,
              sf5.DFIT_DORDER C,
              sf5.FMIT_ITEM D
       WHERE B.ORDER_NO = C.ORDER_NO
              AND C.ITEM_CODE = D.ITEM_CODE
              AND trunc(B.ORDER_DATE) >= to_date('01/01/2017','dd/mm/yyyy') """

  _filename1 = r"C:\Qlikview_Report\YARN INVENTORY\DATA_QN_Daily_1289.xlsx"
  _filename2 = r"C:\QlikView\QVD\QN\DATA_QN_Daily_1289.xlsx"
  
  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename1, index=False)
  df.to_excel(_filename2, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_QN_Daily_1289")
  sendLine("COMPLETE CLS_DATA_QN_Daily_1289")

#############################################

###########################################
class CLS_DATA_OpenPMO_PO_RS_1802(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_OpenPMO_PO_RS_1802()


def DATA_OpenPMO_PO_RS_1802():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_OpenPMO_PO_RS_1802")
  sql =""" select aa.*,
              (select max(rs.need_by_date) from sf5.RSPR_SUMMARY rs where rs.req_number = LTRIM(TO_CHAR(aa.RS_NO))and rs.ITEM_CODE = aa.YARN_ITEM) need_by_date, 
              GET_RS_PO_SS('PO', aa.PO_LINE, aa.YARN_ITEM,bb.CREATION_DATE) PO_NUMBER,  
              BB.UNIT_PRICE,  
              BB.CURRENCY_CODE,
              GET_RS_PO_SS('PRICE', aa.PO_LINE, aa.YARN_ITEM,bb.CREATION_DATE) PO_PRICE,  
              GET_RS_PO_SS('CURRENCY', aa.PO_LINE, aa.YARN_ITEM,bb.CREATION_DATE) PO_CURRENCY,  
              BB.VENDOR_NAME, 
              BB.QUANTITY,
              BB.UOM,
              (select max(dp.yarn_comment) from sf5.dummy_yarn_planin dp where to_char(dp.order_number) = aa.RS_NO and dp.YARN_ITEM = aa.YARN_ITEM
                                                                    and exists ( select 'D' from sf5.dummy_confirm_mrp mp where mp.order_number=dp.order_number 
                                                                    and mp.yarn_item=dp.yarn_item and mp.yarn_color=dp.pl_color and nvl(mp.po_flag,'N')='Y')) yarn_comment, 
              GET_RS_PO_SS('PO_DATE', aa.PO_LINE, aa.YARN_ITEM,bb.CREATION_DATE) PO_DATE,  
              bb.PO_STATUS, 
              bb.CREATION_DATE,
              (select so.so_no_date from sf5.smit_so_header so where so.so_no = aa.so_no) SO_NO_DATE ,
              (SELECT max(dh.START_STEPS) FROM sf5.DUMMY_CONFIRM_ORDER dh WHERE dh.STEPS_ID = 2 and to_char(dh.ORDER_NUMBER)=aa.RS_NO) rs_send_date,
              (SELECT DH.FLOW_STATUS_CODE FROM SF5.DUMMY_SO_HEADERS DH  WHERE to_char(DH.ORDER_NUMBER)=aa.RS_NO) RS_STATUS,
              (SELECT DISTINCT  NPO.PO_STATUS  FROM SF5.NY_PO_LINE_DISTRIBUTIONS_V NPO WHERE NPO.PO_NUMBER =aa.YARN_PONO
                                                                                         AND NPO.PO_LINE_NO =aa.PO_LINE) ORA_PO_STATUS,
              (SELECT ds.CREATION_DATE  FROM SF5.DUMMY_SO_HEADERS ds WHERE to_char(ds.ORDER_NUMBER)=aa.RS_NO) CREATE_DATE   
       from(
        SELECT A.FG_WEEK,A.KNIT_WEEK_END KNIT_WEEK, LTRIM(TO_CHAR(A.RS_NO)) RS_NO,A.SO_NO ,
               A.DUMMY_ITEM_TYPE ORDER_TYPE, A.SALE_NAME, A.TEAM_NAME, A.CUSTOMER_NAME, A.END_BUYER,
               A.MS_APPROVED_DATE,  A.YARN_ITEM, A.YARN_COLOR, A.PLANIN_WW, NVL(A.PO_SHIPPMENT,A.PLANIN_DATE)  YARN_IN_DATE, A.YARN_QTY, 
               A.CONFIRM_PLANIN,
               (SELECT  max(r.PO_NO) FROM SF5.RSPR_SUMMARY R   WHERE REQ_NUMBER= LTRIM(TO_CHAR(A.RS_NO))  and item_code= a.YARN_ITEM  and color_code= a.YARN_COLOR) YARN_PONO,
               (SELECT  max(r.PO_LINE) FROM SF5.RSPR_SUMMARY R   WHERE REQ_NUMBER= LTRIM(TO_CHAR(A.RS_NO))  and item_code= a.YARN_ITEM  and color_code= a.YARN_COLOR) PO_LINE,
               LTRIM(TO_CHAR(A.PO_IN_DATE,'IYYYIW')) PO_IN_WEEK, A.PO_IN_DATE,
               TRUNC(NVL(A.PO_SHIPPMENT,A.PLANIN_DATE))-NVL(TRUNC(A.PO_IN_DATE),TRUNC(SYSDATE)) DIF,
               A.QN_NO ,
               A.QN_DATE              
        FROM SF5.DUMMY_YARN_PLANIN_V A
        WHERE A.PO_FLAG='PO' 
        AND A.MS_APPROVED_DATE >= NVL(TO_DATE('01/01/2021','DD/MM/RRRR'),A.MS_APPROVED_DATE)+0.00000
        UNION
        SELECT LTRIM(TO_CHAR(C.FG_WEEK)) FG_WEEK, LTRIM(TO_CHAR(C.KNIT_WEEK)) KNIT_WEEK, C.RS_NO, C.SO_NO, 
               C.ORDER_TYPE, C.SALE_NAME, C.TEAM_NAME, C.CUSTOMER_NAME,C.END_BUYER,
              C.MS_APPROVED_DATE, C.YARN_ITEM, C.YARN_COLOR, C.PLANIN_WW, C.YARN_IN_DATE,C.YARN_QTY, C.CONFIRM_PLANIN,
              C.PO_NO, C.PO_LINE, C.PO_IN_WEEK, C.PO_IN_DATE,
              TRUNC(NVL(C.YARN_IN_DATE,C.CONFIRM_PLANIN))-NVL(TRUNC(C.PO_IN_DATE),TRUNC(SYSDATE))  DIF,
              QN_NO, QN_DATE
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
                                 D.PO_IN_DATE,
                                 SUBSTR(D.QN_NO,2) QN_NO ,(SELECT ORDER_DATE FROM SF5.DFIT_MORDER WHERE ORDER_NO=SUBSTR(D.QN_NO ,2) ) QN_DATE                       
                    FROM SF5.DUMMY_PURCHASEYARN_H H, SF5.DUMMY_PURCHASEYARN_D D
                    WHERE H.YP_ORDER=D.YP_ORDER
                    AND SCM_QTY>0
        ) C
        WHERE   C.MS_APPROVED_DATE >= NVL(TO_DATE('01/01/2021','DD/MM/RRRR'),C.MS_APPROVED_DATE)+0.00000
        ) AA,
        (select T1.SEGMENT1,t2.LINE_NUM ,
                VD.VENDOR_NAME, 
                T1.CURRENCY_CODE,
                T2.UNIT_MEAS_LOOKUP_CODE UOM, 
                T2.QUANTITY , 
                T2.UNIT_PRICE,  
                T1.AUTHORIZATION_STATUS PO_STATUS,       
                trunc(T1.CREATION_DATE) CREATION_DATE    
        FROM PO_HEADERS_ALL@R12INTERFACE.WORLD T1,
             PO_VENDORS@R12INTERFACE.WORLD VD,
             PO_LINES_ALL@R12INTERFACE.WORLD T2 
        where  VD.VENDOR_ID = T1.VENDOR_ID
        and t1.PO_HEADER_ID = t2.PO_HEADER_ID
        and t1.org_id = t2.org_id
        AND NVL(T2.CANCEL_FLAG,'N') <> 'Y'
        AND T1.org_id in (select o.org_id from rapps.ny_tb_org@R12INTERFACE.WORLD o where o.bu_id = 3)) BB
        WHERE AA.YARN_PONO = BB.SEGMENT1(+)
        and aa.PO_LINE = BB.LINE_NUM(+) 
        AND nvl(bb.CREATION_DATE,sysdate) >= NVL(TO_DATE('01/01/2021','DD/MM/RRRR'),nvl(bb.CREATION_DATE,sysdate))+0.00000 """

  _filename = r"C:\Qlikview_Report\YARN INVENTORY\DATA_OpenPMO_PO_RS_1802.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_OpenPMO_PO_RS_1802")
  sendLine("COMPLETE CLS_DATA_OpenPMO_PO_RS_1802")

#############################################

#############################################
class CLS_DATA_QN_NO_COLOR(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_QN_NO_COLOR()


def DATA_QN_NO_COLOR():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_QN_NO_COLOR")
  sql =""" SELECT QN_NO, QN_DATE, ENTRY_TIME, CUSTOMER_ID, CUSTOMER_NAME, CUSTOMER_END, BF_TYPE, C_TYPE, SALES_ID, SALES_BY, EXCHANGE_RATE, CURRENCY, PAYMENT_CONDITION, QUOTATION_TYPE
    , PRICE_TYPE, COUNTRY, TEAM_NAME, CONVERT_QN, CONVERT_DATE, QN_STATUS, RQN_DATE, RQN_BY, SEASON, STYLING, COMPLETE_QN, QN_TYPE, FREIGHT_COST
    , SUBSTR(ITEM_CODE,1,LENGTH(ITEM_CODE)-2) ITEM_CODE
    , LISTAGG(TOPDYED_COLOR, ',') WITHIN GROUP (ORDER BY TOPDYED_COLOR) TOPDYED_COLOR
    , LISTAGG(ITEM_TYPE, ',') WITHIN GROUP (ORDER BY ITEM_TYPE) ITEM_TYPE
    , AVG(CONSUMPTION) CONSUMPTION
    , LISTAGG(FABRIC_SHADE, ',') WITHIN GROUP (ORDER BY FABRIC_SHADE) FABRIC_SHADE
    , LISTAGG(ITEM_PRICE_TYPE, ',') WITHIN GROUP (ORDER BY ITEM_PRICE_TYPE) ITEM_PRICE_TYPE
    , LISTAGG(ITEM_PRICE, ',') WITHIN GROUP (ORDER BY ITEM_PRICE) ITEM_PRICEcls
    , LISTAGG(TARGET_PRICE, ',') WITHIN GROUP (ORDER BY TARGET_PRICE) TARGET_PRICE
    , LISTAGG(ORIGINAL_PRICE, ',') WITHIN GROUP (ORDER BY ORIGINAL_PRICE) ORIGINAL_PRICE
    , LISTAGG(ITEM_CM_PRICE, ',') WITHIN GROUP (ORDER BY ITEM_CM_PRICE) ITEM_CM_PRICE
    , LISTAGG(STD_ITEM_PRICE, ',') WITHIN GROUP (ORDER BY STD_ITEM_PRICE) STD_ITEM_PRICE
    , LISTAGG(LINE_REMARK, ',') WITHIN GROUP (ORDER BY LINE_REMARK) LINE_REMARK
    , LISTAGG(SHADE_DESC, ',') WITHIN GROUP (ORDER BY SHADE_DESC) SHADE_DESC
    , LISTAGG(SHADE_L, ',') WITHIN GROUP (ORDER BY SHADE_L) SHADE_L
    , LISTAGG(SHADE_M, ',') WITHIN GROUP (ORDER BY SHADE_M) SHADE_M
    , LISTAGG(SHADE_D, ',') WITHIN GROUP (ORDER BY SHADE_D) SHADE_D
    , LISTAGG(SHADE_S, ',') WITHIN GROUP (ORDER BY SHADE_S) SHADE_S
    , LISTAGG(USER_REMARK, ',') WITHIN GROUP (ORDER BY USER_REMARK) USER_REMARK
    , LISTAGG(FIRST_BATCH, ',') WITHIN GROUP (ORDER BY FIRST_BATCH) FIRST_BATCH
    , LISTAGG(MATCH_CONTRAST, ',') WITHIN GROUP (ORDER BY MATCH_CONTRAST) MATCH_CONTRAST
    , LISTAGG(BODY_TRIM, ',') WITHIN GROUP (ORDER BY BODY_TRIM) BODY_TRIM
    , LISTAGG(PILOT_RUN, ',') WITHIN GROUP (ORDER BY PILOT_RUN) PILOT_RUN
    , LISTAGG(STYLE_REVIEW, ',') WITHIN GROUP (ORDER BY STYLE_REVIEW) STYLE_REVIEW
    , SUM(ITEM_WEIGHT) ITEM_WEIGHT
    , SUM(TOTAL_QTY) TOTAL_QTY
    , LISTAGG(UOM, ',') WITHIN GROUP (ORDER BY UOM) UOM
    , OPEN_QN('C'||QN_NO,ITEM_CODE) OPEN_DUMMY_SO
    --, GET_RS_QTY_RESERVED(QN_NO,ITEM_CODE) RS_RESERVE
    --, GET_RS_QTY_ORDERED(QN_NO,ITEM_CODE) RS_ORDER
    , MAX(RS_RESERVE) RS_RESERVE
    , MAX(RS_ORDER) RS_ORDER
    , CASE
            WHEN EXPIRE_DATE IS NULL AND TO_DATE(QN_DATE,'YYYY-MM-DD') <= TO_DATE('31/03/2021','DD/MM/RRRR')
                THEN  '2021-03-31'
            WHEN EXPIRE_DATE IS NULL AND TO_DATE(QN_DATE,'YYYY-MM-DD') <= TO_DATE('30/09/2021','DD/MM/RRRR')
                 AND QN_STATUS = 'Delete' THEN QN_DATE
            WHEN EXPIRE_DATE IS NULL AND TO_DATE(QN_DATE,'YYYY-MM-DD') <= TO_DATE('30/09/2021','DD/MM/RRRR')
                 AND QN_STATUS like 'Reject%' THEN QN_DATE                 
            ELSE
               TO_CHAR(EXPIRE_DATE,'YYYY-MM-DD')
       END  EXPIRE_DATE      
    , SUM(PENDING_QTY) PENDING_QTY
    , SUM(DROP_QTY) DROP_QTY
    , SUM(CLOSED_QTY) CLOSED_QTY
    , SUM(RESERVE_QTY) RESERVE_QTY
    ,'ITEM_WEIGHT-OPEN_DUMMY_SO-DROP_QTY' FORMULA
    , (SUM(ITEM_WEIGHT) - OPEN_QN('C'||QN_NO,ITEM_CODE) - SUM(DROP_QTY)) AS BALANCE_QTY
    , DECODE(UNOPEN_DUMMY(QN_NO),'N','CONTRACT','') CONTRACT
    , GET_PRE_QN_STATUS(QN_NO) SO_DUMMY_STATUS
    , LISTAGG(CONFIRM_PLAN, ',') WITHIN GROUP (ORDER BY CONFIRM_PLAN) CONFIRM_PLAN
    , SUM(COMFIRM_KGS) COMFIRM_KGS
    , YEAR, WEEK, YEAR || WEEK AS WW
    , LINE_ID
    , (SELECT MAX(DROPPED_REASON) FROM DFIT_BF_TARGET BF WHERE M.QN_NO = BF.ORDER_NO AND M.LINE_ID = BF.LINE_ID) DROPPED_REASON
    , YR_NO
    , TYPE_SUB
    , (SELECT MM.ORDER_DATE FROM DFIT_MORDER MM WHERE MM.ORDER_NO = M.QN_NO) QN_DATE_COLOR
    , (SELECT MW.EXPIRED_DATE FROM DFIT_MORDER MW WHERE MW.ORDER_NO = M.QN_NO) EXPIRED_DATE_COLOR
    , ITEM_CODE ITEM_CODE_Color
    , STATUS_LINE
FROM (
        SELECT M.ORDER_NO QN_NO, TO_CHAR(M.ORDER_DATE,'YYYY-MM-DD') QN_DATE, M.ENTRY_TIME,M.CUSTOMER_ID, M.CUSTOMER_NAME, M.CUSTOMER_END, M.BF_TYPE, M.C_TYPE, M.SALES_ID, M.SALES_BY,  M.EXCHANGE_RATE, M.CURRENCY, M.PAYMENT_CONDITION
            ,M.QUOTATION_TYPE, M.PRICE_TYPE, M.COUNTRY, M.TEAM_NAME, M.CONVERT_QN, M.CONVERT_DATE, M.RQN_APPROVED AS QN_STATUS, M.RQN_DATE, M.RQN_BY, M.SEASON, M.STYLING, M.COMPLETE_QN, M.QN_TYPE, M.FREIGHT_COST
            ,D.LINE_ID, D.ITEM_CODE, D.TOPDYED_COLOR, D.ITEM_TYPE,  D.CONSUMPTION,  D.FABRIC_SHADE, D.PRICE_TYPE ITEM_PRICE_TYPE, D.ACTIVE_STATUS,  D.ITEM_PRICE ,D.TARGET_PRICE , D.ORIGINAL_PRICE, D.ITEM_CM_PRICE, D.STD_ITEM_PRICE
            ,D.LINE_REMARK , D.SHADE_DESC, D.SHADE_L, D.SHADE_M, D.SHADE_D, D.SHADE_S, D.USER_REMARK, D.FIRST_BATCH, D.MATCH_CONTRAST, D.BODY_TRIM , D.PILOT_RUN, D.STYLE_REVIEW  
            ,D.ITEM_WEIGHT, D.TOTAL_QTY, D.UOM
            ,(SELECT EXPIRED_DATE FROM DFIC_MORDER O WHERE O.ORDER_NO = M.ORDER_NO) EXPIRE_DATE
            ,GET_PRE_QN_LINE_PENDING(M.ORDER_NO,D.LINE_ID) PENDING_QTY
            ,GET_PRE_QN_LINE_DROP(M.ORDER_NO,D.LINE_ID) DROP_QTY
            ,GET_PRE_QN_LINE_CLOSED(M.ORDER_NO,D.LINE_ID) CLOSED_QTY
            ,GET_PRE_QN_LINE_RESERVED(M.ORDER_NO,D.LINE_ID) RESERVE_QTY
            ,CASE WHEN D.confirm_plan = 'CONFIRM' THEN d.item_weight ELSE 0 END COMFIRM_KGS
            ,D.CONFIRM_PLAN
            ,TO_CHAR(M.ORDER_DATE,'IYYY') YEAR
            ,TO_CHAR(M.ORDER_DATE,'IW') WEEK
            , GET_RS_QTY_RESERVED(M.ORDER_NO, D.ITEM_CODE) RS_RESERVE
            , GET_RS_QTY_ORDERED(M.ORDER_NO, D.ITEM_CODE) RS_ORDER
            , QNYR_NO(M.ORDER_NO) YR_NO
            , QN_TYPE_SUB TYPE_SUB
            , decode(D.ACTIVE_STATUS,'1','APPROVED','4','REJECT',D.ACTIVE_STATUS) STATUS_LINE
        FROM DFIT_QN_REH M, DFIT_QN_RED D
        WHERE M.ORDER_NO = D.ORDER_NO
            AND M.ORDER_NO >= 645358 -- 618692
            AND TO_CHAR(M.ORDER_DATE,'YYYY-MM-DD') >= '2019-01-01' 
            AND NVL(M.RQN_APPROVED,'X') <> 'DELETE' 
        ) M
GROUP BY QN_NO, QN_DATE, ENTRY_TIME, CUSTOMER_ID, CUSTOMER_NAME, CUSTOMER_END, BF_TYPE, C_TYPE, SALES_ID, SALES_BY, EXCHANGE_RATE, CURRENCY, PAYMENT_CONDITION, QUOTATION_TYPE
    ,PRICE_TYPE, COUNTRY, TEAM_NAME, CONVERT_QN, CONVERT_DATE, QN_STATUS, RQN_DATE, RQN_BY, SEASON, STYLING, COMPLETE_QN, QN_TYPE, FREIGHT_COST
    ,ITEM_CODE, YEAR, WEEK, TO_CHAR(EXPIRE_DATE,'YYYY-MM-DD'), EXPIRE_DATE, LINE_ID ,YR_NO, TYPE_SUB, STATUS_LINE
ORDER BY QN_DATE DESC, QN_NO DESC, ITEM_CODE """

  _filename = r"C:\Qlikview_Report\YARN INVENTORY\DATA_QN_NO_COLOR.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_QN_NO_COLOR")
  sendLine("COMPLETE CLS_DATA_QN_NO_COLOR")

#############################################

#############################################
class CLS_DATA_QN_DROPPED(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_QN_DROPPED()


def DATA_QN_DROPPED():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_QN_DROPPED")
  sql =""" select ORDER_NO,LINE_ID,TARGET_WEEK,ITEM_CODE,DROPPED_WEIGHT, ENTRY_DATE DROPPED_DATE, UPDATE_BY DROPPED_BY, DROPPED_REASON
           from DFIT_BF_TARGET   
           where DROPPED_WEIGHT > 0 """

  _filename = r"C:\Qlikview_Report\YARN INVENTORY\DATA_QN_DROPPED.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_QN_DROPPED")
  sendLine("COMPLETE CLS_DATA_QN_DROPPED")

#############################################

#############################################
class CLS_DATA_QN_PLAN(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_QN_PLAN()


def DATA_QN_PLAN():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_QN_PLAN")
  sql =""" select ORDER_NO, LINE_ID, PLAN_TYPE, ITEM_1 ITEM_CODE, PERIOD_WEEK, QUANTITY PLAN_QTY, PLAN_SEQ, UOM
           from dfit_qn_red_plan """

  _filename = r"C:\Qlikview_Report\YARN INVENTORY\DATA_QN_PLAN.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_QN_PLAN")
  sendLine("COMPLETE CLS_DATA_QN_PLAN")

#############################################

#############################################
class CLS_DATA_QN_RS(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_QN_RS()


def DATA_QN_RS():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_QN_RS")
  sql =""" select a.qn_no QN_NO, substr(a.QN_NO,2,length(a.qn_no)) QN_NO_C, a.order_number RS_NO,a.ORDERED_DATE RS_DATE,b.ORDERED_ITEM item_code, sum(b.ORDER_PROD_QTY) RS_QTY, b.ORDER_UOM UOM
           from DUMMY_SO_HEADERS a, DUMMY_LINES_COLOR_VIEW b  
           where a.order_number = B.Order_Number
                 and TO_CHAR(a.ORDERED_DATE,'YYYY-MM-DD') >= '2019-01-01' 
                 and a.qn_no is not null
           group by a.qn_no ,a.order_number ,a.ORDERED_DATE ,b.ORDERED_ITEM , b.ORDER_UOM 
           order by 1 """

  _filename = r"C:\Qlikview_Report\YARN INVENTORY\DATA_QN_RS.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_QN_RS")
  sendLine("COMPLETE CLS_DATA_QN_RS")

#############################################

#############################################
class CLS_DATA_QN_YR(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_QN_YR()


def DATA_QN_YR():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_QN_YR")
  sql =""" select D.QN_NO,substr(D.QN_NO,2,length(d.qn_no)) QN_NO_C , H.YP_ORDER YR_NO,H.OPEN_DATE YR_DATE, D.ITEM_CODE, D.YARN_ITEM, sum(D.YARN_QTY) YR_QTY
           from Dummy_Purchaseyarn_H H, DUMMY_PURCHASEYARN_D D
           WHERE  H.YP_ORDER = D.YP_ORDER
              and TO_CHAR(h.OPEN_DATE,'YYYY-MM-DD') >= '2019-01-01' 
              and d.qn_no is not null
           GROUP BY  D.QN_NO, H.YP_ORDER,H.OPEN_DATE, D.ITEM_CODE, D.YARN_ITEM
           order by 1 """

  _filename = r"C:\Qlikview_Report\YARN INVENTORY\DATA_QN_YR.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_QN_YR")
  sendLine("COMPLETE CLS_DATA_QN_YR")

#############################################


#############################################
class CLS_DATA_QN_VS_SO(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_QN_VS_SO()


def DATA_QN_VS_SO():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_QN_VS_SO")
  sql =""" SELECT m.SO_QN QN_NO, m.ITEM_CODE, m.so_no, 
                 (select (c.so_type) from sf5.DUMMY_SO_HEADERS C where c.qn_no = m.SO_QN and C.Ora_Order_Number = m.so_no) so_type,
                 D.ACT_ORDER_QTY ORDER_QTY
            FROM SF5.SF5_GAP_SO_OE_LINE M, NYIS.WEB_90100010610_OE D
            WHERE EXTRACT(YEAR FROM M.SO_NO_DATE)  >= 2018
              AND M.SO_NO = D.SO_NO(+)
              AND M.SO_LINE = D.SO_LINE(+)
       """

  _filename = r"C:\Qlikview_Report\YARN INVENTORY\DATA_QN_VS_SO.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_QN_VS_SO")
  sendLine("COMPLETE CLS_DATA_QN_VS_SO")

#############################################

#############################################
class CLS_DATA_QN_VS_RS_NEW(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_QN_VS_RS_NEW()


def DATA_QN_VS_RS_NEW():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_QN_VS_RS_NEW")
  sql =""" select AA.*,
       OPEN_QN_RS('C'||QN_NO,ITEM_CODE,RS_NO) RS_Open_QTY,
       OPEN_QN_RS_USEDSTOCK('C'||QN_NO,ITEM_CODE,RS_NO) RS_USED_QTY
from(
SELECT  B.ORDER_NO QN_NO, 
               B.TEAM_NAME,
               B.SALE_EXPORT SALE_NAME,
               B.CUSTOMER_EXPORT CUSTOMER_NAME,
               B.ORDER_DATE QN_DATE,
               B.EXPIRED_DATE,
               C.ITEM_CODE,
               B.CUSTOMER_END,
               C.ITEM_WEIGHT ORDER_QN_QTY,               
              rs.order_number RS_NO
   FROM   sf5.DFIT_MORDER B,
           sf5.DFIT_DORDER C,
           DUMMY_SO_HEADERS rs
    WHERE B.ORDER_NO = C.ORDER_NO
       and  rs.qn_no = 'C'||B.ORDER_NO
       and nvl(rs.close_flag,'N') <> 'C'
       and rs.flow_status_code <> 'CANCEL'
       and rs.fob_point_code not like 'R%'
       and rs.so_reserve is null
       and rs.data_rec_type = 'ORDERED'
       AND trunc(B.ORDER_DATE) >= TO_DATE('01/01/2019','DD/MM/YYYY')) AA
       """

  _filename = r"C:\Qlikview_Report\YARN INVENTORY\DATA_QN_VS_RS_NEW.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_QN_VS_RS_NEW")
  sendLine("COMPLETE CLS_DATA_QN_VS_RS_NEW")

#############################################


#############################################
class CLS_DATA_DUMMY_KNIT_NEW(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_DUMMY_KNIT_NEW()


def DATA_DUMMY_KNIT_NEW():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_DUMMY_KNIT_NEW")
  sql ="""  select H.Order_Number,h.qn_no,
          (case when TUBULAR_TYPE = '2' then substr(L.ORDERED_ITEM,1,length(ORDERED_ITEM)-2)||'A0' else 
                             substr(L.ORDERED_ITEM,1,length(ORDERED_ITEM)-2)||'B0'  end) ORDERED_ITEM,
           l.ORDERED_QUANTITY OPEN_RS,
           sf5.get_RUCONFIRM_STOCK_KP(H.ORDER_NUMBER) KP_NO,
           H.YARN_PONO
     from sf5.DUMMY_SO_HEADERS H,
              sf5.DUMMY_CONFIRM_ORDER C,
              sf5.DUMMY_SO_FABRIC_LINES L
     where H.FLOW_STATUS_CODE='PPC PROCESS FG' AND  NVL(H.DUMMY_TYPE,'NORMAL')<>'SEAMLESS'
     AND SF5.CHECK_STEPS5 (H.ORDER_NUMBER)='PASS' AND
                 C.STEPS_ID = 8 and
                  EXISTS ( SELECT 'T' FROM  sf5.DUMMY_CONFIRM_ORDER B
                                     WHERE B.ORDER_NUMBER=H.ORDER_NUMBER
                                            AND B.ORG_ID=H.ORG_ID 
                                            AND B.STEPS_ID=8
                                            AND CONFIRM_DATE IS NULL 
                                            AND B.START_STEPS IS NOT NULL)
                                            AND NOT EXISTS ( SELECT 'T' FROM  sf5.DUMMY_CONFIRM_ORDER B  
                                                                                 WHERE B.ORDER_NUMBER=H.ORDER_NUMBER
                                                                                        AND B.ORG_ID=H.ORG_ID 
                                                                                        AND B.STEPS_STAGE='STAGE-2'
                                                                                        AND CONFIRM_DATE  IS  NULL )         
       AND H.ORDER_NUMBER = C.ORDER_NUMBER         
       AND H.ORDER_NUMBER = L.ORDER_NUMBER(+)
       AND trunc(H.ORDERED_DATE) >= TO_DATE('01/01/2018','DD/MM/YYYY')
       """

  _filename = r"C:\Qlikview_Report\YARN INVENTORY\DATA_DUMMY_KNIT_NEW.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_DUMMY_KNIT_NEW")
  sendLine("COMPLETE CLS_DATA_DUMMY_KNIT_NEW")

#############################################

#############################################
class CLS_DATA_KNIT_SOKP_NEW(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_KNIT_SOKP_NEW()


def DATA_KNIT_SOKP_NEW():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_KNIT_SOKP_NEW")
  sql ="""  select A.rs_number RS_NO,
                  (SELECT substr(b.QN_NO,2,length(b.QN_NO)) FROM DUMMY_SO_HEADERS b where b.order_number = a.rs_number) QN_NO,
                   A.KNIT_MC_SO,A.KNIT_MC_YEAR,A.KNIT_MC_WW,A.KNIT_ITEM_CODE, A.KNIT_ITEM_DESC,A.KP_NO,SUM(A.KNIT_MC_KG) KP_WEIGHT
            FROM DFIT_KNIT_SOKP A
            where KNIT_MC_YEAR >= 2019
            GROUP BY rs_number,KNIT_MC_SO,KNIT_MC_YEAR,KNIT_MC_WW,KNIT_ITEM_CODE, KNIT_ITEM_DESC,KP_NO
       """

  _filename = r"C:\Qlikview_Report\YARN INVENTORY\DATA_KNIT_SOKP_NEW.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_KNIT_SOKP_NEW")
  sendLine("COMPLETE CLS_DATA_KNIT_SOKP_NEW")

#############################################


threads = []
thread1 = CLS_DATA_Yarn_Received_PO_830();thread1.start();threads.append(thread1)
thread2 = CLS_DATA_Yarn_Received_PO_830_detail();thread2.start();threads.append(thread2)
thread3 = CLS_DATA_Yarn_aging_Received_797();thread3.start();threads.append(thread3)
thread4 = CLS_DATA_RM_YARN_ISSUE_DETAIL();thread4.start();threads.append(thread4)
thread5 = CLS_DATA_YARN_KP_JOB();thread5.start();threads.append(thread5)
#thread6 = CLS_DATA_YARN_VENDOR();thread6.start();threads.append(thread6)
thread7 = CLS_YARN_CONTAIN();thread7.start();threads.append(thread7)
thread8 = CLS_DATA_OPEN_QN_YARN_1126();thread8.start();threads.append(thread8)
thread9 = CLS_DATA_QN_Daily_1289();thread9.start();threads.append(thread9)
thread10 = CLS_DATA_OpenPMO_PO_RS_1802();thread10.start();threads.append(thread10)
thread11 = CLS_DATA_QN_NO_COLOR();thread11.start();threads.append(thread11)
thread12 = CLS_DATA_QN_DROPPED();thread12.start();threads.append(thread12)
thread13 = CLS_DATA_QN_PLAN();thread13.start();threads.append(thread13)
thread14 = CLS_DATA_QN_RS();thread14.start();threads.append(thread14)
thread15 = CLS_DATA_QN_YR();thread15.start();threads.append(thread15)
thread16 = CLS_DATA_QN_VS_SO();thread16.start();threads.append(thread16)
thread17 = CLS_DATA_QN_VS_RS_NEW();thread17.start();threads.append(thread17)
thread18 = CLS_DATA_DUMMY_KNIT_NEW();thread18.start();threads.append(thread18)
thread19 = CLS_DATA_KNIT_SOKP_NEW();thread19.start();threads.append(thread19)


for t in threads:
    t.join()
print ("COMPLETE")

