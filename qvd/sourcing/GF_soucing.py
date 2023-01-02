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




#############################################
class CLS_DATA_GF_ONHAND_BY_LOC(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_QN_RS()


def DATA_QN_RS():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_GF_ONHAND_BY_LOC")
  sql =""" select *
    from(
    SELECT 
         DH.SO_TYPE,
         A.STOCK_SITE,
         GET_WEEK_RANGE(TO_NUMBER(LTRIM(TO_CHAR(SYSDATE,'YYYYIW'))),
       to_number(rtrim((  nvl(A.KNIT_YEAR,DECODE(  SUBSTR(KP_NO,LENGTH(KP_NO)-2,3),'FQC','20'||SUBSTR(KP_NO,3,2)) ) ))||
       ltrim(to_char(  nvl(A.KNIT_WW,DECODE(  SUBSTR(KP_NO,LENGTH(KP_NO)-2,3),'FQC',SUBSTR(KP_NO,5,2)) ) ,'00')))) AGING_WW,  
        NVL(A.KNIT_WW,  DECODE(  SUBSTR(KP_NO,LENGTH(KP_NO)-2,3),'FQC',SUBSTR(KP_NO,5,2))) KNIT_WW,
        NVL(A.KNIT_YEAR,DECODE(  SUBSTR(KP_NO,LENGTH(KP_NO)-2,3),'FQC','20'||SUBSTR(KP_NO,3,2)) ) KNIT_YEAR,      
        SO.CUSTOMER_FG FG_WW,
        SO.CUSTOMER_YEAR FG_YEAR,
        DH.EDD_YEAR2,
        DH.EDD_WEEK2,
        DH.SALES_FG_WEEK ,       
        DH.SALES_FG_YEAR ,    
        S.TEAM_NAME,
        A.CUSTOMER_NAME,      
        SO.BUYYER,           
        A.SALE_NAME,       
        A.SO_NO,
        DH.QN_NO,
        DH.SO_RESERVE,
        DH.SO_BILL_REF,
        A.KP_NO,
        A.ITEM_CODE,
        A.YARN_LOT,
        SUM(A.gf_STOCK_KG) PROD_KG,
        SUM(A.RESERVE_ALL) RESERVE_KG,       
        SUM(A.GF_ONHAND_KG) ONHAND_KG,         
        SUM(A.BALANCE_KG) BALANCE_KG,
        B.MATERIAL_YARN,         
        B.O_FN_GM GM2,          
        B.O_FN_OPEN "WIDTH (อบผ่า)",
        B.O_FN_TUBULAR "WIDTH (อบกลม)",  
        B.ITEM_STRUCTURE,    
        B.O_SL SL,
        B.O_MACHINE_GROUP MC_GROUP ,
        B.O_GAUGE GAUGE,
       (select  max(dfs.MOVEMENT_ACCOUNT)
             from nyf.fmit_gf_warehouse@NYFPHET.WORLD dfs
             where dfs.kp_no = A.KP_NO
                 and dfs.item_code = A.ITEM_CODE
                 and dfs.yarn_lot = A.YARN_LOT
                 and gf_locked = 'N'
                 and dfs.MOVEMENT_TYPE is not null
        ) REASON,
             ( select  max(dfs.MOVEMENT_TYPE)
             from nyf.fmit_gf_warehouse@NYFPHET.WORLD dfs
             where dfs.kp_no = A.KP_NO
                 and dfs.item_code = A.ITEM_CODE
                 and dfs.yarn_lot = A.YARN_LOT
                 and gf_locked = 'N'
                 and dfs.MOVEMENT_TYPE is not null
         ) REASON_SUB,
            SUM(A.GRADE_X) GRADE_X,
            SUM(A.GRADE_X_ONH) GRADE_X_ONH,
       0 TRANS_KNIT,
       GET_QA_REASON(A.KP_NO) QA_REASON,
       GET_QA_REMARK(A.KP_NO) QA_REMARK,
       GET_DEAD_REASON (A.KP_NO) DEADNON_REASON,
       GET_DEAD_REMARK (A.KP_NO) DEADNON_MAIN,
       A.LOCATOR_ID
 FROM       sf5.FMIT_ITEM B,
              SF5.SMIV_GF_CONTROL_ONH_V_LOC A,
              sf5.DFORA_SALE S,
              sf5.SMIT_SO_HEADER SO,
              SF5.DUMMY_SO_HEADERS DH
 WHERE B.ITEM_CODE = A.ITEM_CODE
   AND A.SO_NO = SO.SO_NO(+)
   AND A.SO_NO = DH.ORA_ORDER_NUMBER(+)
   AND A.GF_ONHAND_KG > 0
   AND A.ITEM_CODE NOT LIKE 'C%' 
   AND A.KP_NO LIKE 'K%'
   AND A.KP_NO NOT LIKE 'KP%_NA'
   AND A.SALE_ID = S.SALE_ID(+)
 GROUP BY  DH.SO_TYPE,
        A.STOCK_SITE,
        SO.CUSTOMER_FG,
        SO.CUSTOMER_YEAR,
        A.KNIT_WW,
        A.KNIT_YEAR,   
        S.TEAM_NAME,
        A.CUSTOMER_NAME,       
        SO.BUYYER,          
        A.SALE_NAME,       
        A.SO_NO,
        A.KP_NO,
        A.ITEM_CODE,
        A.YARN_LOT,
        B.MATERIAL_YARN,         
        B.O_FN_GM,  
        B.O_FN_OPEN,
        B.O_FN_TUBULAR,
        B.ITEM_STRUCTURE,    
        B.O_SL,
        B.O_MACHINE_GROUP ,
        B.O_GAUGE,
        B.ITEM_SUB_STRU,                                                                   
        B.ITEM_MATERIAL,              
        B.FABRIC_GROUP,
        DH.QN_NO,
        DH.SO_RESERVE,
        DH.SO_BILL_REF,
        DH.EDD_YEAR2,
        DH.EDD_WEEK2,
        DH.SALES_FG_WEEK ,       
        DH.SALES_FG_YEAR ,
        A.LOCATOR_ID
  UNION
  SELECT  DH.SO_TYPE, 
         A.STOCK_SITE,
      GET_WEEK_RANGE(TO_NUMBER(LTRIM(TO_CHAR(SYSDATE,'YYYYIW'))),
       to_number(rtrim((  nvl(A.KNIT_YEAR,DECODE(  SUBSTR(KP_NO,LENGTH(KP_NO)-2,3),'FQC','20'||SUBSTR(KP_NO,3,2)) ) ))||
       ltrim(to_char(  nvl(A.KNIT_WW,DECODE(  SUBSTR(KP_NO,LENGTH(KP_NO)-2,3),'FQC',SUBSTR(KP_NO,5,2)) ) ,'00')))) AGING_WW,  
        NVL(A.KNIT_WW,  DECODE(  SUBSTR(KP_NO,LENGTH(KP_NO)-2,3),'FQC',SUBSTR(KP_NO,5,2))) KNIT_WW,
        NVL(A.KNIT_YEAR,DECODE(  SUBSTR(KP_NO,LENGTH(KP_NO)-2,3),'FQC','20'||SUBSTR(KP_NO,3,2)) ) KNIT_YEAR,      
        SO.CUSTOMER_FG FG_WW,
        SO.CUSTOMER_YEAR FG_YEAR,
        DH.EDD_YEAR2,
        DH.EDD_WEEK2,
        DH.SALES_FG_WEEK ,       
        DH.SALES_FG_YEAR ,    
        S.TEAM_NAME,
        A.CUSTOMER_NAME,      
        SO.BUYYER,           
        A.SALE_NAME,       
        A.SO_NO,
        DH.QN_NO,
        DH.SO_RESERVE,
        DH.SO_BILL_REF,
        A.KP_NO,
        A.ITEM_CODE,
        A.YARN_LOT,
        SUM(A.gf_STOCK_KG) PROD_KG,   
        (  SELECT SUM(GF_QTY) FROM  SF5.DFIT_GF_BOM 
            WHERE KP_NO=A.KP_NO AND KP_ITEM=A.ITEM_CODE
            AND YARN_LOT=A.YARN_LOT   AND NVL(STK_SO_NO,SO_NO)=A.SO_NO
            AND CANCEL_ACTIVE='N' AND BOM_APPROVED='Y' ) RESERVE_KG, 
        NVL(SUM(A.GF_ONHAND_KG),0)+NVL(SUM(A.GRADE_X_ONH),0) ONHAND_KG,    
       NVL(SUM(A.gf_STOCK_KG),0)-NVL(
       (  SELECT SUM(GF_QTY) FROM  SF5.DFIT_GF_BOM 
            WHERE KP_NO=A.KP_NO AND KP_ITEM=A.ITEM_CODE
            AND YARN_LOT=A.YARN_LOT   AND NVL(STK_SO_NO,SO_NO)=A.SO_NO
            AND CANCEL_ACTIVE='N' AND BOM_APPROVED='Y' ) ,0) BALANCE_KG,
        B.MATERIAL_YARN,         
        B.O_FN_GM GM2,          
        B.O_FN_OPEN "WIDTH (อบผ่า)",
        B.O_FN_TUBULAR "WIDTH (อบกลม)",  
        B.ITEM_STRUCTURE,    
        B.O_SL SL,
        B.O_MACHINE_GROUP MC_GROUP ,
        B.O_GAUGE GAUGE,
       (select  max(dfs.MOVEMENT_ACCOUNT)
             from nyf.fmit_gf_warehouse@NYFPHET.WORLD dfs
             where dfs.kp_no = A.KP_NO
                 and dfs.item_code = A.ITEM_CODE
                 and dfs.yarn_lot = A.YARN_LOT
                 and gf_locked = 'N'
                 and dfs.MOVEMENT_TYPE is not null
            ) REASON,
             ( select  max(dfs.MOVEMENT_TYPE)
             from nyf.fmit_gf_warehouse@NYFPHET.WORLD dfs
             where dfs.kp_no = A.KP_NO
                 and dfs.item_code = A.ITEM_CODE
                 and dfs.yarn_lot = A.YARN_LOT
                 and gf_locked = 'N'
                 and dfs.MOVEMENT_TYPE is not null
            ) REASON_SUB,
            SUM(A.GRADE_X) GRADE_X,
            SUM(A.GRADE_X_ONH) GRADE_X_ONH,
       0 TRANS_KNIT,
       GET_QA_REASON(A.KP_NO) QA_REASON,
       GET_QA_REMARK(A.KP_NO) QA_REMARK,
       GET_DEAD_REASON (A.KP_NO) DEADNON_REASON,
       GET_DEAD_REMARK (A.KP_NO) DEADNON_MAIN,
       A.LOCATOR_ID
 FROM  sf5.FMIT_ITEM B,
              SF5.SMIV_GF_CONTROL_ONH_VNA_LOC  A,
              sf5.DFORA_SALE S,
              sf5.SMIT_SO_HEADER SO,
              SF5.DUMMY_SO_HEADERS DH
 WHERE B.ITEM_CODE = A.ITEM_CODE
   AND A.SO_NO = SO.SO_NO(+)
   AND A.SO_NO = DH.ORA_ORDER_NUMBER(+)
   AND  ( A.GF_ONHAND_KG > 0  OR  NVL(A.GRADE_X_ONH,0) > 0)
   AND A.ITEM_CODE NOT LIKE 'C%' 
   AND A.KP_NO LIKE 'K%'
   AND A.SALE_ID = S.SALE_ID(+)
 GROUP BY  DH.SO_TYPE,
        A.STOCK_SITE,
        SO.CUSTOMER_FG,
        SO.CUSTOMER_YEAR,
        A.KNIT_WW,
        A.KNIT_YEAR,   
        S.TEAM_NAME,
        A.CUSTOMER_NAME,       
        SO.BUYYER,          
        A.SALE_NAME,       
        A.SO_NO,
        A.KP_NO,
        A.ITEM_CODE,
        A.YARN_LOT,
        B.MATERIAL_YARN,         
        B.O_FN_GM,  
        B.O_FN_OPEN,
        B.O_FN_TUBULAR,
        B.ITEM_STRUCTURE,    
        B.O_SL,
        B.O_MACHINE_GROUP ,
        B.O_GAUGE,
        B.ITEM_SUB_STRU,                                                                   
        B.ITEM_MATERIAL,              
        B.FABRIC_GROUP,
        DH.QN_NO,
        DH.SO_RESERVE,
        DH.SO_BILL_REF,
        DH.EDD_YEAR2,
        DH.EDD_WEEK2,
        DH.SALES_FG_WEEK ,       
        DH.SALES_FG_YEAR,
        A.LOCATOR_ID
        ) """

  _filename = r"C:\QVD_DATA\OPE_FABRIC\DATA_GF_ONHAND_BY_LOC.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_GF_ONHAND_BY_LOC")
  sendLine("COMPLETE CLS_DATA_GF_ONHAND_BY_LOC")

#############################################

#############################################
class CLS_DATA_KP_KNIT_QTY(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_KP_KNIT_QTY()


def DATA_KP_KNIT_QTY():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_KP_KNIT_QTY")
  sql =""" SELECT KP_NO, sum(TOTAL_QTY) TOTAL_QTY  
           FROM FMIT_KP_PO_DETAIL
           group by KP_NO """

  _filename = r"C:\QVD_DATA\OPE_FABRIC\DATA_KP_KNIT_QTY.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_KP_KNIT_QTY")
  sendLine("COMPLETE CLS_DATA_KP_KNIT_QTY")

#############################################

#############################################
class CLS_DATA_RECEIPT_WH_byKP(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_RECEIPT_WH_byKP()


def DATA_RECEIPT_WH_byKP():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_RECEIPT_WH_byKP")
  sql =""" select KP_NO, ITEM_CODE, YARN_LOT, sum(FABRIC_WEIGHT)
           from FMIT_GF_WAREHOUSE 
           group by KP_NO, ITEM_CODE, YARN_LOT """

  _filename = r"C:\QVD_DATA\OPE_FABRIC\DATA_RECEIPT_WH_byKP.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_RECEIPT_WH_byKP")
  sendLine("COMPLETE CLS_DATA_RECEIPT_WH_byKP")

#############################################

#############################################
class CLS_DATA_ShipOut_WH_byKP(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_ShipOut_WH_byKP()


def DATA_ShipOut_WH_byKP():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_ShipOut_WH_byKP")
  sql =""" select KP_NO, ITEM_CODE, YARN_LOT, sum(FABRIC_WEIGHT)
           from FMIT_GF_WAREHOUSE 
           group by KP_NO, ITEM_CODE, YARN_LOT """

  _filename = r"C:\QVD_DATA\OPE_FABRIC\DATA_ShipOut_WH_byKP.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_ShipOut_WH_byKP")
  sendLine("COMPLETE CLS_DATA_ShipOut_WH_byKP")

#############################################

#############################################
class CLS_DATA_WIP_byKP_LOT(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_WIP_byKP_LOT()


def DATA_WIP_byKP_LOT():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_WIP_byKP_LOT")
  sql =""" SELECT KP_NO, ITEM_CODE, YARN_LOT, SUM(DECODE(SUBSTR(ITEM_CODE,1,1),'F',NVL(FABRIC_WEIGHT,0),NVL(SCH_ITEM_QTY,0) ))  TOTAL_QTY
           FROM FMIT_KP_FABRIC F
           WHERE NOT EXISTS ( SELECT  FABRIC_ID FROM FMIT_GF_WAREHOUSE W WHERE W.FABRIC_ID=F.FABRIC_ID)
           GROUP by KP_NO, ITEM_CODE, YARN_LOT """

  _filename = r"C:\QVD_DATA\OPE_FABRIC\DATA_WIP_byKP_LOT.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_WIP_byKP_LOT")
  sendLine("COMPLETE CLS_DATA_WIP_byKP_LOT")

#############################################

#############################################
class CLS_DATA_GF_ONHAND_DEFECT(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_GF_ONHAND_DEFECT()


def DATA_GF_ONHAND_DEFECT():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_GF_ONHAND_DEFECT")
  sql =""" SELECT  DECODE(SUBSTR(SO.SO_NO,1,1),'9','บิลจอง','บิลสี') SO_TYPE,
              GET_WEEK_RANGE(TO_NUMBER(LTRIM(TO_CHAR(SYSDATE,'YYYYIW'))),KNIT_YEARWW) AGING_WW,
              A.KNIT_YEAR,
              SO.CUSTOMER_FG FG_WW, SO.CUSTOMER_YEAR FG_YEAR,
              S.TEAM_NAME,
              A.CUSTOMER_NAME, SO.BUYYER, A.SALE_NAME, A.SO_NO,
              (SELECT QN_NO FROM SF5.DUMMY_SO_HEADERS DM WHERE DM.ORA_ORDER_NUMBER=A.SO_NO) QN_NO,
              A.KP_NO,
              A.ITEM_CODE,
              ( SELECT CATEGORY_GROUP
                FROM SF5.ERP_ITEM_CATEGORY C WHERE ITEM_CATEGORY=B.ITEM_CATEGORY) ITEM_CATEGORY_SALE, 
              A.YARN_LOT,
              A.LOCATOR_ID, A.QC_GRADE, A.SET_SPECIFIC,
              A.PROD_QTY PROD_KG, A.ONHAND_WEIGHT ONHAND_KG, 
              B.MATERIAL_YARN,         
              B.O_FN_GM GM2,          
              B.O_FN_OPEN "WIDTH (อบผ่า)",
              B.O_FN_TUBULAR "WIDTH (อบกลม)",  
              B.ITEM_STRUCTURE,    
              B.O_SL SL,
              B.O_MACHINE_GROUP MC_GROUP ,
              B.O_GAUGE GAUGE,
             (SELECT RE.PB_REASON FROM SF5.DFIT_FOLLOW_RESERVE RE
              WHERE RE.SO_NO = A.SO_NO
              AND RE.KP_NO = A.KP_NO 
              AND CAP_WEEK = A.KNIT_WW
              AND CAP_YEAR = A.KNIT_YEAR
              ) REASON,
             (SELECT RE.PB_REASON_SUB FROM SF5.DFIT_FOLLOW_RESERVE RE
              WHERE RE.SO_NO = A.SO_NO
              AND RE.KP_NO = A.KP_NO 
              AND CAP_WEEK = A.KNIT_WW
              AND CAP_YEAR = A.KNIT_YEAR
              ) REASON_SUB,
              A.RECEIVE_DATE,
             GET_QA_REASON(A.KP_NO) QA_REASON,
             GET_QA_REMARK(A.KP_NO) QA_REMARK
       FROM SF5.SMIV_GF_WAREHOUSE_ONH_V A,
           SF5.FMIT_ITEM B,SF5.SMIT_SO_HEADER SO,SF5.DFORA_SALE S,
           sf5.fmit_locator L
       WHERE A.ITEM_CODE=B.ITEM_CODE
       AND A.SO_NO =SO.SO_NO(+)
       AND A.SALE_ID=S.SALE_ID(+)
       AND A.STOCK_SITE LIKE '1%' 
       AND A.ITEM_CODE NOT LIKE 'C%' 
       AND A.KP_NO LIKE 'KP%'
       AND A.LOCATOR_ID = L.LOCATOR_ID
       AND L.JOB_LOCATOR_ACTIVE = 'N' """

  _filename = r"C:\QVD_DATA\OPE_FABRIC\DATA_GF_ONHAND_DEFECT.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_GF_ONHAND_DEFECT")
  sendLine("COMPLETE CLS_DATA_GF_ONHAND_DEFECT")

#############################################



threads = []

thread1 = CLS_DATA_GF_ONHAND_BY_LOC();thread1.start();threads.append(thread1)
thread2 = CLS_DATA_KP_KNIT_QTY();thread2.start();threads.append(thread2)
thread3 = CLS_DATA_RECEIPT_WH_byKP();thread3.start();threads.append(thread3)
thread4 = CLS_DATA_ShipOut_WH_byKP();thread4.start();threads.append(thread4)
thread5 = CLS_DATA_WIP_byKP_LOT();thread5.start();threads.append(thread5)
thread6 = CLS_DATA_GF_ONHAND_DEFECT();thread6.start();threads.append(thread6)


for t in threads:
    t.join()
print ("COMPLETE")

