import cx_Oracle
import csv
import os
from pathlib import Path
import requests
from datetime import datetime
import threading
import time


oracle_client = "C:\instantclient_19_5"
os.environ["ORACLE_HOME"]=oracle_client
os.environ["PATH"]=oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"]="AMERICAN_AMERICA.TH8TISASCII"


###########################################
class CLS_step1step2_vn(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        step1step2_vn()
        
def step1step2_vn():
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="VN")
  conn = cx_Oracle.connect(user="vn", password="vn", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""SELECT C.SHIPMENT_DATE,C.CUST_CODE,OE.CUST_DESC(C.CUST_CODE)CUST_NAME,C.BRAND_CODE,OE.BRAND_DESC(C.OU_CODE,C.BRAND_CODE) BRAND_NAME,A.OU_CODE,A.SO_YEAR,A.SO_NO,C.ORDER_TYPE,C.GMT_TYPE,C.SAM,C.SCORE SAMIE,OE.GET_SO_QTY(C.OU_CODE,C.SO_YEAR,C.SO_NO) ORDERQTY,
CASE WHEN C.SCORE >1 THEN OE.GET_SO_QTY(C.OU_CODE,C.SO_YEAR,C.SO_NO) *C.SCORE ELSE OE.GET_SO_QTY(C.OU_CODE,C.SO_YEAR,C.SO_NO) *C.SAM END AS ORDERMIN,
SUM(A.STEP1_PCS) STEP1QTY,
CASE WHEN C.SCORE >1 THEN NVL(SUM(A.STEP1_PCS),0)  *C.SCORE ELSE NVL(SUM(A.STEP1_PCS),0)  *C.SAM END AS STEP1MIN,
--SUM(A.ALLOCATE_QTY) ALLOCATE_QTY,
--CASE WHEN C.SCORE >1 THEN NVL(SUM(A.ALLOCATE_QTY),0)  *C.SCORE ELSE NVL(SUM(A.ALLOCATE_QTY),0)  *C.SAM END AS ALLOCATEMIN,
GET_STEP2_QTY(A.OU_CODE,A.SO_YEAR,A.SO_NO) CUTPLAN_QTY,
CASE WHEN C.SCORE >1 THEN NVL(GET_STEP2_QTY(A.OU_CODE,A.SO_YEAR,A.SO_NO) ,0)  *C.SCORE ELSE NVL(GET_STEP2_QTY(A.OU_CODE,A.SO_YEAR,A.SO_NO) ,0)  *C.SAM END AS CUTPLANMIN,
SUM(A.STEP1_PCS)-NVL(GET_STEP2_QTY(A.OU_CODE,A.SO_YEAR,A.SO_NO) ,0) BALANCEQTY,
CASE WHEN C.SCORE >1 THEN NVL(SUM(A.STEP1_PCS)- NVL(GET_STEP2_QTY(A.OU_CODE,A.SO_YEAR,A.SO_NO) ,0),0)  *NVL(C.SCORE,0) ELSE NVL(SUM(A.STEP1_PCS)-NVL(GET_STEP2_QTY(A.OU_CODE,A.SO_YEAR,A.SO_NO) ,0),0)  *NVL(C.SAM,0) END AS BALANCMIN,
GET_STEP2_CUT_PLANT(A.OU_CODE,A.SO_YEAR,A.SO_NO)STEP2_CUT_PLAN,min(MPS_S1_DATE) STEP1_MINDATE,
MAX(MPS_S1_DATE) STEP1_MAXDATE,
GET_STEP2_MINDATE(A.OU_CODE,A.SO_YEAR,A.SO_NO)STEP2_MINDATE,GET_STEP2_MAXDATE(A.OU_CODE,A.SO_YEAR,A.SO_NO)STEP2_MAXDATE
FROM NYG_MPS_STEP1 A,OE_SO C
WHERE 
A.OU_CODE=C.OU_CODE
AND A.SO_YEAR=C.SO_YEAR
AND A.SO_NO=C.SO_NO
AND C.SO_STATUS <>'C'
AND A.MPS_CANCEL<>'Y'
AND A.OU_CODE='NVN'
AND A.SO_YEAR >=TO_CHAR(SYSDATE,'YY')-5
GROUP BY A.OU_CODE,A.SO_YEAR,A.SO_NO,C.OU_CODE,C.SO_YEAR,C.SO_NO,C.SHIPMENT_DATE,C.CUST_CODE,C.BRAND_CODE,C.ORDER_TYPE,C.GMT_TYPE,C.SAM,C.SCORE""")
  
  _csv = r"C:\QVDatacenter\SCM\GARMENT\VN\step1step2_vn.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  conn.close()

###########################################
class CLS_step1step2_NYG(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        step1step2_NYG()
        
def step1step2_NYG():
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""SELECT C.SHIPMENT_DATE,C.CUST_CODE,OE.CUST_DESC(C.CUST_CODE)CUST_NAME,C.BRAND_CODE,OE.BRAND_DESC(C.OU_CODE,C.BRAND_CODE) BRAND_NAME,A.OU_CODE,A.SO_YEAR,A.SO_NO,C.ORDER_TYPE,C.GMT_TYPE,C.SAM,C.SCORE SAMIE,OE.GET_SO_QTY(C.OU_CODE,C.SO_YEAR,C.SO_NO) ORDERQTY,
CASE WHEN C.SCORE >1 THEN OE.GET_SO_QTY(C.OU_CODE,C.SO_YEAR,C.SO_NO) *C.SCORE ELSE OE.GET_SO_QTY(C.OU_CODE,C.SO_YEAR,C.SO_NO) *C.SAM END AS ORDERMIN,
SUM(A.STEP1_PCS) STEP1QTY,
CASE WHEN C.SCORE >1 THEN NVL(SUM(A.STEP1_PCS),0)  *C.SCORE ELSE NVL(SUM(A.STEP1_PCS),0)  *C.SAM END AS STEP1MIN,
--SUM(A.ALLOCATE_QTY) ALLOCATE_QTY,
--CASE WHEN C.SCORE >1 THEN NVL(SUM(A.ALLOCATE_QTY),0)  *C.SCORE ELSE NVL(SUM(A.ALLOCATE_QTY),0)  *C.SAM END AS ALLOCATEMIN,
GET_STEP2_QTY(A.OU_CODE,A.SO_YEAR,A.SO_NO) CUTPLAN_QTY,
CASE WHEN C.SCORE >1 THEN NVL(GET_STEP2_QTY(A.OU_CODE,A.SO_YEAR,A.SO_NO) ,0)  *C.SCORE ELSE NVL(GET_STEP2_QTY(A.OU_CODE,A.SO_YEAR,A.SO_NO) ,0)  *C.SAM END AS CUTPLANMIN,
SUM(A.STEP1_PCS)-NVL(GET_STEP2_QTY(A.OU_CODE,A.SO_YEAR,A.SO_NO) ,0) BALANCEQTY,
CASE WHEN C.SCORE >1 THEN NVL(SUM(A.STEP1_PCS)- NVL(GET_STEP2_QTY(A.OU_CODE,A.SO_YEAR,A.SO_NO) ,0),0)  *NVL(C.SCORE,0) ELSE NVL(SUM(A.STEP1_PCS)-NVL(GET_STEP2_QTY(A.OU_CODE,A.SO_YEAR,A.SO_NO) ,0),0)  *NVL(C.SAM,0) END AS BALANCMIN,
GET_STEP2_CUT_PLANT(A.OU_CODE,A.SO_YEAR,A.SO_NO)STEP2_CUT_PLAN,min(MPS_S1_DATE) STEP1_MINDATE,
MAX(MPS_S1_DATE) STEP1_MAXDATE,
GET_STEP2_MINDATE(A.OU_CODE,A.SO_YEAR,A.SO_NO)STEP2_MINDATE,GET_STEP2_MAXDATE(A.OU_CODE,A.SO_YEAR,A.SO_NO)STEP2_MAXDATE
FROM NYG_MPS_STEP1 A,OE_SO C
WHERE 
A.OU_CODE=C.OU_CODE
AND A.SO_YEAR=C.SO_YEAR
AND A.SO_NO=C.SO_NO
AND C.SO_STATUS <>'C'
AND A.MPS_CANCEL<>'Y'
AND A.OU_CODE='N03'
AND A.SO_YEAR >=TO_CHAR(SYSDATE,'YY')-5
GROUP BY A.OU_CODE,A.SO_YEAR,A.SO_NO,C.OU_CODE,C.SO_YEAR,C.SO_NO,C.SHIPMENT_DATE,C.CUST_CODE,C.BRAND_CODE,C.ORDER_TYPE,C.GMT_TYPE,C.SAM,C.SCORE""")
  
  _csv = r"C:\QVDatacenter\SCM\GARMENT\NYG\step1step2_nyg.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  conn.close()

###########################################
class CLS_step1step2_TRM(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        step1step2_TRM()
        
def step1step2_TRM():
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYGM", password="NYGM", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""SELECT C.SHIPMENT_DATE,C.CUST_CODE,trm.OE.CUST_DESC@trm.world(C.CUST_CODE)CUST_NAME,C.BRAND_CODE,trm.OE.BRAND_DESC@trm.world(C.OU_CODE,C.BRAND_CODE) BRAND_NAME,A.OU_CODE,A.SO_YEAR,A.SO_NO,C.ORDER_TYPE,C.GMT_TYPE,C.SAM,C.SCORE SAMIE,trm.OE.GET_SO_QTY@trm.world(C.OU_CODE,C.SO_YEAR,C.SO_NO) ORDERQTY,
CASE WHEN C.SCORE >1 THEN trm.OE.GET_SO_QTY@trm.world(C.OU_CODE,C.SO_YEAR,C.SO_NO) *C.SCORE ELSE trm.OE.GET_SO_QTY@trm.world(C.OU_CODE,C.SO_YEAR,C.SO_NO) *C.SAM END AS ORDERMIN,
SUM(A.STEP1_PCS) STEP1QTY,
CASE WHEN C.SCORE >1 THEN NVL(SUM(A.STEP1_PCS),0)  *C.SCORE ELSE NVL(SUM(A.STEP1_PCS),0)  *C.SAM END AS STEP1MIN,
--SUM(A.ALLOCATE_QTY) ALLOCATE_QTY,
--CASE WHEN C.SCORE >1 THEN NVL(SUM(A.ALLOCATE_QTY),0)  *C.SCORE ELSE NVL(SUM(A.ALLOCATE_QTY),0)  *C.SAM END AS ALLOCATEMIN,
trm.GET_STEP2_QTY@trm.world(A.OU_CODE,A.SO_YEAR,A.SO_NO) CUTPLAN_QTY,
CASE WHEN C.SCORE >1 THEN NVL(trm.GET_STEP2_QTY@trm.world(A.OU_CODE,A.SO_YEAR,A.SO_NO) ,0)  *C.SCORE ELSE NVL(trm.GET_STEP2_QTY@trm.world(A.OU_CODE,A.SO_YEAR,A.SO_NO) ,0)  *C.SAM END AS CUTPLANMIN,
SUM(A.STEP1_PCS)-NVL(trm.GET_STEP2_QTY@trm.world(A.OU_CODE,A.SO_YEAR,A.SO_NO) ,0) BALANCEQTY,
CASE WHEN C.SCORE >1 THEN NVL(SUM(A.STEP1_PCS)- NVL(trm.GET_STEP2_QTY@trm.world(A.OU_CODE,A.SO_YEAR,A.SO_NO) ,0),0)  *NVL(C.SCORE,0) ELSE NVL(SUM(A.STEP1_PCS)-NVL(trm.GET_STEP2_QTY@trm.world(A.OU_CODE,A.SO_YEAR,A.SO_NO) ,0),0)  *NVL(C.SAM,0) END AS BALANCMIN,
trm.GET_STEP2_CUT_PLANT@trm.world(A.OU_CODE,A.SO_YEAR,A.SO_NO) STEP2_CUT_PLAN,min(MPS_S1_DATE) STEP1_MINDATE,
MAX(MPS_S1_DATE) STEP1_MAXDATE,
trm.GET_STEP2_MINDATE@trm.world(A.OU_CODE,A.SO_YEAR,A.SO_NO) STEP2_MINDATE,trm.GET_STEP2_MAXDATE@trm.world(A.OU_CODE,A.SO_YEAR,A.SO_NO)STEP2_MAXDATE
FROM trm.NYG_MPS_STEP1@trm.world A,trm.OE_SO@trm.world C
WHERE 
A.OU_CODE=C.OU_CODE
AND A.SO_YEAR=C.SO_YEAR
AND A.SO_NO=C.SO_NO
AND C.SO_STATUS <>'C'
AND A.MPS_CANCEL<>'Y'
AND A.OU_CODE='N03'
AND A.SO_YEAR >=TO_CHAR(SYSDATE,'YY')-1
GROUP BY A.OU_CODE,A.SO_YEAR,A.SO_NO,C.OU_CODE,C.SO_YEAR,C.SO_NO,C.SHIPMENT_DATE,C.CUST_CODE,C.BRAND_CODE,C.ORDER_TYPE,C.GMT_TYPE,C.SAM,C.SCORE""")
  
  _csv = r"C:\QVDatacenter\SCM\GARMENT\TRM\step1step2_TRM.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  conn.close()


###########################################

class CLS_OE_SO_COSTSHEET_ALLBU(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        OE_SO_COSTSHEET_ALLBU()
        
def OE_SO_COSTSHEET_ALLBU():
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""select 'NYG' BU,a.ou_code,a.so_year,a.so_no,a.COST_SHEETID,b.style_code,b.style_ref,b.cust_code,b.brand_code,b.dept_code from OE_SO_COSTSHEET a,oe_so b where a.ou_code=b.ou_code and a.so_year=b.so_year and a.so_no=b.so_no and a.cost_sheetid is not null and b.so_year >=to_char(sysdate,'YY')-3 and  b.so_status<>'C' and b.ou_code='N03'
UNION all
select 'TRM' BU,a.ou_code,a.so_year,a.so_no,a.COST_SHEETID,b.style_code,b.style_ref,b.cust_code,b.brand_code,b.dept_code from trm.OE_SO_COSTSHEET@TRM.WORLD a,trm.oe_so@TRM.WORLD b where a.ou_code=b.ou_code and a.so_year=b.so_year and a.so_no=b.so_no and a.cost_sheetid is not null and b.so_status<>'C' and b.ou_code='N03'
UNION all
select 'NYV' BU,a.ou_code,a.so_year,a.so_no,a.COST_SHEETID,b.style_code,b.style_ref,b.cust_code,b.brand_code,b.dept_code from vn.OE_SO_COSTSHEET@vnsqprod.WORLD a,vn.oe_so@vnsqprod.WORLD b where a.ou_code=b.ou_code and a.so_year=b.so_year and a.so_no=b.so_no and a.cost_sheetid is not null and b.so_status<>'C'and b.ou_code='NVN'
UNION all
select 'GRW' BU,a.ou_code,a.so_year,a.so_no,a.COST_SHEETID,b.style_code,b.style_ref,b.cust_code,b.brand_code,b.dept_code from nygm.OE_SO_COSTSHEET@NGWSP.WORLD a,nygm.oe_so@NGWSP.WORLD  b where a.ou_code=b.ou_code and a.so_year=b.so_year and a.so_no=b.so_no and a.cost_sheetid is not null and b.so_status<>'C' and b.ou_code='N03'
""")
  
  _csv = r"C:\QVD_DATA\COM_GARMENT\ALLBU\OE_SO_COSTSHEET_ALLBU.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  conn.close()

########################################### WCS close so production k'nok sirada
'''
class CLS_WCS_CLOSE_SOPRO_ALLBU(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        WCS_CLOSE_SOPRO_ALLBU()
        
def WCS_CLOSE_SOPRO_ALLBU():
  my_dsn = cx_Oracle.makedsn("172.16.6.82",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""select BU, SO_NO, SO_YEAR, BRAND, SHIPMENT_HEAD, QTY, EXPORT, CLOSEPRO,PRO_UPD CLOSEPRO_UPD, CLOSEACC,GET_SO_EXPORT_MAXDATE(so_year,so_no) MaxExportDate
from(SELECT 'NYG1' BU,a.so_no, a.so_year,
           BRAND_DESC('N03',A.brand_code) brand,
           GET_SHIPMENT_DATE(a.so_no,a.so_year)  shipment_head,
         GET_ORDER_QTY('N03',a.so_YEAR,a.so_NO) qty,
                (  NVL ((SELECT SUM (NVL (jj.pcs_qty, 0))
                     FROM dfit_fg_issue_detail jj
                    WHERE jj.so_no = a.so_no
                      AND jj.so_year = a.so_year
                      AND jj.issue_type = 'EXPORT'),
                  0
                 )
           - NVL ((SELECT SUM (NVL (ee1.pcs_qty, 0))
                     FROM dfit_fg_detail ee1
                    WHERE ee1.so_no = a.so_no
                      AND ee1.so_year = a.so_year
                      AND ee1.issue_type = 'ADJUST-EXPORT'
                      AND ee1.rec_date IS NOT NULL),
                  0
                 )
          ) export ,a.CLOSE_ORDER_DATE CLOSEPRO , a.UPD_BY PRO_UPD,GET_SO_CLOSE_DATE_ACCOUNT(a.so_year,a.so_no) CLOSEACC
     FROM oe_trans_head a,OE_SO_v b
    WHERE  
    a.so_year=b.so_year
    and a.so_no=b.so_no
    and b.ou_code='N03'
    and b.SO_STATUS <>'C'
    and a.so_year >= TO_CHAR (get_sysdate, 'YY') - 2
    union all
    SELECT 'NYG2' BU,a.so_no, a.so_year,
           BRAND_DESC('N03',A.brand_code) brand,
           GET_SHIPMENT_DATE(a.so_no,a.so_year)  shipment_head,
         GET_ORDER_QTY('N03',a.so_YEAR,a.so_NO) qty,
                (  NVL ((SELECT SUM (NVL (jj.pcs_qty, 0))
                     FROM nyg_pho.dfit_fg_issue_detail@nyg2.world jj
                    WHERE jj.so_no = a.so_no
                      AND jj.so_year = a.so_year
                      AND jj.issue_type = 'EXPORT'),
                  0
                 )
           - NVL ((SELECT SUM (NVL (ee1.pcs_qty, 0))
                     FROM nyg_pho.dfit_fg_detail@nyg2.world ee1
                    WHERE ee1.so_no = a.so_no
                      AND ee1.so_year = a.so_year
                      AND ee1.issue_type = 'ADJUST-EXPORT'
                      AND ee1.rec_date IS NOT NULL),
                  0
                 )
          ) export ,a.CLOSE_ORDER_DATE CLOSEPRO , a.UPD_BY PRO_UPD,GET_SO_CLOSE_DATE_ACCOUNT(a.so_year,a.so_no) CLOSEACC
     FROM nyg_pho.oe_trans_head@nyg2.world a,OE_SO_v b
    WHERE  
    a.so_year=b.so_year
    and a.so_no=b.so_no
    and b.ou_code='N03'
    and b.SO_STATUS <>'C'
    and a.so_year >= TO_CHAR (get_sysdate, 'YY') - 2
    union all
    SELECT 'NYG3' BU,a.so_no, a.so_year,
           BRAND_DESC('N03',A.brand_code) brand,
           GET_SHIPMENT_DATE(a.so_no,a.so_year)  shipment_head,
         GET_ORDER_QTY('N03',a.so_YEAR,a.so_NO) qty,
                (  NVL ((SELECT SUM (NVL (jj.pcs_qty, 0))
                     FROM nyg3.dfit_fg_issue_detail jj
                    WHERE jj.so_no = a.so_no
                      AND jj.so_year = a.so_year
                      AND jj.issue_type = 'EXPORT'),
                  0
                 )
           - NVL ((SELECT SUM (NVL (ee1.pcs_qty, 0))
                     FROM nyg3.dfit_fg_detail ee1
                    WHERE ee1.so_no = a.so_no
                      AND ee1.so_year = a.so_year
                      AND ee1.issue_type = 'ADJUST-EXPORT'
                      AND ee1.rec_date IS NOT NULL),
                  0
                 )
          ) export ,a.CLOSE_ORDER_DATE CLOSEPRO, a.UPD_BY PRO_UPD,GET_SO_CLOSE_DATE_ACCOUNT(a.so_year,a.so_no) CLOSEACC
     FROM nyg3.oe_trans_head a,OE_SO_v b
    WHERE  
    a.so_year=b.so_year
    and a.so_no=b.so_no
    and b.ou_code='N03'
    and b.SO_STATUS <>'C'
    and a.so_year >= TO_CHAR (get_sysdate, 'YY') - 2
        union all
    SELECT 'NYG4' BU,a.so_no, a.so_year,
           BRAND_DESC('N03',A.brand_code) brand,
           GET_SHIPMENT_DATE(a.so_no,a.so_year)  shipment_head,
         GET_ORDER_QTY('N03',a.so_YEAR,a.so_NO) qty,
                (  NVL ((SELECT SUM (NVL (jj.pcs_qty, 0))
                     FROM nyg4.dfit_fg_issue_detail@nyg4.world jj
                    WHERE jj.so_no = a.so_no
                      AND jj.so_year = a.so_year
                      AND jj.issue_type = 'EXPORT'),
                  0
                 )
           - NVL ((SELECT SUM (NVL (ee1.pcs_qty, 0))
                     FROM nyg4.dfit_fg_detail@nyg4.world ee1
                    WHERE ee1.so_no = a.so_no
                      AND ee1.so_year = a.so_year
                      AND ee1.issue_type = 'ADJUST-EXPORT'
                      AND ee1.rec_date IS NOT NULL),
                  0
                 )
          ) export ,a.CLOSE_ORDER_DATE CLOSEPRO,a.UPD_BY PRO_UPD,GET_SO_CLOSE_DATE_ACCOUNT(a.so_year,a.so_no) CLOSEACC
     FROM nyg4.oe_trans_head@nyg4.world a,OE_SO_v b
    WHERE  
    a.so_year=b.so_year
    and a.so_no=b.so_no
    and b.ou_code='N03'
    and b.SO_STATUS <>'C'
    and a.so_year >= TO_CHAR (get_sysdate, 'YY') - 2
    union all
    --trimax
    SELECT 'TRM' BU,a.so_no, a.so_year,
           nyg1.BRAND_DESC@NYG5.WORLD('N03',A.brand_code) brand,
           nyg1.GET_SHIPMENT_DATE@NYG5.WORLD(a.so_no,a.so_year)  shipment_head,
           nyg1.GET_ORDER_QTY@NYG5.WORLD('N03',a.so_YEAR,a.so_NO) qty,
                (  NVL ((SELECT SUM (NVL (jj.pcs_qty, 0))
                     FROM nyg1.dfit_fg_issue_detail@NYG5.WORLD jj
                    WHERE jj.so_no = a.so_no
                      AND jj.so_year = a.so_year
                      AND jj.issue_type = 'EXPORT'),
                  0
                 )
           - NVL ((SELECT SUM (NVL (ee1.pcs_qty, 0))
                     FROM nyg1.dfit_fg_detail@NYG5.WORLD ee1
                    WHERE ee1.so_no = a.so_no
                      AND ee1.so_year = a.so_year
                      AND ee1.issue_type = 'ADJUST-EXPORT'
                      AND ee1.rec_date IS NOT NULL),
                  0
                 )
          ) export ,a.CLOSE_ORDER_DATE CLOSEPRO,a.UPD_BY PRO_UPD,nyg1.GET_SO_CLOSE_DATE_ACCOUNT@NYG5.WORLD(a.so_year,a.so_no) CLOSEACC
     FROM nyg1.oe_trans_head@NYG5.WORLD a,nyg1.OE_SO_v@NYG5.WORLD  b
    WHERE  
    a.so_year=b.so_year
    and a.so_no=b.so_no
    and b.ou_code='N03'
    and b.SO_STATUS <>'C'
    and a.so_year >= TO_CHAR (get_sysdate, 'YY') - 2
    union all
    --vn
    SELECT 'NYV' BU,a.so_no, a.so_year,
           nyg1.BRAND_DESC@NYG6.WORLD('NVN',A.brand_code) brand,
           nyg1.GET_SHIPMENT_DATE@NYG6.WORLD(a.so_no,a.so_year)  shipment_head,
           nyg1.GET_ORDER_QTY2@NYG6.WORLD('NVN',a.so_YEAR,a.so_NO) qty,
                (  NVL ((SELECT SUM (NVL (jj.pcs_qty, 0))
                     FROM nyg1.dfit_fg_issue_detail@NYG6.WORLD jj
                    WHERE jj.so_no = a.so_no
                      AND jj.so_year = a.so_year
                      AND jj.issue_type = 'EXPORT'),
                  0
                 )
           - NVL ((SELECT SUM (NVL (ee1.pcs_qty, 0))
                     FROM nyg1.dfit_fg_detail@NYG6.WORLD ee1
                    WHERE ee1.so_no = a.so_no
                      AND ee1.so_year = a.so_year
                      AND ee1.issue_type = 'ADJUST-EXPORT'
                      AND ee1.rec_date IS NOT NULL),
                  0
                 )
          ) export ,a.CLOSE_ORDER_DATE CLOSEPRO,a.UPD_BY PRO_UPD,nyg1.GET_SO_CLOSE_DATE_ACCOUNT@NYG6.WORLD(a.so_year,a.so_no) CLOSEACC
     FROM nyg1.oe_trans_head@NYG6.WORLD a,nyg1.OE_SO_v@NYG6.WORLD b
    WHERE  
    a.so_year=b.so_year
    and a.so_no=b.so_no
    and b.ou_code='NVN'
    and b.SO_STATUS <>'C'
    and a.so_year >= TO_CHAR (get_sysdate, 'YY') - 2)""")
  
  _csv = r"C:\QVD_DATA\COM_GARMENT\ALLBU\WCS_CLOSE_SOPRO_ALLBU.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  conn.close()
'''

########################################### WCS close so production k'nok sirada

class CLS_WCS_PO_PRINT_EMB(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        WCS_PO_PRINT_EMB()


def WCS_PO_PRINT_EMB():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""SELECT SO_NO,SO_YEAR,WEEKS,VEND_ID,VEND_NAME,VEND_TYPE,FULL_EMB,QTY,SO_STATUS
  ,(SELECT SUM(OUT_QTY)  from dfit_ep_detail_sup q where q.SO_YEAR=vV.SO_YEAR  AND q.SO_NO=VV.SO_NO AND Q.ww_step_name='P') OU_PC_QTY
  ,(SELECT SUM(IN_QTY)  from dfit_ep_detail_sup q where q.SO_YEAR=vV.SO_YEAR  AND q.SO_NO=VV.SO_NO AND Q.ww_step_name='P') IN_PC_QTY
    ,(SELECT SUM(OUT_QTY)  from dfit_ep_detail_sup q where q.SO_YEAR=vV.SO_YEAR  AND q.SO_NO=VV.SO_NO AND Q.ww_step_name='E') OU_EMB_QTY
  ,(SELECT SUM(IN_QTY)  from dfit_ep_detail_sup q where q.SO_YEAR=vV.SO_YEAR  AND q.SO_NO=VV.SO_NO AND Q.ww_step_name='E') IN_EMB_QTY
FROM (
SELECT distinct so_no,so_year,TO_CHAR(SHIPMENT_DATE-40,'YYYYIW') WEEKS,DECODE(VEND_ID,'999995','NYG4','999994','NYG3','999993','NYG2','999992','NYG2','999991','NYG1' ,VEND_ID)  VEND_ID
   ,VEND_NAME,
   DECODE(INSTR(VEND_NAME,'EMB'),0,EMBROIDERY,'EMBROIDERY') VEND_TYPE ,FULL_EMB,QTY,so_status
 FROM (
SELECT  TO_CHAR(SYSDATE,'YYYYIW') WEEKS,OU_CODE, SO_YEAR, SO_NO, (select h.shipment_date from oe_so h where h.ou_code='N03'
AND H.SO_YEAR=V.SO_YEAR
AND H.SO_NO=V.SO_NO) SHIPMENT_DATE,(select h.so_status from oe_so h where h.ou_code='N03'
AND H.SO_YEAR=V.SO_YEAR
AND H.SO_NO=V.SO_NO) so_status,
 ORDER_ID, COLOR_FC, PLAN_YEAR, PLAN_WEEK, VEND_ID,
 (SELECT DISTINCT R.VEND_NAME FROM RT_VENDOR R WHERE R.VEND_ID=V.VEND_ID) VEND_NAME
,(select distinct  exp_type from MASTER_SUPP_CAPISITY c where  c.vend_id=v.vend_id AND EXP_TYPE='EMBROIDERY') EMBROIDERY
,(select distinct  FULL_CAP from MASTER_SUPP_CAPISITY c where  c.vend_id=v.vend_id AND EXP_TYPE='EMBROIDERY') FULL_EMB
,(select distinct  exp_type from MASTER_SUPP_CAPISITY c where  c.vend_id=v.vend_id AND EXP_TYPE='PRINT') PRINT
,(select distinct  FULL_CAP from MASTER_SUPP_CAPISITY c where  c.vend_id=v.vend_id AND EXP_TYPE='PRINT') FULL_PRINT
,(SELECT qty from oe_sub_gmt_qty q where q.SO_YEAR=V.SO_YEAR
AND q.SO_NO=V.SO_NO) qty
  FROM SO_OE_ALLOCATE_SUB_VENDOR v)WHERE  DECODE(INSTR(VEND_NAME,'EMB'),0,EMBROIDERY,'EMBROIDERY')='EMBROIDERY'
  UNION ALL  
   SELECT distinct so_no,so_year,TO_CHAR(SHIPMENT_DATE-40,'YYYYIW') WEEKS,DECODE(VEND_ID,'999995','NYG4','999994','NYG3','999993','NYG2','999992','NYG2','999991','NYG1' ,VEND_ID) VEND_ID,VEND_NAME
 ,DECODE(INSTR(VEND_NAME,'PRINT'),0,PRINT,'PRINT')  VEND_TYPE ,FULL_PRINT,QTY,so_status
 FROM (
SELECT  TO_CHAR(SYSDATE,'YYYYIW') WEEKS,OU_CODE, SO_YEAR, SO_NO, (select h.shipment_date from oe_so h where h.ou_code='N03'
AND H.SO_YEAR=V.SO_YEAR
AND H.SO_NO=V.SO_NO) SHIPMENT_DATE,(select h.so_status from oe_so h where h.ou_code='N03'
AND H.SO_YEAR=V.SO_YEAR
AND H.SO_NO=V.SO_NO) so_status,
 ORDER_ID, COLOR_FC, PLAN_YEAR, PLAN_WEEK, VEND_ID,
 (SELECT DISTINCT R.VEND_NAME FROM RT_VENDOR R WHERE R.VEND_ID=V.VEND_ID) VEND_NAME
,(select distinct  exp_type from MASTER_SUPP_CAPISITY c where  c.vend_id=v.vend_id AND EXP_TYPE='EMBROIDERY') EMBROIDERY
,(select distinct  FULL_CAP from MASTER_SUPP_CAPISITY c where  c.vend_id=v.vend_id AND EXP_TYPE='EMBROIDERY') FULL_EMB
,(select distinct  exp_type from MASTER_SUPP_CAPISITY c where  c.vend_id=v.vend_id AND EXP_TYPE='PRINT') PRINT
,(select distinct  FULL_CAP from MASTER_SUPP_CAPISITY c where  c.vend_id=v.vend_id AND EXP_TYPE='PRINT') FULL_PRINT
,(SELECT qty from oe_sub_gmt_qty q where q.SO_YEAR=V.SO_YEAR
AND q.SO_NO=V.SO_NO) qty
  FROM SO_OE_ALLOCATE_SUB_VENDOR v)  WHERE DECODE(INSTR(VEND_NAME,'PRINT'),0,PRINT,'PRINT')='PRINT') vv 
  ORDER BY 2,1,3,4""")

  _csv = r"C:\QVD_DATA\COM_GARMENT\ALLBU\WCS_PO_PRINT_EMB.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)
  conn.close()

###########################################` soft fc nyg,vn k'FON SCM

class CLS_SOFT_SA_FC_WW_GMT(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        SOFT_SA_FC_WW_GMT()


def SOFT_SA_FC_WW_GMT():
  print("START SOFT_SA_FC_WW_GWT")
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""select 'NYG' BU, FC_YEAR, FC_WEEK, STYLE_REF, SAM_CODE, DEPT_CODE, GMT_TYPE, BRAND_CODE, QTY_PCS, SAM_PCS, TTL_FC, TTL_ACTUAL, FACTORY, ACTIVE, FLAG, CRE_BY, CRE_DATE, UPD_BY, UPD_DATE, LOAD_SEQ
from SA_FC_WW_GMT
where
ou_code='N03'
UNION all 
select 'NVN' BU, FC_YEAR, FC_WEEK, STYLE_REF, SAM_CODE, DEPT_CODE, GMT_TYPE, BRAND_CODE, QTY_PCS, SAM_PCS, TTL_FC, TTL_ACTUAL, FACTORY, ACTIVE, FLAG, CRE_BY, CRE_DATE, UPD_BY, UPD_DATE, LOAD_SEQ
from vn.SA_FC_WW_GMT@VNSQPROD.WORLD
where
ou_code='NVN'
order by bu, fc_year,fc_week

""")

  _csv = r"C:\QVDatacenter\SCM\GARMENT\ALLBU\SOFT_SA_FC_WW_GMT_VN-NYG.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)
  conn.close()

###########################################` soft fc GRW k'FON SCM

class CLS_SOFT_SA_FC_WW(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        SOFT_SA_FC_WW()


def SOFT_SA_FC_WW():
  my_dsn = cx_Oracle.makedsn("172.16.6.87", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""select *
from SA_FC_WW
where
ou_code='N03'

""")

  _csv = r"C:\QVDatacenter\SCM\GARMENT\ALLBU\SOFT_SA_FC_WW_GRW.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)
  conn.close()


###########################################

class CLS_ITEM_NYG_Match_NYK_ITEM(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        ITEM_NYG_Match_NYK_ITEM()
        
def ITEM_NYG_Match_NYK_ITEM():
  my_dsn = cx_Oracle.makedsn("172.16.6.74",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""
  SELECT  DISTINCT D.CUSTOMER_ITEM  ,D.ITEM_CODE , CUSTOMER_COLOR, PL_COLORLAB
FROM SMIT_CUSTOMER_POD D , SMIT_CUSTOMER_POH H
WHERE D.PO_NO=H.PO_NO
AND H.CUSTOMER_ID LIKE '9999%'
AND D.ITEM_CODE IS NOT NULL
AND D.CUSTOMER_ITEM LIKE 'F%'
AND D.ITEM_CODE LIKE 'F%'
AND EXISTS (SELECT  CUST_PO_NUMBER FROM DUMMY_SO_HEADERS_TMP DM WHERE DM.CUST_PO_NUMBER=H.PO_NO)
ORDER BY 1,2,3,4
""")
  
  _csv = r"C:\QVD_DATA\COM_GARMENT\ALLBU\ITEM_NYG_Match_NYK_ITEM.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  conn.close()




  ###########################################

class CLS_Loss_Gain(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        Loss_Gain()
        
def Loss_Gain():
  my_dsn = cx_Oracle.makedsn("172.16.6.74",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""  
SELECT  H.ORDER_NUMBER RS_NUMBER, H.ORA_ORDER_NUMBER SO_NO,C.R12_LINE_ID LINE_ID,
        H.DUMMY_ITEM_TYPE ORDER_TYPE,H.FLOW_STATUS_CODE,H.FOB_POINT_CODE FOB_CODE,
        H.ORDERED_DATE SO_NO_DATE,
        H.CUST_PO_NUMBER PO_NO, H.CUSTOMER_NUMBER CUSTOMER_ID,
        (SELECT CUSTOMER_NAME FROM DFORA_CUSTOMER C WHERE C.CUSTOMER_ID=H.CUSTOMER_NUMBER) CUSTOMER_NAME,
        H.SALESREP_NUMBER, (SELECT SALE_NAME FROM DFORA_SALE S WHERE S.SALE_ID=H.SALESREP_NUMBER) SALE_NAME,
        H.ATTRIBUTE2 END_BUYER,
        L.ORDERED_ITEM ITEM_CODE,
        (SELECT ITEM_DESC FROM FMIT_ITEM F WHERE F.ITEM_CODE=L.ORDERED_ITEM) ITEM_DESC,
        (SELECT ITEM_CATEGORY FROM FMIT_ITEM F WHERE F.ITEM_CODE=L.ORDERED_ITEM) ITEM_CATEGORY,
        L.O_ITEM_REFERENCE,L.TUBULAR_TYPE, L.FABRIC_WIDE, L.FABRIC_GM2,L.O_YARN_COUNT,
        C.COMM_PLCOLOR COLOR_CODE,(SELECT COLOR_DESC FROM DFORA_COLOR CL WHERE CL.COLOR_CODE=C.COMM_PLCOLOR) COLOR_DESC,
        (SELECT PL_COLORLAB FROM SMIT_SO_LINE L WHERE L.SO_NO=H.ORA_ORDER_NUMBER AND L.LINE_ID=C.R12_LINE_ID) SO_COLOR,
        C.ORDER_CUSTOMER CUSTOMER_ORDER,
        C.ORDER_PROD_QTY ,--PRODUCTION_QTY_OLD,
        C.ORDER_PROD_ROLL ,--PRODUCTION_ROLL_OLD,
        C.WKPER_LOSS PERCENT_LOSS,
        C.RD_COLOR
FROM DUMMY_SO_HEADERS H, DUMMY_SO_LINES L, DUMMY_COLOR_LINES C
WHERE H.ORDER_NUMBER=L.ORDER_NUMBER
AND L.ORDER_NUMBER=C.ORDER_NUMBER
AND L.LINE_NUMBER=C.LINE_NUMBER
AND H.FLOW_STATUS_CODE<>'CANCEL'  
AND NVL(H.CLOSE_FLAG,'N')<>'C'
""")
  
  _csv = r"C:\QVD_DATA\COM_GARMENT\ALLBU\Loss_Gain.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  conn.close()



#############################################
threads = []
#'''
thread1 = CLS_step1step2_vn();thread1.start();threads.append(thread1)
thread2 = CLS_step1step2_TRM();thread2.start();threads.append(thread2)
thread3 = CLS_step1step2_NYG();thread3.start();threads.append(thread3)
thread4 = CLS_OE_SO_COSTSHEET_ALLBU();thread4.start();threads.append(thread4)
#'''
thread5 = CLS_SOFT_SA_FC_WW();thread5.start();threads.append(thread5)
#'''
thread6 = CLS_WCS_PO_PRINT_EMB();thread6.start();threads.append(thread6)
thread7 = CLS_SOFT_SA_FC_WW_GMT();thread7.start();threads.append(thread7)
thread8 = CLS_ITEM_NYG_Match_NYK_ITEM();thread8.start();threads.append(thread8)
thread9 = CLS_Loss_Gain();thread9.start();threads.append(thread9)
#'''




for t in threads:
    t.join()
print ("COMPLETE")

