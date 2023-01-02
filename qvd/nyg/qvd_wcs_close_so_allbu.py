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



########################################### WCS close so production k'nok sirada

class CLS_WCS_CLOSE_SOPRO_ALLBU(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        WCS_CLOSE_SOPRO_ALLBU()
        
def WCS_CLOSE_SOPRO_ALLBU():
  my_dsn = cx_Oracle.makedsn("172.16.6.82",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  args = ['G1','G1WCSSPO0001','WCS_CLOSE_SOPRO_ALLBU.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print("sart  WCS_CLOSE_SOPRO_ALLBU")
  cursor.execute("""select BU, SO_NO, SO_YEAR, BRAND, SHIPMENT_HEAD, QTY, EXPORT, CLOSEPRO,PRO_UPD CLOSEPRO_UPD, CLOSEACC,MaxExportDate
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
          ) export ,a.CLOSE_ORDER_DATE CLOSEPRO , a.UPD_BY PRO_UPD,GET_SO_CLOSE_DATE_ACCOUNT(a.so_year,a.so_no) CLOSEACC,GET_SO_EXPORT_MAXDATE(a.so_year,a.so_no) MaxExportDate
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
          ) export ,a.CLOSE_ORDER_DATE CLOSEPRO , a.UPD_BY PRO_UPD,GET_SO_CLOSE_DATE_ACCOUNT(a.so_year,a.so_no) CLOSEACC,GET_SO_EXPORT_MAXDATE(a.so_year,a.so_no) MaxExportDate
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
          ) export ,a.CLOSE_ORDER_DATE CLOSEPRO, a.UPD_BY PRO_UPD,GET_SO_CLOSE_DATE_ACCOUNT(a.so_year,a.so_no) CLOSEACC,GET_SO_EXPORT_MAXDATE(a.so_year,a.so_no) MaxExportDate
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
          ) export ,a.CLOSE_ORDER_DATE CLOSEPRO,a.UPD_BY PRO_UPD,GET_SO_CLOSE_DATE_ACCOUNT(a.so_year,a.so_no) CLOSEACC,GET_SO_EXPORT_MAXDATE(a.so_year,a.so_no) MaxExportDate
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
          ) export ,a.CLOSE_ORDER_DATE CLOSEPRO,a.UPD_BY PRO_UPD,nyg1.GET_SO_CLOSE_DATE_ACCOUNT@NYG5.WORLD(a.so_year,a.so_no) CLOSEACC,nyg1.GET_SO_EXPORT_MAXDATE@NYG5.WORLD(a.so_year,a.so_no) MaxExportDate
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
          ) export ,a.CLOSE_ORDER_DATE CLOSEPRO,a.UPD_BY PRO_UPD,nyg1.GET_SO_CLOSE_DATE_ACCOUNT@NYG6.WORLD(a.so_year,a.so_no) CLOSEACC,nyg1.GET_SO_EXPORT_MAXDATE@NYG6.WORLD(a.so_year,a.so_no) MaxExportDate
     FROM nyg1.oe_trans_head@NYG6.WORLD a,nyg1.OE_SO_v@NYG6.WORLD b
    WHERE  
    a.so_year=b.so_year
    and a.so_no=b.so_no
    and b.ou_code='NVN'

    and b.SO_STATUS <>'C'
    and a.so_year >= TO_CHAR (get_sysdate, 'YY') - 2)""")
  
  _csv = r"C:\QVD_DATA\COM_GARMENT\ALLBU\WCS_CLOSE_SOPRO_ALLBU.csv"

  with open(_csv, "w", newline='', encoding='utf-8-sig') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  args = ['G1','G1WCSSPO0001',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()
  print("WCS_CLOSE_SOPRO_ALLBU Done")  
#############################################
threads = []

thread5 = CLS_WCS_CLOSE_SOPRO_ALLBU();thread5.start();threads.append(thread5)



for t in threads:
    t.join()
print ("COMPLETE")

