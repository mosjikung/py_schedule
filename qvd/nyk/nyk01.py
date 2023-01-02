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
os.environ["ORACLE_HOME"] = oracle_client
os.environ["PATH"] = oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.TH8TISASCII"


class CLS_SALE_ORDER_OE(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      SALE_ORDER_OE()


def SALE_ORDER_OE():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nyis", password="nyis",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  cursor.execute("""SELECT M.*
,D.YARN_COST, D.PRINT_COST, D.FINISING_COST, D.TOTAL_COST_KG, D.YARN_COST_AMOUNT, D.PRINT_COST_AMOUNT, D.FINISING_COST_AMOUNT, D.TOTAL_COST_AMOUNT, D.OE_GAP, D.ACT_ORDER_QTY
FROM SF5.SF5_GAP_SO_OE_LINE M, NYIS.WEB_90100010610_OE D
WHERE EXTRACT(YEAR FROM M.SO_NO_DATE)  >= 2012
AND M.SO_NO = D.SO_NO(+)
AND M.SO_LINE = D.SO_LINE(+) """)

  _csv = r"C:\IT_ONLY\NYG\SALE_ORDER_OE.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)
  conn.close()
  print("COMPLETE SALE_ORDER_OE")


class CLS_SALE_ORDER_OE_COLLAR(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      SALE_ORDER_OE_COLLAR()


def SALE_ORDER_OE_COLLAR():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="sf5", password="omsf5",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  cursor.execute("""SELECT   g.so_no, r.line_id so_line, g.so_no_date,  
             TO_CHAR (g.so_no_date, 'MON') so_month, pl_colorlab, color_code,  
             DECODE (tubular_type,  
             1, 'อบกลม',  
             2, 'อบผ่า',  
             'OTHER'  
             ) finishing_type,  
             get_customer_cus_id (g.customer_id) oe_cus_code,  
             get_customer_name_nyf (g.customer_id) oe_cus_name, g.buyyer,  
             o.order_type_name, o.order_type_desc, g.style, g.nyf_cus_po,  
             g.customer_year so_fg_year, g.customer_fg so_fg_week,  
             g.sale_id oe_sale_id, get_sale_name (g.sale_id) oe_sale_name,  
             web_get_oe_team (g.sale_id, g.so_no_date) oe_team_name,  
             web_get_oe_division (g.sale_id, g.so_no_date) oe_division_name,  
             g.fob_code oe_fob_code, SUM (r.ordered_quantity) oe_order, g.so_status, 
             DECODE (r.ordered_quantity,  
             0, 0,  
             ROUND (  SUM (r.ordered_quantity * r.nyf_pr_selling)  
             / SUM (r.ordered_quantity),  
             2  
             )  
             ) oe_order_price,  
             r.item_code,  
             DECODE (SUBSTR (r.item_code, 1, 1),  
             'F', SUBSTR (r.item_code, 1, LENGTH (r.item_code) - 2),  
             r.item_code  
             ) oe_so_item,  
             DECODE (SUBSTR (r.item_code, 1, 1),  
             'F', SUBSTR (r.item_code, 1, LENGTH (r.item_code) - 2)  
             || DECODE (r.tubular_type, 1, 'B0', 'A0'),  
             r.item_code  
             ) oe_so_item_grey,  
             color_shade, g.so_qn, g.conversion_code oe_conversion,  
             DECODE (g.conversion_code,  
             'THB', 1,  
             g.conversion_rate  
             ) oe_conversion_rate,  
             r.uom oe_so_uom, SUM (r.cust_order_qty) cust_order_qty,  
             d.sale_fgweek, d.fg_yearww  
             ,NULL ITEM_CATEGORY_SALES,NULL ITEM_CATEGORY,NULL ITEM_STRUCTURE,NULL MACHINE_GROUP,  
             NULL O_FN_OPEN, NULL O_FN_TUBULAR, NULL O_FN_YARD, NULL O_FN_GM, NULL O_YARN_COUNT,   
             NULL O_GAUGE, NULL O_MAT_CONS,  
             --round(SUM (r.ordered_quantity)/3,4) ACT_ORDER_QTY,
             round(CASE WHEN r.uom = 'PCS' THEN SUM (r.ordered_quantity)/36
                  WHEN r.uom = 'LBS' THEN SUM (r.ordered_quantity)/2.2046
                  WHEN r.uom = 'DOZ' THEN SUM (r.ordered_quantity)/3   
                  ELSE SUM(r.ordered_quantity) END ,4) ACT_ORDER_QTY, 
             SUM (r.cust_order_qty) cust_order_qty1  
             FROM smit_so_header g,  
             smit_so_line r,  
             (SELECT   f.order_type_id, MAX (order_type_name) order_type_name,  
             MAX (description) order_type_desc  
             FROM mis_nyf_oe_r12 f  
             WHERE NVL (f.oe_order, 'N') = 'Y' AND f.order_type_id <> 1262  
             GROUP BY f.order_type_id) o,  
             (SELECT ora_order_number so_no,  
             LTRIM  
             (TO_NUMBER (   sales_fg_year  
             || LTRIM (TO_CHAR (sales_fg_week, '09'))  
             )  
             ) sale_fgweek,  
             LTRIM (TO_NUMBER (   fg_year  
             || LTRIM (TO_CHAR (fg_week, '09'))  
             )  
             ) fg_yearww  
             FROM dummy_so_headers) d  
             WHERE g.order_type_id = o.order_type_id  
             AND g.so_no = r.so_no  
              AND TO_CHAR(g.so_no_date ,'YYYYMM')>='201801'  
             AND (g.so_no NOT LIKE '99%' AND LENGTH (g.so_no) <> 11)  
             AND g.so_no = d.so_no(+)  
             AND SUBSTR (r.item_code, 1, 1) <> 'F'  
             GROUP BY g.so_no,  
             r.line_id,  
             g.so_no_date,  
             g.customer_id,  
             g.buyyer,  
             g.style,  
             g.nyf_cus_po,  
             g.customer_year,  
             g.customer_fg, g.so_status, 
             g.sale_id,  
             g.fob_code,  
             r.item_code,  
             pl_colorlab,  
             color_code,  
             tubular_type,  
             DECODE (SUBSTR (r.item_code, 1, 1),  
             'F', SUBSTR (r.item_code, 1, LENGTH (r.item_code) - 2),  
             r.item_code  
             ),  
             DECODE (SUBSTR (r.item_code, 1, 1),  
             'F', SUBSTR (r.item_code, 1, LENGTH (r.item_code) - 2)  
             || DECODE (r.tubular_type, 1, 'B0', 'A0'),  
             r.item_code  
             ),  
             color_shade,  
             g.so_qn,  
             g.conversion_code,  
             o.order_type_name,  
             o.order_type_desc,  
             DECODE (g.conversion_code, 'THB', 1, g.conversion_rate),  
             r.uom,  
             r.ordered_quantity,  
             d.sale_fgweek,  
             d.fg_yearww   """)

  _csv = r"C:\QVD_DATA\COM_FABRIC\SALE_ORDER_OE_COLLAR.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)
  conn.close()
  print("COMPLETE SALE_ORDER_OE_COLLAR")


class CLS_QA_HISTORY(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      QA_HISTORY()


def QA_HISTORY():
  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="DEMO", password="DEMO",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  cursor.execute("""Select Qc.Ou_Code,Qc.Batch_No,Qc.Color_Code,Qc.Item_Code,
       Bd.So_No,Bd.Line_Id,
       Bd.Schedule_Id,Qc.APP_COLORLIP_DATE,
       Decode(Qc.APP_COLORLIP_STATUS,'P',Decode(Qc.APP_COLORLIP_SUBSTATUS,'L','L/G','Pass')
       ,'R','Fail','N','การเจรจาสี','B','รอ Body','D','รอตัดสินใจ','F','ไม่มีตัวอย่าง') App_ColorLip,
       Qc.APP_COLORLIP_BY App_ColorLip_By, 
	     Qc.APP_COLORLIP_REJECT,Qc.APP_COLORLIP_REMARK, 
	     Qc.APP_COLORLIP_FIRST,Qc.APP_COLORLIP_SHADE, 
	     Qc.APP_COLORLIP_TYPE1 Hue_App_ColorLip, 
       Qc.APP_COLORLIP_TYPE2 Croma_App_ColorLip
From DFQC_HISTORY_APP Qc,
     Dfit_Btdata Bd
Where Bd.Ou_COde = Qc.Ou_Code 
      And Bd.Batch_No = Qc.Batch_No 
      And Trunc(Qc.ENTRY_DATE) = (Trunc(Sysdate) - 1)""")

  _csv = r"C:\QVD_DATA\PRO_NYK\QA_HISTORY.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)
  conn.close()
  print("COMPLETE QA_HISTORY")


class CLS_QA_Approve_PL(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      QA_Approve_PL()


def QA_Approve_PL():
  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="DEMO", password="DEMO",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  cursor.execute("""Select Distinct Dph.Ou_Code,Dph.Batch_No,
                Db.So_No,Db.Line_Id,
                Db.Customer_Id,Db.Customer_Name,
                Db.Sale_Id,Db.Sale_Name,
                Db.Schedule_Id,Db.Item_Code,Db.Item_Desc,Db.Color_Code,
                Db.Color_Desc,
                Db.BUYYER,
                Db.Mps_Week_Year,Db.Mps_Week_No,
                To_Char(Dph.Pl_Date,'DD/MM/YYYY') Pl_Date,
	              Dph.Pl_No,
	              Dph.Grade_No,
	              To_Char(Dph.Approve_Date,'DD/MM/YYYY HH24:MI') App_Date,
	              Dph.Approved_By App_By,
	              Decode(Dph.Approved_Active,'P','Pass','R','Reject','N','None Approved') App_Status,
	              Count(Dpd.Seq_No) Seq_No,
	              Sum(Nvl(Dpd.Qty,0)) Qty,
	              Sum(Nvl(Dpd.Unit_Qty,0)) Unit_Qty,
	              Sum(Nvl(Dpd.Qty_Rm,0)) Qty_Rm,
	              Sum(Nvl(Dpd.Unit_Rm,0)) Unit_Rm,
	              Sum(Nvl(Dpd.Yard,0)) Yard,
                Approved_Remark App_Remark
From Dfpl_Header Dph,
     Dfpl_Detail Dpd,
     Dfit_Btdata Db
Where DB.Ou_Code = Dph.Ou_Code
      And Db.Batch_No = Dph.Batch_No
      And Dph.Ou_Code = Dpd.Ou_Code
      And Dph.Batch_No = Dpd.Batch_No
      And Dph.Pl_No = Dpd.Pl_No
      And nvl(Dph.Status,'0') = '0'
      And Db.Status <> '9'
      And To_Char(Db.Batch_Entry_Date,'YYYY') >= '2011'
      And substr(Dph.pl_no,1,2) <> 'P2' 
      And Trunc(Dph.Pl_Date) = (Trunc(Sysdate) - 1)
      And Dph.Approved_Active Is Not Null
Group By Dph.Ou_Code,Dph.Batch_No,
                Db.So_No,Db.Line_Id,
                Db.Customer_Id,Db.Customer_Name,
                Db.Sale_Id,Db.Sale_Name,
                Db.Schedule_Id,Db.Item_Code,Db.Item_Desc,Db.Color_Code,
                Db.Color_Desc,
                Db.BUYYER,
                Db.Mps_Week_Year,Db.Mps_Week_No,
                To_Char(Dph.Pl_Date,'DD/MM/YYYY'),
	              Dph.Pl_No,
	              Dph.Grade_No,
	              To_Char(Dph.Approve_Date,'DD/MM/YYYY HH24:MI'),
	              Dph.Approved_By,
	              Decode(Dph.Approved_Active,'P','Pass','R','Reject','N','None Approved'),
                Approved_Remark
Order By Dph.Ou_Code,
         Dph.Batch_No, 
         Dph.Pl_No""")

  _csv = r"C:\QVD_DATA\PRO_NYK\QA_Approve_PL.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)
  conn.close()
  print("COMPLETE QA_Approve_PL")

###############################################################
class CLS_MAP_ITEM_FAB_NYKNYG(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      MAP_ITEM_FAB_NYKNYG()


def MAP_ITEM_FAB_NYKNYG():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="sf5", password="omsf5",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  # cursor = conn.cursor()

  sql = """SELECT  P.PO_NO,  L.SO_NO  SO_NO_NYK, L.ITEM_CODE  ITEM_NYK , L.PL_COLORLAB COLOR_NYK,
                  P.CUSTOMER_ITEM  ITEM_NYG, P.CUSTOMER_COLOR COLOR_NYG, 
                 P.CUSTOMER_SIZE SIZE_NYG,
                 P.SO_NO_DOC SO_NO_NYG, P.ORDER_QTY  PO_ORDER_QTY
FROM SMIT_SO_LINE L, SMIT_SO_HEADER H, SMIT_CUSTOMER_POD P
WHERE L.SO_NO=H.SO_NO
AND H.PO_NO=P.PO_NO
AND L.ITEM_CODE=P.ITEM_CODE
AND L.PL_COLORLAB=P.PL_COLORLAB
AND H.SO_NO_DATE >= TO_DATE('01/01/2020','DD/MM/RRRR')
AND H.FOB_CODE NOT LIKE 'R%'   AND H.CUSTOMER_ID IN (3871, 1361, 3876, 1832105)
AND SUBSTR(P.PO_NO,1,1) NOT IN ('S','R')
AND H.SO_STATUS NOT LIKE '00%'
ORDER BY 1,2  """

  df = pd.read_sql_query(sql, conn)

  df.to_excel(
      r'C:\QVDatacenter\SCM\GARMENT\ALLBU\MAP_ITEM_FAB_NYKNYG.xlsx', index=False)
  conn.close()
  print("COMPLETE MAP_ITEM_FAB_NYKNYG")

###############################################################

###############################################################
class CLS_ORDERYARN_SCM_VS_SOURCING(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      ORDERYARN_SCM_VS_SOURCING()


def ORDERYARN_SCM_VS_SOURCING():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  # cursor = conn.cursor()

  sql = """ SELECT  * from ORDERYARN_SCM_VS_SOURCING_V
             """

  df = pd.read_sql_query(sql, conn)

  df.to_excel(
      r'C:\QVD_DATA\COM_FABRIC\ORDERYARN_SCM_VS_SOURCING.xlsx', index=False)
  conn.close()
  print("COMPLETE ORDERYARN_SCM_VS_SOURCING")

###############################################################



threads = []

thread1 = CLS_SALE_ORDER_OE_COLLAR()
thread1.start()
threads.append(thread1)

thread2 = CLS_QA_HISTORY()
thread2.start()
threads.append(thread2)

thread3 = CLS_QA_Approve_PL()
thread3.start()
threads.append(thread3)

thread4 = CLS_MAP_ITEM_FAB_NYKNYG()
thread4.start()
threads.append(thread4)

thread5 = CLS_ORDERYARN_SCM_VS_SOURCING()
thread5.start()
threads.append(thread5)





for t in threads:
    t.join()
print("COMPLETE")
