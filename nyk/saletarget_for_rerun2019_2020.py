import cx_Oracle
import csv
import os
from pathlib import Path
import requests
import threading
import time
import datetime
from datetime import datetime, timedelta
# import openpyxl
# from openpyxl.styles import PatternFill, Border, Side, Alignment, Protection, Font

oracle_client = "C:\instantclient_19_5"
os.environ["ORACLE_HOME"]=oracle_client
os.environ["PATH"]=oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"]="AMERICAN_AMERICA.TH8TISASCII"


# def gen_cursor_to_file(PathFileNameXLSX, cursor):
#   wb = openpyxl.Workbook()
#   ws = wb['Sheet']
#   home = os.path.expanduser('~')
  
#   for c, col in enumerate(cursor.description):
#     ws.cell(row=1, column=c+1, value=col[0])
  
#   r = 2
#   for row in cursor:
#     for c, col in enumerate(cursor.description):
#       ws.cell(row=r, column=c+1, value=row[c])
#     r = r + 1
#   wb.save(PathFileNameXLSX)

class CLS_WEB_90100010610_NI(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        WEB_90100010610_NI()

def WEB_90100010610_NI():
  my_dsn = cx_Oracle.makedsn("172.16.6.74",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()

  sql = """ DELETE FROM WEB_90100010610_NI 
            WHERE EXTRACT(year FROM DOCUMENT_DATE) > 2020 
            AND TRUNC(DOCUMENT_DATE) >= TO_DATE( '01' || to_char( trunc(sysdate) -60,'/MM/YYYY'),'DD/MM/YYYY') """

  cursor.execute(sql)

  conn.commit()

  sql = """INSERT INTO WEB_90100010610_NI
          SELECT M.* , FI.ITEM_DESC,NYIS.GET_ITEM_CATEGORY_FOR_SALES@BIS.WORLD(M.LINE_ITEM_CODE) ITEM_CATEGORY_SLAES , I.ITEM_CATEGORY, I.ITEM_STRUCTURE, 
        I.MACHINE_GROUP, I.O_FN_OPEN, I.O_FN_TUBULAR, I.O_FN_YARD, I.O_FN_GM, I.O_YARN_COUNT, I.O_GAUGE, I.O_MAT_CONS, 
        (SELECT DISTINCT NVL(H.SEGMENT1,'NON HVA') MKT_TYPE 
         FROM ITM.DVTH_DEVELOP_ITEM D,ITM.DVTH_H_SEGMENT H 
        WHERE D.DEV_ITEM_NO=H.DEV_ITEM_NO AND H.SEGMENT2 = '01.STATUS HVA' 
         AND NVL(D.APPR3_COST_ACTIVE,'N')='Y' 
         AND D.DEV_ITEM_CODE =M.ITEM_FABRIC) MKT_TYPE 
         ,NVL((SELECT DIVISION_CODE FROM NYIS.PS_SALES_SECURITY@BIS.WORLD S WHERE S.SALES_CODE = M.ORA_SALE_ID ),DIVISION ) DIVISION_CODE_NEW
        ,NVL((SELECT TEAM_CODE FROM NYIS.PS_SALES_SECURITY@BIS.WORLD S WHERE S.SALES_CODE = M.ORA_SALE_ID  ),TEAM_NAME) TEAM_NAME_NEW
         ,(SELECT MAX(COMMERCIAL_INVOICE)  FROM DEMO.NYF_FGPK_HEADER@REPLICA1.WORLD
                      WHERE COMMERCIAL_INVOICE<>'D6SF5FMPS01'   AND SO_NO=M.SO_NO
                      AND  LTRIM(TO_CHAR(INV_NO)) = M.DOCUMENT_NUMBER) COMMERCIAL_INVOICE
          FROM NYIS.V_TARGET_INVOICE_GAP@BIS.WORLD M, NYIS.ITEM_SPEC@BIS.WORLD I , SF5.FMIT_ITEM FI 
         WHERE  M.ITEM_FABRIC = I.ITEM_CODE(+) 
         AND M.LINE_ITEM_CODE = FI.ITEM_CODE(+) 
	       AND EXTRACT(year FROM DOCUMENT_DATE) > 2020
         AND TRUNC(DOCUMENT_DATE) >= TO_DATE( '01' || to_char( trunc(sysdate) -60,'/MM/YYYY'),'DD/MM/YYYY') 
         """

        

  cursor.execute(sql)
  conn.commit()
  conn.close()
  print('Complete WEB_90100010610_NI')


class CLS_WEB_90100010610_OE(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        WEB_90100010610_OE()

def WEB_90100010610_OE():
  my_dsn = cx_Oracle.makedsn("172.16.6.74",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()

  sql = """ DELETE FROM WEB_90100010610_OE WHERE EXTRACT(year FROM SO_NO_DATE) = 2020 """
  # sql = """ DELETE FROM WEB_90100010610_OE WHERE EXTRACT(year FROM SO_NO_DATE) >= 2021 """

  cursor.execute(sql)

  conn.commit()

  nYear = [2020]
  # nYear = [2021,2022]
  nMonth = [1,2,3,4,5,6,7,8,9,10,11,12]


  for i in nYear:
    for j in nMonth:
      print(i, j)

      sql = """INSERT INTO WEB_90100010610_OE
              SELECT M.* FROM (
              SELECT M.*, I.ITEM_DESC, SF5.GET_ITEM_CATEGORY_FOR_SALES(M.OE_SO_ITEM_GREY) ITEM_CATEGORY_SALES , I.ITEM_CATEGORY, I.ITEM_STRUCTURE,I.MACHINE_GROUP,
              I.O_FN_OPEN, I.O_FN_TUBULAR, I.O_FN_YARD, I.O_FN_GM, I.O_YARN_COUNT, I.O_GAUGE, I.O_MAT_CONS, 
              --CASE WHEN (INSTR(OE_SO_UOM,'KG') > 0) THEN OE_ORDER ELSE OE_ORDER/5 END ACT_ORDER_QTY
              OE_ORDER ACT_ORDER_QTY
              ,(SELECT MAX(SO_RESERVE) FROM DUMMY_SO_HEADERS H WHERE H.ORA_ORDER_NUMBER=M.SO_NO) SO_RESERVE 
              ,(SELECT MAX(SO_BILL_REF) FROM DUMMY_SO_HEADERS H WHERE H.ORA_ORDER_NUMBER=M.SO_NO) SO_BILL_REF 
              ,(SELECT MAX(SO_TYPE) FROM DUMMY_SO_HEADERS H WHERE H.ORA_ORDER_NUMBER=M.SO_NO) SO_TYPE 
              ,(SELECT MAX(DIVISION_CODE) FROM NYIS.PS_SALES_SECURITY@BIS.WORLD S WHERE S.SALES_CODE = M.OE_SALE_ID ) DIVISION_CODE_NEW
              ,(SELECT MAX(TEAM_CODE) FROM NYIS.PS_SALES_SECURITY@BIS.WORLD S WHERE S.SALES_CODE = M.OE_SALE_ID  ) TEAM_NAME_NEW
              ,NULL ORDER_KGS
              FROM SF5.SF5_GAP_SO_OE_LINE_V M, SF5.FMIT_ITEM I 
              WHERE 1 = 1
              AND EXTRACT(year FROM SO_NO_DATE) = {year}
              AND EXTRACT(month FROM SO_NO_DATE) = {month}
              AND M.OE_SO_ITEM_GREY = I.ITEM_CODE(+) 
              AND OE_SO_ITEM LIKE 'F%' ) M
              WHERE 1 = 1 """.format(year=i,month=j)

      cursor.execute(sql)
      conn.commit()
      # print(sql)

  conn.close()
  print('Complete WEB_90100010610_OE')


def EXPORT_OE_ACTUAL():
  my_dsn = cx_Oracle.makedsn("172.16.6.74",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()

  sql = """ DELETE FROM EXPORT_OE_ACTUAL WHERE 1 = 1 """
  cursor.execute(sql)

  conn.commit()

  sql = """INSERT INTO EXPORT_OE_ACTUAL
              SELECT nvl(D.RNK,0) RNK,
      M.SO_NO, SO_LINE, SO_NO_DATE, SO_MONTH, PL_COLORLAB, COLOR_CODE, FINISHING_TYPE, OE_CUS_CODE, OE_CUS_NAME, BUYYER, ORDER_TYPE_NAME, ORDER_TYPE_DESC, STYLE, NYF_CUS_PO, SO_FG_YEAR, SO_FG_WEEK, OE_SALE_ID, OE_SALE_NAME, TEAM_NAME_NEW OE_TEAM_NAME, DIVISION_CODE_NEW OE_DIVISION_NAME, OE_FOB_CODE, OE_ORDER, SO_STATUS, OE_ORDER_PRICE, M.ITEM_CODE, OE_SO_ITEM, OE_SO_ITEM_GREY, COLOR_SHADE, SO_QN, OE_CONVERSION, OE_CONVERSION_RATE, OE_SO_UOM, CUST_ORDER_QTY, SALE_FGWEEK, RU_CONFIRM_FG, OE_AMOUNT, YARN_COST, PRINT_COST, FINISING_COST, TOTAL_COST_KG, YARN_COST_AMOUNT, PRINT_COST_AMOUNT, FINISING_COST_AMOUNT, TOTAL_COST_AMOUNT,
       ROUND(OE_GAP,6) as OE_GAP, ITEM_DESC, ITEM_CATEGORY_SALES, ITEM_CATEGORY, ITEM_STRUCTURE, MACHINE_GROUP, O_FN_OPEN, O_FN_TUBULAR, O_FN_YARD, O_FN_GM, O_YARN_COUNT, O_GAUGE, O_MAT_CONS, ACT_ORDER_QTY, SO_RESERVE, SO_BILL_REF, SO_TYPE,
       OE_DIVISION_NAME OE_DIVISION_NAME_OLD, OE_TEAM_NAME OE_TEAM_NAME_OLD, ORDER_KGS
            FROM NYIS.WEB_90100010610_OE M, (           
            select RANK() OVER(
			PARTITION BY SO_NO, ITEM_CODE
			ORDER BY KNIT_YEAR, KNIT_WEEK) RNK
,D.SO_NO, D.ITEM_CODE
            FROM SF5.DUMMY_MAIN_RESERVE_D D
            where  knit_year > 2019
             AND  D.CONF_RU_TYPE='CONFIRM_WEEK'
union 
SELECT RANK() OVER(
			PARTITION BY SO_NO, ITEM_CODE
			ORDER BY KNIT_YEAR, KNIT_WEEK) RNK, M.so_no, m.item_code
FROM (
SELECT  KNIT_MC_SO SO_NO, KNIT_ITEM_CODE ITEM_CODE, KNIT_MC_YEAR KNIT_YEAR, KNIT_MC_WW KNIT_WEEK
FROM DFIT_KNIT_SOKP
where KNIT_MC_YEAR > 2019
GROUP BY KNIT_MC_SO , KNIT_ITEM_CODE , KNIT_MC_YEAR , KNIT_MC_WW
 ) M            
            ) D
            WHERE EXTRACT(YEAR FROM SO_NO_DATE)> 2019
            AND M.SO_NO = D.SO_NO(+)
            AND M.OE_SO_ITEM_GREY = D.ITEM_CODE(+)
        order by M.SO_NO, SO_LINE, OE_SO_ITEM_GREY, d.RNK """

  cursor.execute(sql)
  conn.commit()
  conn.close()
  print('Complete EXPORT_OE_ACTUAL')


WEB_90100010610_OE()
# WEB_90100010610_NI()

# EXPORT_OE_ACTUAL()

#git



# Python3 code to iterate over a list

