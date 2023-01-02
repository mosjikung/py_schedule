#!/usr/bin/python

# import asyncio
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
# my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
#Replica.world

time_start  = datetime.now() 
allTxt = ''

def printttime(txt):
  global allTxt
  
  dateTimeObj = datetime.now()
  timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
  now  = datetime.now() 
  duration = now - time_start
  if allTxt == '':
    sendLine('naylit01.py Start')
    allTxt = '\n'
  print('naylit01.py ' + timestampStr + ' ' + str(duration.total_seconds()) + ' ' + txt)
  txtSend = 'naylit01.py ' + timestampStr + ' ' + str(duration.total_seconds()) + ' ' + txt +'\n'
  allTxt = allTxt + txtSend


def sendLine(txt):
  url = 'https://notify-api.line.me/api/notify'
  token = 'ZE6d4wFQO2qQiSMAMqecrPbj6R3nhj0y1STqOJ6xQ1s'
  headers = {'content-type':'application/x-www-form-urlencoded','Authorization':'Bearer ' + token}
  msg = txt
  requests.post(url,headers=headers,data = {'message':msg})
  
  
###########################################
class CLS_NAYLIT_ORDER(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        NAYLIT_ORDER()
        
def NAYLIT_ORDER():
  my_dsn = cx_Oracle.makedsn("172.16.6.76",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyis", password="nyis", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""SELECT H.*
       FROM V_SO_DAILY_NYL H """)
  
  _csv = r"C:\QVD_DATA\NAYLIT\NAYLIT_ORDER.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  conn.close()
  printttime(_csv)

####################################################
class CLS_NAYLIT_INVOICE(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        NAYLIT_INVOICE()
        
def NAYLIT_INVOICE():
  my_dsn = cx_Oracle.makedsn("172.16.6.76",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyis", password="nyis", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""SELECT decode(CTX.TYPE,'INV','INV','CM','CN','DM','DN',null) TRANS_TYPE, 
                TRX.TRX_NUMBER, 
                TRX.TRX_DATE, 
                TRX.BILL_TO_CUSTOMER_ID, 
                (SELECT NVL(ATTRIBUTE15,CUSTOMER_NAME)FROM  rapps.NY_RA_CUSTOMERS@R12INTERFACE.WORLD WHERE CUSTOMER_ID = TRX.BILL_TO_CUSTOMER_ID) CUSTOMER_NAME, 
                TRX.PURCHASE_ORDER, 
                LRX.SALES_ORDER , TO_NUMBER(LRX.SALES_ORDER_LINE) SALES_ORDER_LINE, NVL(TRX.ATTRIBUTE3,TRX.INTERFACE_HEADER_ATTRIBUTE4) PACKING, 
                TRX.INTERFACE_HEADER_ATTRIBUTE5 INPUB, 
                LRX.QUANTITY_INVOICED, 
                LRX.UNIT_SELLING_PRICE, 
                LRX.EXTENDED_AMOUNT, 
                LRX.UOM_CODE, 
                LRX.DESCRIPTION, 
                LRX.ATTRIBUTE1 LIN_PUB, 
                LRX.ATTRIBUTE2 Fabric_type, 
                LRX.ATTRIBUTE3 COL_CODE, 
                TRX.INVOICE_CURRENCY_CODE CUR, 
                TRX.EXCHANGE_RATE, 
                TRX.ATTRIBUTE10 COM, 
                LRX.ATTRIBUTE13 KG_QTY 
      FROM rapps.RA_CUSTOMER_TRX_ALL@R12INTERFACE.WORLD TRX, 
           rapps.RA_CUSTOMER_TRX_LINES_ALL@R12INTERFACE.WORLD LRX, 
           apps.RA_CUST_TRX_TYPES_ALL@R12INTERFACE.WORLD CTX 
      WHERE TRX.CUSTOMER_TRX_ID = LRX.CUSTOMER_TRX_ID 
        AND LRX.LINE_TYPE = 'LINE' 
        AND TRX.CUST_TRX_TYPE_ID = CTX.CUST_TRX_TYPE_ID 
        AND TRX.ORG_ID =  278 
      ORDER BY 2,3""")
  
  _csv = r"C:\QVD_DATA\NAYLIT\NAYLIT_INVOICE.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  conn.close()
  printttime(_csv)
  
###################################################################  
class CLS_PO_RM_TRACKING(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        PO_RM_TRACKING()
        
def PO_RM_TRACKING():
  my_dsn = cx_Oracle.makedsn("172.16.6.76",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyis", password="nyis", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""SELECT PO_NO,PO_LINE,PO_DATE,ITEM_CODE,PO_QTY,REC_RM_TYPE,INVOICE_NO,YARN_LOT,locator_id,START_REC_DATE,LAST_REC_DATE,
        TOT_BARCODE,REC_RM_KG,RESERVE_RM_KG,ISSUE_RM_KG,BALANCE_RM_KG
    from fccs_bu2.V_PO_RM_NAYLIT_WH@salesf5.world w
    where 1 = 1 """)
  
  _csv = r"C:\QVD_DATA\NAYLIT\PO_RM_TRACKING.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  conn.close()
  printttime(_csv)
  
###################################################################  
class CLS_Prod_Time_Att_Process(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        Prod_Time_Att_Process()
        
def Prod_Time_Att_Process():
  my_dsn = cx_Oracle.makedsn("172.16.6.76",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyis", password="nyis", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""SELECT sh.ou_code ou_code,sh.so_no,sh.line_id,
        nvl(sh.nyf_cus_po,'N') po_no,sh.batch_no,sh.schedule_id,
        sh.customer_id,sh.customer_name customer_name,sh.customer_type,
        sh.item_code,sh.item_desc item_desc,
        sh.color_code,sh.color_desc color_desc,
        decode(sh.tubular_type,'1','tubular','2','Open',null) tubular_type,SL.ORDERED_QUANTITY soline_order,
        sh.so_no_date,sh.close_date batch_close_date,sh.sale_id,sh.sale_name,
        decode(sh.job_type,'N','Normal','D','Repair.Internal','R','Repair.External') Bt_job_type,
        sh.total_qty Batch_qty, sh.send_qt_date QT_REC_DATE,sh.rec_qt_date QT_Send_Result_date,sh.App_ColorLip_Date,
        max(ph.pl_date) Last_pl_date,max(trunc(ph.shipment_date)) Last_RecWH_date,
        sum(get_pldh_tot_sum(Ph.Ou_Code,Ph.Pl_No,Ph.Batch_no)) Pack_qty,
        (select sum(w.fabric_weight) from fccs_bu2.dfwh_warehouse@salesf5.world w
         where w.ou_code=sh.ou_code and w.batch_no=sh.batch_no) rec_wh_qty,
         get_step_name_eng (m.step_no) step_process,m.machine_no,to_number(m.method_cont||m.seq_no) step_seq,
         m.plan_date,m.start_date,m.end_date,
         DECODE(NVL(o.SCH_CLOSED,'N'),'N','Normal',
                                   'Y','Closed',
                                   'H','Hold',
                                   'C','Cancel') SCH_STATUS,
        o.SCH_CLOSED_DATE
  FROM demo.dfit_btdata sh,demo.dfora_so_line sl,
       demo.dfpl_header ph,demo.dfbt_monitor m,demo.dfit_mc_schedule o
  WHERE nvl(sh.status,'0')<>'9'
         and sh.ou_code = ph.ou_code(+)
         AND sh.batch_no = ph.batch_no(+)
         and sh.ou_code = m.ou_code(+)
         AND sh.batch_no = m.batch_no(+)
         and sh.so_no=sl.so_no
         and sh.line_id=sl.line_id    
         and sh.schedule_id=o.schedule_id
         and substr(sh.so_no,1,1)=5                
         group by sh.ou_code,sh.so_no,sh.line_id,SL.ORDERED_QUANTITY,
               nvl(sh.nyf_cus_po,'N'),sh.batch_no,sh.schedule_id,
               sh.customer_id,sh.customer_name,sh.customer_type,
               sh.item_code,sh.item_desc,
               sh.color_code,sh.color_desc,
               decode(sh.tubular_type,'1','tubular','2','Open',null),
               sh.so_no_date,sh.close_date,sh.sale_id,sh.sale_name,
               decode(sh.job_type,'N','Normal','D','Repair.Internal','R','Repair.External'),
               sh.total_qty, sh.send_qt_date ,sh.rec_qt_date ,sh.App_ColorLip_Date,
               get_step_name_eng (m.step_no),m.machine_no,m.method_cont,m.seq_no,
               m.plan_date,m.start_date,m.end_date,o.SCH_CLOSED,o.SCH_CLOSED_DATE
     order by sh.so_no,sh.line_id,sh.schedule_id,sh.batch_no,to_number(m.method_cont||m.seq_no)""")
  
  _csv = r"C:\QVD_DATA\NAYLIT\Prod_Time_Att_Process.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  conn.close()
  printttime(_csv)
  
  
###################################################################  
class CLS_Wip_Batch_Dye_Prod(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        Wip_Batch_Dye_Prod()
        
def Wip_Batch_Dye_Prod():
  my_dsn = cx_Oracle.makedsn("172.16.6.76",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyis", password="nyis", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()

  cursor.execute(""" SELECT sh.ou_code ou_code,sh.so_no,sh.line_id,
        nvl(sh.nyf_cus_po,'N') po_no,sh.batch_no,sh.schedule_id,
        sh.customer_id,sh.customer_name customer_name,sh.customer_type,
        sh.item_code,sh.item_desc item_desc,
        sh.color_code,sh.color_desc color_desc,
        decode(sh.tubular_type,'1','tubular','2','Open',null) tubular_type,SL.ORDERED_QUANTITY soline_order,
        sh.so_no_date,sh.sale_id,sh.sale_name,
        decode(sh.job_type,'N','Normal','D','Repair.Internal','R','Repair.External') Bt_job_type,
        decode(sh.job_type,'N',sh.total_qty,0) Grey_RM_QTY,
        sh.total_qty Batch_qty, --sh.send_qt_date QT_REC_DATE,sh.rec_qt_date QT_Send_Result_date,sh.App_ColorLip_Date,
        sh.batch_entry_date ,ph.pl_date Last_Pack_date,trunc(ph.shipment_date) Last_RecWH_date,
        sh.close_date batch_close_date,
        get_pldh_tot_sum(Ph.Ou_Code,Ph.Pl_No,Ph.Batch_no) Pack_qty,
        get_plwh_tot_naylit(Ph.Ou_Code,Ph.Pl_No,Ph.Batch_no) rec_wh_qty, --     nvl((select sum(w.fabric_weight) from fccs_bu2.dfwh_warehouse@salesf5.world w where w.ou_code=sh.ou_code and w.batch_no=sh.batch_no and w.pl_no=ph.pl_no),0) rec_wh_qty,
        DECODE(NVL(o.SCH_CLOSED,'N'),'N','Normal',
                                   'Y','Closed',
                                   'H','Hold',
                                   'C','Cancel') SCH_STATUS,o.SCH_CLOSED_DATE,
        (select r.remark_batch from demo.dfbt_header r 
         where r.ou_code=sh.ou_code 
         and r.batch_no=sh.batch_no) remark_batch,
        ph.pl_no,PH.GRADE_NO
  FROM demo.dfit_btdata sh,demo.dfora_so_line sl,
       demo.dfpl_header ph,demo.dfit_mc_schedule o
  WHERE nvl(sh.status,'0')<>'9'
         and sh.ou_code = ph.ou_code(+)
         AND sh.batch_no = ph.batch_no(+)
         and sh.so_no=sl.so_no
         and sh.line_id=sl.line_id    
         and sh.schedule_id=o.schedule_id
         and substr(sh.so_no,1,1)=5                
         AND sh.so_no_date >=  TO_DATE('01/01/2019','dd/mm/yyyy')+0.00000
     order by sh.so_no,sh.line_id,sh.schedule_id,sh.batch_no """)
  # cursor.execute("""SELECT sh.ou_code ou_code,sh.so_no,sh.line_id,
  #       nvl(sh.nyf_cus_po,'N') po_no,sh.batch_no,sh.schedule_id,
  #       sh.customer_id,sh.customer_name customer_name,sh.customer_type,
  #       sh.item_code,sh.item_desc item_desc,
  #       sh.color_code,sh.color_desc color_desc,
  #       decode(sh.tubular_type,'1','tubular','2','Open',null) tubular_type,SL.ORDERED_QUANTITY soline_order,
  #       sh.so_no_date,sh.sale_id,sh.sale_name,
  #       decode(sh.job_type,'N','Normal','D','Repair.Internal','R','Repair.External') Bt_job_type,
  #       decode(sh.job_type,'N',sh.total_qty,0) Grey_RM_QTY,
  #       sh.total_qty Batch_qty, --sh.send_qt_date QT_REC_DATE,sh.rec_qt_date QT_Send_Result_date,sh.App_ColorLip_Date,
  #       sh.batch_entry_date ,max(ph.pl_date) Last_Pack_date,max(trunc(ph.shipment_date)) Last_RecWH_date,
  #       sh.close_date batch_close_date,
  #       sum(get_pldh_tot_sum(Ph.Ou_Code,Ph.Pl_No,Ph.Batch_no)) Pack_qty,
  #       sum(get_plwh_tot_naylit(Ph.Ou_Code,Ph.Pl_No,Ph.Batch_no)) rec_wh_qty, --     nvl((select sum(w.fabric_weight) from fccs_bu2.dfwh_warehouse@salesf5.world w where w.ou_code=sh.ou_code and w.batch_no=sh.batch_no and w.pl_no=ph.pl_no),0) rec_wh_qty,
  #        DECODE(NVL(o.SCH_CLOSED,'N'),'N','Normal',
  #                                  'Y','Closed',
  #                                  'H','Hold',
  #                                  'C','Cancel') SCH_STATUS,o.SCH_CLOSED_DATE
  # FROM demo.dfit_btdata sh,demo.dfora_so_line sl,
  #      demo.dfpl_header ph,demo.dfit_mc_schedule o
  # WHERE nvl(sh.status,'0')<>'9'
  #        and sh.ou_code = ph.ou_code(+)
  #        AND sh.batch_no = ph.batch_no(+)
  #        and sh.so_no=sl.so_no
  #        and sh.line_id=sl.line_id    
  #        and sh.schedule_id=o.schedule_id
  #        and substr(sh.so_no,1,1)=5                
  #        group by sh.ou_code,sh.so_no,sh.line_id,SL.ORDERED_QUANTITY,
  #              nvl(sh.nyf_cus_po,'N'),sh.batch_no,sh.schedule_id,
  #              sh.customer_id,sh.customer_name,sh.customer_type,
  #              sh.item_code,sh.item_desc,
  #              sh.color_code,sh.color_desc,
  #              decode(sh.tubular_type,'1','tubular','2','Open',null),
  #              sh.so_no_date,sh.batch_entry_date,sh.close_date,sh.sale_id,sh.sale_name,
  #              sh.job_type,sh.total_qty, --sh.send_qt_date ,sh.rec_qt_date ,sh.App_ColorLip_Date,
  #              o.SCH_CLOSED,o.SCH_CLOSED_DATE
  #    order by sh.so_no,sh.line_id,sh.schedule_id,sh.batch_no""")
  
  _csv = r"C:\QVD_DATA\NAYLIT\Wip_Batch_Dye_Prod.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  conn.close()
  printttime(_csv)
  
  
###################################################################  
class CLS_Inventory_FG_Ageing_Onhand(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        Inventory_FG_Ageing_Onhand()
        
def Inventory_FG_Ageing_Onhand():
  my_dsn = cx_Oracle.makedsn("172.16.6.76",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyis", password="nyis", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""SELECT WH.LOC_NO,trunc(WH.RECEIVE_DATE) RECEIVE_DATE, -- trunc(WH.SHIPMENT_DATE) SHIPMENT_DATE,
              WH.SALE_ID,s.SALE_NAME,WH.CUSTOMER_ID,WH.CUSTOMER_NAME,
              WH.SO_NO,nvl(h.nyf_cus_po,'N') po_no,-- WH.PL_NO,Decode(WH.JOB_TYPE,'N','NORMAL','D','INTERNAL-REPAIR','R','EXTERNAL') Job_Type,
              WH.PL_NO,WH.BATCH_NO,WH.ITEM_NYF,WH.ITEM_NYF_DESC,
              WH.COLOR_ID,WH.COLOR_DESC,trunc(WH.APPOINT_DATE) APPOINT_DATE,--    WH.FABRIC_STATUS,
              WH.ROLL,WH.WEIGHT,WH.DOZ,WH.YARD,
              nvl(FN_MPS_WEEK(WH.BATCH_NO, WH.SO_NO),0) MPS_WEEK,
              nvl(h.customer_fg,0) FG_WEEK, --FN_FG_WEEK(WH.SO_NO) FG_WEEK,
              decode(WH.FABRIC_TYPE,'PURCHASE','PURCHASE','DYEHOUSE') FABRIC_TYPE,
              h.buyyer Buyer,WH.OU_CODE,s.team_name,
              to_char(WH.SHIPMENT_DATE,'IW') REC_WW,wh.WIDTH,
              wh.WEIGHT_G,wh.FG_TYPE,wh.grade,(trunc(sysdate) - trunc(WH.RECEIVE_DATE)) ageing_day,
              WH.PL_COLORLAB,nvl(WH.SCHEDULE_ID,1) schedule_id
      FROM fccs_bu2.DFWH_WAREHOUSE_VIEW@salesf5.world WH,
             sf5.dfora_sale@salesf5.world s,
             sf5.smit_so_header@salesf5.world h
      WHERE  wh.so_no=h.so_no(+)
      and wh.sale_id = s.sale_id(+)
      --AND WH.RECEIVE_DATE >=  TO_DATE(V_TS,'dd/mm/yyyy')+0.00000
      --AND WH.RECEIVE_DATE <=  TO_DATE(V_TE,'dd/mm/yyyy')+0.99999
      --AND WH.ITEM_CODE = NVL(V_ITEM1,WH.ITEM_CODE)
      --AND WH.PL_NO = NVL(V_PL,WH.PL_NO)
      --AND WH.SO_NO = NVL(V_SO,WH.SO_NO)
      --and wh.loc_no >= nvl(V_LOC1,wh.loc_no)
      --and wh.loc_no <= nvl(V_LOC2,wh.loc_no)
      --and nvl(h.nyf_cus_po,'N') =nvl(V_PO,nvl(h.nyf_cus_po,'N'))
      """)
  
  _csv = r"C:\QVD_DATA\NAYLIT\Inventory_FG_Ageing_Onhand.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  conn.close()
  printttime(_csv)
  
  
###################################################################  
class CLS_FG_Issued(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        FG_Issued()
        
def FG_Issued():
  my_dsn = cx_Oracle.makedsn("172.16.6.76",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyis", password="nyis", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()


  cursor.execute("""SELECT trunc(DECODE (wh.fabric_status,'RETURN',WH.RETURN_DATE,WH.SHIPPING_DATE))  ISSUE_DATE,  
        DECODE (wh.fabric_status,  'SHIPPED', 'CUSTOMER',  'RETURN'  ) issue_type,
        DECODE (SUBSTR (wh.item_code, 1, 1),  'C', 'COLLOR',  'FABRIC'  ) fabric_type, WH.batch_no,
        P.PACKING_NO,P.PK_TYPE,P.INV_NO,P.SO_NO,P.LINE_ID so_line, p.customer_id,
        P.CUSTOMER_DESC CUSTOMER_NAME,WH.OU_CODE,WH.PL_NO,WH.ITEM_NYF,wh.item_code,wh.item_desc,
        count(wh.fabric_id) TOT_ROLL,
        ROUND(SUM(NVL( WH.fabric_WEIGHT,0)),2)  WEIGHT_kg,
        ROUND(SUM(NVL( WH.yard,0)),2)  yard,
        ROUND(SUM(NVL( WH.unit_qty,0)),2)  unit_qty,
        round(SUM(WH.fabric_WEIGHT*nvl(wh.rm_cost_kg,0))/SUM(WH.fabric_WEIGHT),2) rm_cost_kg,
        round(SUM(WH.fabric_WEIGHT*nvl(wh.nyk_dye_price,0))/SUM(WH.fabric_WEIGHT),2) Dye_Price_kg,
        round(SUM(WH.fabric_WEIGHT*(nvl(wh.nyk_dye_price,0)+nvl(wh.rm_cost_kg,0)))/SUM(WH.fabric_WEIGHT),2) FG_COST_kg,
        c.nyf_pr_selling selling_pr_kg,nvl(c.uom,'KG') unit_uom,
        S1.SALE_ID,S1.SALE_NAME,s1.TEAM_NAME,b.buyyer Buyer 
    FROM  fccs_bu2.dfwh_warehouse@salesf5.world WH, fccs_bu2.nyf_fgpk_header@salesf5.world p,
          sf5.DFORA_SALE@salesf5.world S1,
         SF5.smit_so_header@salesf5.world B,SF5.smit_so_line@salesf5.world c
    WHERE  P.SALE_ID=S1.SALE_ID(+)
    and p.so_no=b.so_no
    and p.so_no=c.so_no(+) and P.LINE_ID=c.line_id(+)
    AND decode(wh.fabric_status,'RETURN',WH.RETURN_DATE,WH.SHIPPING_DATE) >=  to_date('01/01/2019','dd/mm/yyyy')+0.00000
    AND WH.SHIPPED_PL_NO=P.PACKINg_no
    AND NVL(P.CANCEL_ACTIVE,'N')='N'
    GROUP BY DECODE (SUBSTR (wh.item_code, 1, 1),  'C', 'COLLOR', 'FABRIC'  )  ,
             DECODE (wh.fabric_status,  'SHIPPED', 'CUSTOMER',  'RETURN' ),
             trunc(DECODE (wh.fabric_status,'RETURN',WH.RETURN_DATE,WH.SHIPPING_DATE)) , 
             P.PACKING_NO,P.PK_TYPE,P.INV_NO,P.SO_NO,P.LINE_ID,p.customer_id,P.CUSTOMER_DESC,WH.OU_CODE ,WH.PL_NO ,
             WH.ITEM_NYF,WH.ITEM_CODE,wh.item_desc,c.nyf_pr_selling ,nvl(c.uom,'KG'),WH.batch_no,
             S1.SALE_ID,S1.SALE_NAME,s1.TEAM_NAME,b.buyyer""" )     

  
  _csv = r"C:\QVD_DATA\NAYLIT\FG_Issued.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  conn.close()
  printttime(_csv)
  
  
  
  
  
class CLS_OmFollowUpByProcessClosed(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        OmFollowUpByProcessClosed()
        
def OmFollowUpByProcessClosed():
  
  my_dsn = cx_Oracle.makedsn("172.16.6.74",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYTG", password="NYTG", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""SELECT TO_NUMBER(TO_CHAR(C.SO_NO_DATE,'YYYY'))SO_YEAR, TO_NUMBER(TO_CHAR(C.SO_NO_DATE,'IW'))SO_WEEK,
                                                          C.CUSTOMER_YEAR FG_YEAR, C.CUSTOMER_FG FG_WEEK,
                                                          A.WORK_WEEK_YEAR CUSTOMER_YEAR , A.WORK_WEEK_NO CUSTOMER_FG ,
                                                          A.SO_NO,GET_SO_DUMMY(A.SO_NO) RS_NO, REPLACE(C.PO_NO,'+','') PO_NO, A.KP_NO , A.CUSTOMER_ID CUSTOMER, REPLACE(A.END_BUYER,'''') BUYER,
                                                         E.SALES_SEGMENT DIVISION, A.TEAM_NAME, A.SALE_ID SALE,
                                                          A.LINE_ID SO_LINE, A.ITEM_CODE, A.ITEM_DESC,REPLACE(NVL(A.PL_COLOR_LAB, A.COLOR_CODE), '- ',' ') PL_COLOR_LAB, REPLACE(A.COLOR_DESC,'- ',' ') COLOR_DESC,
                                                          A.SCHEDULE_ID PROD_ID, A.KNIT_MC_GROUP KNIT_GROUP, A.KNIT_MC_GUAGE KNIT_GUAGE,
                                                         DECODE(A.KNIT_MC_CAT,'COMCL','ว่าจ้างทอปก','COMKN','ว่าจ้างทอผ้า','GFSTOCK','GF-STOCK','FQC',GET_KNIT_TYPE_FQC(A.SCHEDULE_ID, A.SO_NO, A.LINE_ID, A.ITEM_CODE),NULL,GET_KNIT_TYPE_FQC(A.SCHEDULE_ID, A.SO_NO, A.LINE_ID, A.ITEM_CODE),'ทอเพชรบุรี') KNIT_TYPE,
                                                          A.MC_GROUP DH_GROUP, A.MACHINE_NO  DH_NO,
                                                         NVL(A.FABRIC_QUANTITY,0) MPS_QTY,  NVL(B.NYK_REC_WEIGHT,0) RM_QTY, NVL(A.FABRIC_QUANTITY,0) *0.9  TARGET_QTY,
                                                         DECODE(NVL(A.SCH_CLOSED,'N'),'Y',0, ((NVL(A.FABRIC_QUANTITY,0))-NVL(B.NYK_FG_WEIGHT,0))) WIP_QTY,
                                                          A.GREY_ACTIVE,A.ME_DATE BOM_DATE, A.FABRIC_RECEIVE GREY_IN, 
                                                          B.NYF_SHP_DATE,B.NYK_REC_DATE,B.DYE_START_DATE,B.NYK_FG_DATE, TO_CHAR(B.NYK_FG_DATE,'IYYY/IW') NYK_FG_WEEK,
                                                         A.SCH_CLOSED,A.SCH_CLOSED_DATE,
                                                            GET_MPS_WIP_STEP (A.SCHEDULE_ID,
                                                                                                      DECODE(NVL(A.SCH_CLOSED,'N'),'Y','Y', NVL(A.GREY_ACTIVE,'N')),
                                                                                                      DECODE(NVL(A.SCH_CLOSED,'N'),'Y', A.SCH_CLOSED_DATE, A.FABRIC_RECEIVE),
                                                                                                       DECODE(NVL(A.SCH_CLOSED,'N'),'Y', A.SCH_CLOSED_DATE, NVL(B.NYK_REC_DATE, NVL(B.DYE_START_DATE, NVL(B.DYE_END_DATE,B.NYK_FG_DATE)))),
                                                                                                       DECODE(NVL(A.SCH_CLOSED,'N'),'Y', 1, DECODE(NVL(B.DYE_START_DATE, NVL(B.DYE_END_DATE, B.NYK_FG_DATE)),NULL,B.NO_BATCH,1)),
                                                                                                       DECODE(NVL(A.SCH_CLOSED,'N'),'Y', A.SCH_CLOSED_DATE, DECODE(SUBSTR(A.SCHEDULE_ID,1,1),'8',SYSDATE,NVL(B.DYE_START_DATE, NVL(B.DYE_END_DATE, B.NYK_FG_DATE)))),
                                                                                                       A.SCH_CLOSED) WIP_STEP,
                                                         D.FQC_REASON, D.LAST_BATCH, D.BATCH_STEP, D.BATCH_ROLL, D.BATCH_QTY, D.REC_LEAD_REC,
                                                          TRUNC(NVL(A.FABRIC_RECEIVE, SYSDATE))- TRUNC(A.ME_DATE) LEAD_TIME_GREY_IN,
                                                         TRUNC(B.DYE_START_DATE) - TRUNC(B.NYK_REC_DATE) REC_TO_DYED,
                                                         GET_RANGE_GROUP7(NVL(TRUNC(B.NYK_FG_DATE),  NVL(TRUNC(A.SCH_CLOSED_DATE), TRUNC(SYSDATE))) - TRUNC(B.NYK_REC_DATE))  LEAD_TIME_NYK_REC_GROUP,
                                                         NVL(TRUNC(B.NYK_FG_DATE),  NVL(TRUNC(A.SCH_CLOSED_DATE), TRUNC(SYSDATE))) - TRUNC(B.NYK_REC_DATE)  LEAD_ITEM_NYK_REC, D.MIN_OU,
                                                         DECODE(SUBSTR(A.SCHEDULE_ID,1,1),'8','CS','5','FQC','9','Hot Line','Normal') MPS_TYPE,
                                                         DECODE(SUBSTR(A.SO_NO,4,2),'16','Salesman','18','Salesman','91','Salesman','13','Salesman','28','Salesman','61','Pilot Run','62','Pilot Run','Normal') SO_TYPE,
                                                         DECODE(SUBSTR(A.NYK_ITEM_PROCESS,1,3),'NC0','NP',DECODE (SUBSTR (NVL(A.PL_COLOR_LAB, 'NULL'), 1, 2), 'CO','YARN-DYED', 'RD', 'TOP-DYED','PIECE-DYED')) DYE_TYPE,
                                                         DECODE(SUBSTR(A.ITEM_CODE,1,1),'C','Collar','Fabric') ITEM_TYPE,
                                                         GET_FQC_CAUSE(A.SCHEDULE_ID) FQC_CAUSE,
                                                         GET_FQC_PROBLEM (A.SCHEDULE_ID) FQC_PROBLEM,
                                                         A.MPS_CONDITION,
                                                           (SUBSTR(A.ITEM_CODE,LENGTH(A.ITEM_CODE)-1,1)) ITEM_SHADE,
                                                           DECODE(A.TUBULAR_TYPE,'1','อบกลม','2','อบผ่า','A','อบผ่า','B','อบกลม,null') TUBULAR_TYPE,
                                                           (SELECT T.ITEM_CATEGORY FROM SF5.FMIT_ITEM T WHERE A.ITEM_CODE= T.ITEM_CODE) ITEM_CATEGORY,
                                                           (SELECT T.O_YARN_COUNT FROM SF5.FMIT_ITEM T WHERE A.ITEM_CODE= T.ITEM_CODE) ITEM_YARN,
                                                           (SELECT T.MACHINE_GROUP FROM SF5.FMIT_ITEM T WHERE A.ITEM_CODE= T.ITEM_CODE) ITEM_MC_GROUP,
                                                           (SELECT T.O_GAUGE FROM SF5.FMIT_ITEM T WHERE A.ITEM_CODE= T.ITEM_CODE) ITEM_GAUGE,
                                                           C.FOB_CODE ,GET_SO_PRINT_TYPE(A.SO_NO , A.LINE_ID) PRINT_TYPE,
                                                           nvl((select max(p.pack_type) from demo.dfbt_header@replica1.world p
                                                          where p.pack_type is not null and p.schedule_id=a.schedule_id),'NORMAL') PACK_TYPE,
                                                          DM.EDD_YEAR2,
                                                          DM.EDD_WEEK2,
                                                          A.COM_DATE,
                                                           L.ORDERED_QUANTITY ORDER_QTY,
                                                           L.CUST_ORDER_QTY CUST_ORDER ,
                                                           (select sum(wh.fabric_weight)
                                                            from fccs_bu2.dfwh_warehouse wh
                                                            where wh.so_no =  a.SO_NO
                                                              and wh.line_id = l.LINE_ID
                                                              and wh.SCHEDULE_ID = a.SCHEDULE_ID) FG_QTY,
                                                           B.NYK_SHP_CUST SHIP_CUST_QTY,
                                                           NVL(B.NYK_FG_WEIGHT,0)-NVL(B.NYK_SHP_CUST,0) ONHAND_QTY,
                                                          (case when (SELECT T.ITEM_CATEGORY FROM SF5.FMIT_ITEM T WHERE A.ITEM_CODE= T.ITEM_CODE)= 'SEAMLESS' then 'Seamless' else DECODE(SUBSTR(A.ITEM_CODE,1,1),'C','Collar','Fabric') end) fabric_type,                                                           
                                                           A.TEAM_WORK_YEAR||'/'||A.TEAM_WORK_WEEK DYE_LOAD_WEEK,
                                                           to_char(d.nyk_shpcust_date,'IW') start_ship_week,
                                                           to_char(d.customer_shp_date,'IW') end_ship_week,
                                                           c.style customer_style, c.customer_ref , l.order_type_id size_code,
                                                           sf5.chk_bt_act_wip(A.SCHEDULE_ID) Actual_wip,
                                                           (SELECT max(hq.fqc_date)  FROM SF5.SF5_FQC_CUSTOMER_H HQ WHERE HQ.SCHEDULE_NEW= a.SCHEDULE_ID) fqc_date,
                                                           (SELECT max(hq.mer_confirm_date)  FROM SF5.SF5_FQC_CUSTOMER_H HQ WHERE HQ.SCHEDULE_NEW= a.SCHEDULE_ID) mer_confirm_date,
                                                            L.UOM,
                                                           (select sum(wh.yard)
                                                            from fccs_bu2.dfwh_warehouse wh
                                                            where wh.so_no =  a.SO_NO
                                                              and wh.line_id = l.LINE_ID
                                                              and wh.SCHEDULE_ID = a.SCHEDULE_ID) FG_YARD_QTY,
                                                            (select sum(decode(FABRIC_STATUS,'SHIPPED',wh.yard,0))
                                                            from demo.dfwh_warehouse@REPLICA1.WORLD wh
                                                            where wh.so_no =  a.SO_NO
                                                              and wh.line_id = l.LINE_ID
                                                              and wh.SCHEDULE_ID = a.SCHEDULE_ID) SHIP_YARD_QTY,
                                                           (SELECT  c.WKPER_LOSS 
                                                            FROM sf5.DUMMY_COLOR_LINES   C, sf5.DUMMY_SO_HEADERS H
                                                            WHERE C.ORDER_NUMBER=H.ORDER_NUMBER
                                                            AND H.ORA_ORDER_NUMBER=a.SO_NO
                                                            AND C.R12_LINE_ID= l.LINE_ID) LOSS_PERCENT_DUMMY
                                           FROM SF5.DFIT_MC_SCHEDULE A,
                                                       SF5.DFIT_MC_SCH_OMNOI B,
                                                       SF5.SMIT_SO_HEADER C,
                                                       DFIT_MC_SCH_OMNOI D,
                                                       SF5.DFORA_SALE E,
                                                       SF5.DUMMY_SO_HEADERS DM,
                                                       SF5.SMIT_SO_LINE L
                                         WHERE (C.CUSTOMER_YEAR, C.CUSTOMER_FG) IN (SELECT NYEAR,NWEEK
                                                                                      FROM BAL_PERIOD_WW_V
                                                                                      WHERE PERIOD_TIME >= NVL('201901', PERIOD_TIME)
                                                                                      AND PERIOD_TIME <=  NVL('204052', PERIOD_TIME)
                                                                                    )
                                         AND  (A.WORK_WEEK_YEAR,A.WORK_WEEK_NO) IN (SELECT NYEAR,NWEEK
                                                                                      FROM BAL_PERIOD_WW_V
                                                                                      WHERE PERIOD_TIME >= NVL('201901', PERIOD_TIME)
                                                                                      AND PERIOD_TIME <=  NVL('204052', PERIOD_TIME))                                
                                           AND C.SO_NO = A.SO_NO
                                           AND A.SO_NO = L.SO_NO
                                           AND A.LINE_ID = L.LINE_ID
                                           AND A.SCHEDULE_ID = B.SCHEDULE_ID(+)
                                           AND B.SCHEDULE_ID = D.SCHEDULE_ID(+)
                                           AND A.NYK_CANCLE_SCH IS NULL
                                           AND A.FABRIC_QUANTITY>0
                                           AND E.SALE_ID (+) = C.SALE_ID
                                           AND LENGTH(A.SO_NO)<>11
                                           AND C.SO_STATUS NOT LIKE '00%'
                                           AND SUBSTR(C.SO_NO,1,1) = '5'
                                           --AND NVL(A.SCH_CLOSED,'N') = 'Y'
                                          AND A.SO_NO = DM.ORA_ORDER_NUMBER (+) """)
  
  _csv = r"C:\QVD_DATA\NAYLIT\OmFollowUpByProcessClosed.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  conn.close()
  printttime(_csv)
  
  
  
class CLS_OmFollowUpByProcessWIP(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        OmFollowUpByProcessWIP()
        
def OmFollowUpByProcessWIP():
  
  my_dsn = cx_Oracle.makedsn("172.16.6.74",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYTG", password="NYTG", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""SELECT TO_NUMBER(TO_CHAR(C.SO_NO_DATE,'YYYY'))SO_YEAR, TO_NUMBER(TO_CHAR(C.SO_NO_DATE,'IW'))SO_WEEK,
                                                           C.CUSTOMER_YEAR FG_YEAR, C.CUSTOMER_FG FG_WEEK,
                                                          A.WORK_WEEK_YEAR CUSTOMER_YEAR , A.WORK_WEEK_NO CUSTOMER_FG ,
                                                         A.SO_NO,GET_SO_DUMMY(A.SO_NO) RS_NO, REPLACE(C.PO_NO,'+','') PO_NO, A.KP_NO , A.CUSTOMER_ID CUSTOMER, REPLACE(A.END_BUYER,'''') BUYER,
                                                         E.SALES_SEGMENT DIVISION, A.TEAM_NAME, A.SALE_ID SALE,
                                                          A.LINE_ID SO_LINE, A.ITEM_CODE, A.ITEM_DESC,REPLACE(NVL(A.PL_COLOR_LAB, A.COLOR_CODE), '- ',' ') PL_COLOR_LAB, REPLACE(A.COLOR_DESC,'- ',' ') COLOR_DESC,
                                                          A.SCHEDULE_ID PROD_ID, A.KNIT_MC_GROUP KNIT_GROUP, A.KNIT_MC_GUAGE KNIT_GUAGE,
                                                         DECODE(A.KNIT_MC_CAT,'COMCL','ว่าจ้างทอปก','COMKN','ว่าจ้างทอผ้า','GFSTOCK','GF-STOCK','FQC',GET_KNIT_TYPE_FQC(A.SCHEDULE_ID, A.SO_NO, A.LINE_ID, A.ITEM_CODE),NULL,GET_KNIT_TYPE_FQC(A.SCHEDULE_ID, A.SO_NO, A.LINE_ID, A.ITEM_CODE),'ทอเพชรบุรี') KNIT_TYPE,
                                                          A.MC_GROUP DH_GROUP, A.MACHINE_NO  DH_NO,
                                                         NVL(A.FABRIC_QUANTITY,0) MPS_QTY,  NVL(B.NYK_REC_WEIGHT,0) RM_QTY, NVL(A.FABRIC_QUANTITY,0) *0.9  TARGET_QTY,
                                                         DECODE(NVL(A.SCH_CLOSED,'N'),'Y',0, ((NVL(A.FABRIC_QUANTITY,0))-NVL(B.NYK_FG_WEIGHT,0))) WIP_QTY,
                                                       A.GREY_ACTIVE,A.ME_DATE BOM_DATE, A.FABRIC_RECEIVE GREY_IN, --A.NYF_TRANS_DATE TRANS_DATE,
                                                          B.NYF_SHP_DATE,B.NYK_REC_DATE,B.DYE_START_DATE,B.NYK_FG_DATE, TO_CHAR(B.NYK_FG_DATE,'IYYY/IW') NYK_FG_WEEK,
                                                           A.SCH_CLOSED,A.SCH_CLOSED_DATE,
                                                            GET_MPS_WIP_STEP (A.SCHEDULE_ID,
                                                                                                      DECODE(NVL(A.SCH_CLOSED,'N'),'Y','Y', NVL(A.GREY_ACTIVE,'N')),
                                                                                                      DECODE(NVL(A.SCH_CLOSED,'N'),'Y', A.SCH_CLOSED_DATE, A.FABRIC_RECEIVE),
                                                                                                       DECODE(NVL(A.SCH_CLOSED,'N'),'Y', A.SCH_CLOSED_DATE, NVL(B.NYK_REC_DATE, NVL(B.DYE_START_DATE, NVL(B.DYE_END_DATE,B.NYK_FG_DATE)))),
                                                                                                       DECODE(NVL(A.SCH_CLOSED,'N'),'Y', 1, DECODE(NVL(B.DYE_START_DATE, NVL(B.DYE_END_DATE, B.NYK_FG_DATE)),NULL,B.NO_BATCH,1)),
                                                                                                       DECODE(NVL(A.SCH_CLOSED,'N'),'Y', A.SCH_CLOSED_DATE, DECODE(SUBSTR(A.SCHEDULE_ID,1,1),'8',SYSDATE,NVL(B.DYE_START_DATE, NVL(B.DYE_END_DATE, B.NYK_FG_DATE)))),
                                                                                                       A.SCH_CLOSED) WIP_STEP,
                                                         D.FQC_REASON, D.LAST_BATCH, D.BATCH_STEP, D.BATCH_ROLL, D.BATCH_QTY, D.REC_LEAD_REC,
                                                          TRUNC(NVL(A.FABRIC_RECEIVE, SYSDATE))- TRUNC(A.ME_DATE) LEAD_TIME_GREY_IN,
                                                         TRUNC(B.DYE_START_DATE) - TRUNC(B.NYK_REC_DATE) REC_TO_DYED,
                                                          GET_RANGE_GROUP7(NVL(TRUNC(B.NYK_FG_DATE),  NVL(TRUNC(A.SCH_CLOSED_DATE), TRUNC(SYSDATE))) - TRUNC(B.NYK_REC_DATE))  LEAD_TIME_NYK_REC_GROUP,
                                                         NVL(TRUNC(B.NYK_FG_DATE),  NVL(TRUNC(A.SCH_CLOSED_DATE), TRUNC(SYSDATE))) - TRUNC(B.NYK_REC_DATE)  LEAD_ITEM_NYK_REC, D.MIN_OU,
                                                         DECODE(SUBSTR(A.SCHEDULE_ID,1,1),'8','CS','5','FQC','9','Hot Line','Normal') MPS_TYPE,
                                                         DECODE(SUBSTR(A.SO_NO,4,2),'16','Salesman','18','Salesman','91','Salesman','13','Salesman','28','Salesman','61','Pilot Run','62','Pilot Run','Normal') SO_TYPE,
                                                         DECODE(SUBSTR(A.NYK_ITEM_PROCESS,1,3),'NC0','NP',DECODE (SUBSTR (NVL(A.PL_COLOR_LAB, 'NULL'), 1, 2), 'CO','YARN-DYED', 'RD', 'TOP-DYED','PIECE-DYED')) DYE_TYPE,
                                                         DECODE(SUBSTR(A.ITEM_CODE,1,1),'C','Collar','Fabric') ITEM_TYPE,
                                                         GET_FQC_CAUSE(A.SCHEDULE_ID) FQC_CAUSE,
                                                         GET_FQC_PROBLEM (A.SCHEDULE_ID) FQC_PROBLEM,
                                                         A.MPS_CONDITION,
                                                           (SUBSTR(A.ITEM_CODE,LENGTH(A.ITEM_CODE)-1,1)) ITEM_SHADE,
                                                           DECODE(A.TUBULAR_TYPE,'1','อบกลม','2','อบผ่า','A','อบผ่า','B','อบกลม,null') TUBULAR_TYPE,
                                                           (SELECT T.ITEM_CATEGORY FROM SF5.FMIT_ITEM T WHERE A.ITEM_CODE= T.ITEM_CODE) ITEM_CATEGORY,
                                                           (SELECT T.O_YARN_COUNT FROM SF5.FMIT_ITEM T WHERE A.ITEM_CODE= T.ITEM_CODE) ITEM_YARN,
                                                           (SELECT T.MACHINE_GROUP FROM SF5.FMIT_ITEM T WHERE A.ITEM_CODE= T.ITEM_CODE) ITEM_MC_GROUP,
                                                           (SELECT T.O_GAUGE FROM SF5.FMIT_ITEM T WHERE A.ITEM_CODE= T.ITEM_CODE) ITEM_GAUGE,
                                                           C.FOB_CODE ,GET_SO_PRINT_TYPE(A.SO_NO , A.LINE_ID) PRINT_TYPE,
                                                           nvl((select max(p.pack_type) from demo.dfbt_header@replica1.world p
                                                          where p.pack_type is not null and p.schedule_id=a.schedule_id),'NORMAL') PACK_TYPE,
                                                          DM.EDD_YEAR2,
                                                          DM.EDD_WEEK2,
                                                          A.COM_DATE,
                                                           L.ORDERED_QUANTITY ORDER_QTY,
                                                           L.CUST_ORDER_QTY CUST_ORDER ,
                                                           B.NYK_FG_WEIGHT FG_QTY,
                                                           B.NYK_SHP_CUST SHIP_CUST_QTY,
                                                           NVL(B.NYK_FG_WEIGHT,0)-NVL(B.NYK_SHP_CUST,0) ONHAND_QTY,
                                                           (case when (SELECT T.ITEM_CATEGORY FROM SF5.FMIT_ITEM T WHERE A.ITEM_CODE= T.ITEM_CODE)= 'SEAMLESS' then 'Seamless' else DECODE(SUBSTR(A.ITEM_CODE,1,1),'C','Collar','Fabric') end) fabric_type,
                                                           A.TEAM_WORK_YEAR||'/'||A.TEAM_WORK_WEEK DYE_LOAD_WEEK,
                                                           to_char(d.nyk_shpcust_date,'IW') start_ship_week,
                                                           to_char(d.customer_shp_date,'IW') end_ship_week,
                                                           c.style customer_style, c.customer_ref , l.order_type_id size_code,
                                                           sf5.chk_bt_act_wip(A.SCHEDULE_ID) Actual_wip,
                                                           (SELECT max(hq.fqc_date)  FROM SF5.SF5_FQC_CUSTOMER_H HQ WHERE HQ.SCHEDULE_NEW= a.SCHEDULE_ID) fqc_date,
                                                           (SELECT max(hq.mer_confirm_date)  FROM SF5.SF5_FQC_CUSTOMER_H HQ WHERE HQ.SCHEDULE_NEW= a.SCHEDULE_ID) mer_confirm_date,
                                                           L.UOM,
                                                           (select sum(wh.yard)
                                                            from demo.dfwh_warehouse@REPLICA1.WORLD wh
                                                            where wh.so_no =  a.SO_NO
                                                              and wh.line_id = l.LINE_ID
                                                              and wh.SCHEDULE_ID = a.SCHEDULE_ID) FG_YARD_QTY,
                                                            (select sum(decode(FABRIC_STATUS,'SHIPPED',wh.yard,0))
                                                            from demo.dfwh_warehouse@REPLICA1.WORLD wh
                                                            where wh.so_no =  a.SO_NO
                                                              and wh.line_id = l.LINE_ID
                                                              and wh.SCHEDULE_ID = a.SCHEDULE_ID) SHIP_YARD_QTY,
                                                           (SELECT  c.WKPER_LOSS 
                                                            FROM sf5.DUMMY_COLOR_LINES   C, sf5.DUMMY_SO_HEADERS H
                                                            WHERE C.ORDER_NUMBER=H.ORDER_NUMBER
                                                            AND H.ORA_ORDER_NUMBER=a.SO_NO
                                                            AND C.R12_LINE_ID= l.LINE_ID) LOSS_PERCENT_DUMMY
                                           FROM SF5.DFIT_MC_SCHEDULE A,
                                                       SF5.DFIT_MC_SCH_OMNOI B,
                                                       SF5.SMIT_SO_HEADER C,
                                                       DFIT_MC_SCH_OMNOI D,
                                                       SF5.DFORA_SALE E,
                                                       SF5.DUMMY_SO_HEADERS DM,
                                                       SF5.SMIT_SO_LINE L
                            WHERE (C.CUSTOMER_YEAR, C.CUSTOMER_FG) IN (SELECT NYEAR,NWEEK
                                                                                                                                                  FROM BAL_PERIOD_WW_V
                                                                                                                                       WHERE PERIOD_TIME >= NVL('201901', PERIOD_TIME)
                                                                                                                                       AND PERIOD_TIME <=  NVL('204052', PERIOD_TIME)
                                                                                                                                       )
                                        AND  (A.WORK_WEEK_YEAR,A.WORK_WEEK_NO) IN (SELECT NYEAR,NWEEK
                                                                                                                                                  FROM BAL_PERIOD_WW_V
                                                                                                                                       WHERE PERIOD_TIME >= NVL('201901', PERIOD_TIME)
                                                                                                                                       AND PERIOD_TIME <=  NVL('204052', PERIOD_TIME))          
                                           AND C.SO_NO = A.SO_NO
                                           AND A.SO_NO = L.SO_NO
                                           AND A.LINE_ID = L.LINE_ID
                                           AND A.SCHEDULE_ID = B.SCHEDULE_ID(+)
                                           AND B.SCHEDULE_ID = D.SCHEDULE_ID(+)
                                           AND A.NYK_CANCLE_SCH IS NULL
                                           AND A.FABRIC_QUANTITY>0
                                           AND E.SALE_ID (+) = C.SALE_ID
                                           AND LENGTH(A.SO_NO)<>11
                                           AND C.SO_STATUS NOT LIKE '00%'
                                           AND SUBSTR(C.SO_NO,1,1) = '5'
                                           AND A.SO_NO = DM.ORA_ORDER_NUMBER (+) """)
  _csv = r"C:\QVD_DATA\NAYLIT\OmFollowUpByProcessWIP.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  conn.close()
  printttime(_csv)


###################################################################
class CLS_FG_Receive(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    FG_Receive()


def FG_Receive():
  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyis", password="nyis", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()

  cursor.execute("""SELECT TRUNC(WH.RECEIVE_DATE)  RECEIVE_DATE,WH.SO_NO,WH.LINE_ID,WH.PO_NO,
            WH.CUSTOMER_NAME,WH.BUYER,
            WH.PL_NO,WH.SCHEDULE_ID,
            WH.OU_CODE,WH.BATCH_NO,WH.GRADE,WH.LOC_NO,
            WH.ITEM_CODE,WH.ITEM_DESC,WH.COLOR_ID,WH.COLOR_DESC,
            DECODE(WH.FABRIC_TYPE,'PURCHASE','PURCHASE','DYEHOUSE') FABRIC_TYPE,
            COUNT(WH.FABRIC_ID) TOT_ROLL,
            ROUND(SUM(NVL( WH.FABRIC_WEIGHT,0)),2)  WEIGHT_KG,
            ROUND(SUM(NVL( WH.YARD,0)),2)  YARD,
            ROUND(SUM(NVL( WH.UNIT_QTY,0)),2)  UNIT_QTY,
            ROUND(SUM(WH.FABRIC_WEIGHT*NVL(WH.RM_COST_KG,0))/SUM(WH.FABRIC_WEIGHT),2) RM_COST_KG,
            ROUND(SUM(WH.FABRIC_WEIGHT*NVL(WH.NYK_DYE_PRICE,0))/SUM(WH.FABRIC_WEIGHT),2) DYE_PRICE_KG,
            ROUND(SUM(WH.FABRIC_WEIGHT*(NVL(WH.NYK_DYE_PRICE,0)+NVL(WH.RM_COST_KG,0)))/SUM(WH.FABRIC_WEIGHT),2) FG_COST_KG,
            S.SALE_ID,S.SALE_NAME,S.TEAM_NAME
FROM FCCS_BU2.DFWH_WAREHOUSE@SALESF5.WORLD WH,SF5.SMIT_SO_LINE@SALESF5.WORLD C,
     SF5.DFORA_SALE@SALESF5.WORLD S
WHERE WH.SO_NO=C.SO_NO(+) 
AND WH.LINE_ID=C.LINE_ID(+)
AND WH.SALE_ID=S.SALE_ID(+)
GROUP BY  TRUNC(WH.RECEIVE_DATE),DECODE(WH.FABRIC_TYPE,'PURCHASE','PURCHASE','DYEHOUSE'),
          WH.SO_NO,WH.LINE_ID,WH.PO_NO,WH.CUSTOMER_NAME,WH.BUYER,
          WH.PL_NO,WH.SCHEDULE_ID,WH.BATCH_NO,WH.GRADE,
          WH.ITEM_CODE,WH.ITEM_DESC,WH.COLOR_ID,WH.COLOR_DESC,WH.OU_CODE,WH.LOC_NO
          ,S.SALE_ID,S.SALE_NAME,S.TEAM_NAME
ORDER BY 1,2,3""" )

  _csv = r"C:\QVD_DATA\NAYLIT\FG_Receive.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)
  conn.close()
  printttime(_csv)


class CLS_Wip_BT_detail(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    Wip_BT_detail()


def Wip_BT_detail():

  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""SELECT sh.ou_code ou_code,sh.so_no,sh.line_id,
        nvl(sh.nyf_cus_po,'N') po_no,sh.batch_no,sh.schedule_id,
        sh.customer_id,sh.customer_name customer_name,sh.customer_type,
        sh.item_code,sh.item_desc item_desc,
        sh.color_code,sh.color_desc color_desc,
        decode(sh.tubular_type,'1','tubular','2','Open',null) tubular_type,SL.ORDERED_QUANTITY soline_order,
        sh.so_no_date,sh.sale_id,sh.sale_name,
        decode(sh.job_type,'N','Normal','D','Repair.Internal','R','Repair.External') Bt_job_type,
        decode(sh.job_type,'N',sh.total_qty,0) Grey_RM_QTY,
        sh.total_qty Batch_qty, --sh.send_qt_date QT_REC_DATE,sh.rec_qt_date QT_Send_Result_date,sh.App_ColorLip_Date,
        sh.batch_entry_date ,ph.pl_date Last_Pack_date,trunc(ph.shipment_date) Last_RecWH_date,
        sh.close_date batch_close_date,
        get_pldh_tot_sum(Ph.Ou_Code,Ph.Pl_No,Ph.Batch_no) Pack_qty,
        get_plwh_tot_naylit(Ph.Ou_Code,Ph.Pl_No,Ph.Batch_no) rec_wh_qty, 
        DECODE(NVL(o.SCH_CLOSED,'N'),'N','Normal',
                                   'Y','Closed',
                                   'H','Hold',
                                   'C','Cancel') SCH_STATUS,o.SCH_CLOSED_DATE,
        (select r.remark_batch from demo.dfbt_header r 
         where r.ou_code=sh.ou_code 
         and r.batch_no=sh.batch_no) remark_batch,
         ph.pl_no,PH.GRADE_NO,
         GET_MPS_WIP_STEP (o.SCHEDULE_ID,
                           DECODE(NVL(o.SCH_CLOSED,'N'),'Y','Y', NVL(o.GREY_ACTIVE,'N')),
                           DECODE(NVL(o.SCH_CLOSED,'N'),'Y', o.SCH_CLOSED_DATE, o.FABRIC_RECEIVE),
                           DECODE(NVL(o.SCH_CLOSED,'N'),'Y', o.SCH_CLOSED_DATE, NVL(B.NYK_REC_DATE, NVL(B.DYE_START_DATE, NVL(B.DYE_END_DATE,B.NYK_FG_DATE)))),
                           DECODE(NVL(o.SCH_CLOSED,'N'),'Y', 1, DECODE(NVL(B.DYE_START_DATE, NVL(B.DYE_END_DATE, B.NYK_FG_DATE)),NULL,B.NO_BATCH,1)),
                           DECODE(NVL(o.SCH_CLOSED,'N'),'Y', o.SCH_CLOSED_DATE, DECODE(SUBSTR(o.SCHEDULE_ID,1,1),'8',SYSDATE,NVL(B.DYE_START_DATE, NVL(B.DYE_END_DATE, B.NYK_FG_DATE)))),
                           o.SCH_CLOSED) WIP_STEP
  FROM demo.dfit_btdata sh,demo.dfora_so_line sl,
       demo.dfpl_header ph,sf5.dfit_mc_schedule@SALESF5.WORLD o,SF5.DFIT_MC_SCH_OMNOI@SALESF5.WORLD B
  WHERE nvl(sh.status,'0')<>'9'
         and sh.ou_code = ph.ou_code(+)
         AND sh.batch_no = ph.batch_no(+)
         and sh.so_no=sl.so_no
         and sh.line_id=sl.line_id    
         and sh.schedule_id=o.schedule_id
         and sh.schedule_id=b.schedule_id
         and substr(sh.so_no,1,1)=5                
         AND sh.so_no_date >=  TO_DATE('01/01/2019','dd/mm/yyyy')+0.00000
        
     order by sh.so_no,sh.line_id,sh.schedule_id,sh.batch_no """)

  _csv = r"C:\QVD_DATA\NAYLIT\Wip_BT_detail.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)
  conn.close()
  printttime(_csv)


class CLS_schedule_data(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    schedule_data()

def schedule_data():

  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  cursor.execute("""SELECT M.*,
D.*,
D.NYK_REC_WEIGHT - D.FG_KG AS LOSS_KG
,CASE WHEN NVL(D.NYK_REC_WEIGHT,0) = 0 THEN 0 ELSE ROUND((D.NYK_REC_WEIGHT - D.FG_KG)*100/D.NYK_REC_WEIGHT,2) END LOSS_P
FROM
(
SELECT C.SO_NO, C.SO_NO_DATE SO_NO_DATE, C.PO_NO, C.REMARK, C.NYF_BUYER, C.CUSTOMER_YEAR, C.CUSTOMER_FG, TO_CHAR(C.SO_PROD_CLOSED,'YYYY-MM-DD') SO_PROD_CLOSED
    		--,(SELECT SUM(ORDERED_QUANTITY) FROM SF5.SMIT_SO_LINE L WHERE L.SO_NO = C.SO_NO) ORDERED_QUANTITY
            ,L.LINE_ID, L.ITEM_CODE, L.ITEM_DESC, L.COLOR_CODE, L.CANCLED_QUANTITY, L.ORDERED_QUANTITY, L.UOM, L.PL_COLORLAB
            ,(SELECT SUM(FABRIC_QUANTITY) FROM  SF5.DFIT_MC_SCHEDULE S WHERE S.SO_NO = L.SO_NO AND S.LINE_ID = L.LINE_ID AND S.NYK_CANCLE_SCH IS NULL) SCHEDULE_QUANTITY
			FROM SF5.SMIT_SO_HEADER C, SF5.SMIT_SO_LINE L
			WHERE C.SO_NO = L.SO_NO
            AND EXISTS (SELECT A.* FROM  SF5.DFIT_MC_SCHEDULE A
			WHERE C.SO_NO = A.SO_NO
			AND A.TEAM_NAME = 'T-NAYLIT')
        ) M,(
        SELECT S.SO_NO, S.LINE_ID , S.SCHEDULE_ID, S.FABRIC_QUANTITY, S.MACHINE_NO, S.MC_SIZE, S.TEAM_WORK_YEAR, S.TEAM_WORK_WEEK
    ,B.GREY_ACTIVE, TO_CHAR(B.FABRIC_RECEIVE,'YYYY-MM-DD') FABRIC_RECEIVE, TO_CHAR(B.NYK_REC_DATE,'YYYY-MM-DD') NYK_REC_DATE,NVL(B.NYK_REC_WEIGHT,0) NYK_REC_WEIGHT , TO_CHAR(B.DYE_END_DATE,'YYYY-MM-DD') DYE_END_DATE,
    TO_CHAR(B.NYK_FG_DATE,'YYYY-MM-DD') NYK_FG_DATE,B.NYK_FG_WEIGHT,B.NYK_FG_NO,B.NYK_SHP_CUST, TO_CHAR(B.NYK_SHPCUST_DATE,'YYYY-MM-DD') NYK_SHPCUST_DATE,
    (SELECT COUNT(BATCH_NO) FROM DFIT_BTDATA BT WHERE BT.SCHEDULE_ID = S.SCHEDULE_ID and STATUS<>'9') BATCH_CNT
    ,NVL(B.nyk_fg_weight,0) AS FG_KG, NVL(B.nyk_fg_target,0) AS FG_YARD
    ,(select max(p.po_no) 
from fccs_bu2.fmit_pk_header p
where p.schedule_id=S.SCHEDULE_ID) po_RM
,GET_MPS_WIP_STEP (S.SCHEDULE_ID,
                           DECODE(NVL(S.SCH_CLOSED,'N'),'Y','Y', NVL(s.GREY_ACTIVE,'N')),
                           DECODE(NVL(s.SCH_CLOSED,'N'),'Y', s.SCH_CLOSED_DATE, s.FABRIC_RECEIVE),
                           DECODE(NVL(s.SCH_CLOSED,'N'),'Y', s.SCH_CLOSED_DATE, NVL(B.NYK_REC_DATE, NVL(B.DYE_START_DATE, NVL(B.DYE_END_DATE,B.NYK_FG_DATE)))),
                           DECODE(NVL(s.SCH_CLOSED,'N'),'Y', 1, DECODE(NVL(B.DYE_START_DATE, NVL(B.DYE_END_DATE, B.NYK_FG_DATE)),NULL,B.NO_BATCH,1)),
                           DECODE(NVL(s.SCH_CLOSED,'N'),'Y', s.SCH_CLOSED_DATE, DECODE(SUBSTR(s.SCHEDULE_ID,1,1),'8',SYSDATE,NVL(B.DYE_START_DATE, NVL(B.DYE_END_DATE, B.NYK_FG_DATE)))),
                           s.SCH_CLOSED) WIP_STEP
            FROM SF5.DFIT_MC_SCHEDULE S, DFIT_MC_SCH_OMNOI B
            WHERE S.SCHEDULE_ID = B.SCHEDULE_ID(+)
            AND S.NYK_CANCLE_SCH IS NULL
            AND substr( S.SO_NO,1,1) = '5'
        ) D
   WHERE M.SO_NO = D.SO_NO
   AND M.LINE_ID = D.LINE_ID
   ORDER BY M.SO_NO, M.LINE_ID, D.SCHEDULE_ID  """)

  _csv = r"C:\QVD_DATA\NAYLIT\schedule_data.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)
  conn.close()
  printttime(_csv)


class CLS_REC_FG(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    REC_FG()

def REC_FG():

  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
 
  sql = """SELECT TRUNC(WH.RECEIVE_DATE)  RECEIVE_DATE,WH.SO_NO,WH.LINE_id,wh.po_no,
            WH.CUSTOMER_NAME,wh.BUYER,
            WH.PL_NO,wh.schedule_id,
            wh.ou_code,WH.BATCH_NO,wh.grade,wh.loc_no,
            wh.item_code,WH.ITEM_DESC,wh.color_id,wh.color_desc,
            decode(WH.FABRIC_TYPE,'PURCHASE','PURCHASE','DYEHOUSE') FABRIC_TYPE,
            count(wh.fabric_id) TOT_ROLL,
            ROUND(SUM(NVL( WH.fabric_WEIGHT,0)),2)  WEIGHT_kg,
            ROUND(SUM(NVL( WH.yard,0)),2)  yard,
            ROUND(SUM(NVL( WH.unit_qty,0)),2)  unit_qty,
            round(SUM(WH.fabric_WEIGHT*nvl(wh.rm_cost_kg,0))/SUM(WH.fabric_WEIGHT),2) rm_cost_kg,
            round(SUM(WH.fabric_WEIGHT*nvl(wh.nyk_dye_price,0))/SUM(WH.fabric_WEIGHT),2) Dye_Price_kg,
            round(SUM(WH.fabric_WEIGHT*(nvl(wh.nyk_dye_price,0)+nvl(wh.rm_cost_kg,0)))/SUM(WH.fabric_WEIGHT),2) FG_COST_kg,
            S.SALE_ID,S.SALE_NAME,s.team_name
FROM fccs_bu2.dfwh_warehouse@salesf5.world WH,sf5.smit_so_line@salesf5.world c,
     sf5.DFORA_SALE@salesf5.world S
WHERE WH.so_no=c.so_no(+) 
and wh.line_id=c.line_id(+)
AND WH.SALE_ID=S.SALE_ID(+)
AND WH.RECEIVE_DATE >=  TO_DATE('01/01/2019','dd/mm/yyyy')+0.00000
GROUP BY  TRUNC(WH.RECEIVE_DATE),decode(WH.FABRIC_TYPE,'PURCHASE','PURCHASE','DYEHOUSE'),
          WH.SO_NO,WH.LINE_id,wh.po_no,WH.CUSTOMER_NAME,wh.BUYER,
          WH.PL_NO,wh.schedule_id,WH.BATCH_NO,wh.grade,
          WH.ITEM_CODE,wh.item_desc,wh.color_id,wh.color_desc,wh.ou_code,wh.loc_no
          ,S.SALE_ID,S.SALE_NAME,s.team_name
order by 1,2,3  """

  df = pd.read_sql_query(sql, conn)

  df.fillna("", inplace=True)

  df.to_excel(
      r'C:\QVD_DATA\NAYLIT\REC_FG.xlsx', index=False)
  conn.close()
  printttime(r'C:\QVD_DATA\NAYLIT\REC_FG.xlsx')

class CLS_ISSUE_FG(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    ISSUE_FG()

def ISSUE_FG():

  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
 
  sql = """ SELECT trunc(DECODE (wh.fabric_status,'RETURN',WH.RETURN_DATE,WH.SHIPPING_DATE))  ISSUE_DATE,  
        DECODE (wh.fabric_status,  'SHIPPED', 'CUSTOMER',  'RETURN'  ) issue_type,
        DECODE (SUBSTR (wh.item_code, 1, 1),  'C', 'COLLOR',  'FABRIC'  ) fabric_type,
        P.PACKING_NO,P.PK_TYPE,P.INV_NO,P.SO_NO,P.LINE_ID so_line, p.customer_id,
        P.CUSTOMER_DESC CUSTOMER_NAME,WH.OU_CODE,WH.PL_NO,WH.ITEM_NYF,wh.item_code,wh.item_desc,
        count(wh.fabric_id) TOT_ROLL,
        ROUND(SUM(NVL( WH.fabric_WEIGHT,0)),2)  WEIGHT_kg,
        ROUND(SUM(NVL( WH.yard,0)),2)  yard,
        ROUND(SUM(NVL( WH.unit_qty,0)),2)  unit_qty,
        round(SUM(WH.fabric_WEIGHT*nvl(wh.rm_cost_kg,0))/SUM(WH.fabric_WEIGHT),2) rm_cost_kg,
        round(SUM(WH.fabric_WEIGHT*nvl(wh.nyk_dye_price,0))/SUM(WH.fabric_WEIGHT),2) Dye_Price_kg,
        round(SUM(WH.fabric_WEIGHT*(nvl(wh.nyk_dye_price,0)+nvl(wh.rm_cost_kg,0)))/SUM(WH.fabric_WEIGHT),2) FG_COST_kg,
        c.nyf_pr_selling selling_pr_kg,nvl(c.uom,'KG') unit_uom,
        S1.SALE_ID,S1.SALE_NAME,s1.TEAM_NAME,b.buyyer Buyer 
    FROM  fccs_bu2.dfwh_warehouse@salesf5.world WH, fccs_bu2.nyf_fgpk_header@salesf5.world p,
          sf5.DFORA_SALE@salesf5.world S1,
         SF5.smit_so_header@salesf5.world B,SF5.smit_so_line@salesf5.world c
    WHERE  P.SALE_ID=S1.SALE_ID(+)
    and p.so_no=b.so_no
    and p.so_no=c.so_no(+) and P.LINE_ID=c.line_id(+)
    AND decode(wh.fabric_status,'RETURN',WH.RETURN_DATE,WH.SHIPPING_DATE) >=  TO_DATE('01/01/2019','dd/mm/yyyy')+0.00000
    AND WH.SHIPPED_PL_NO=P.PACKINg_no
    AND NVL(P.CANCEL_ACTIVE,'N')='N'
    GROUP BY DECODE (SUBSTR (wh.item_code, 1, 1),  'C', 'COLLOR', 'FABRIC'  )  ,
             DECODE (wh.fabric_status,  'SHIPPED', 'CUSTOMER',  'RETURN' ),
             trunc(DECODE (wh.fabric_status,'RETURN',WH.RETURN_DATE,WH.SHIPPING_DATE)) , 
             P.PACKING_NO,P.PK_TYPE,P.INV_NO,P.SO_NO,P.LINE_ID,p.customer_id,P.CUSTOMER_DESC,WH.OU_CODE ,WH.PL_NO ,
             WH.ITEM_NYF,WH.ITEM_CODE,wh.item_desc,c.nyf_pr_selling ,nvl(c.uom,'KG'),
             S1.SALE_ID,S1.SALE_NAME,s1.TEAM_NAME,b.buyyer """

  df = pd.read_sql_query(sql, conn)

  df.fillna("", inplace=True)

  df.to_excel(
      r'C:\QVD_DATA\NAYLIT\ISSUE_FG.xlsx', index=False)
  conn.close()
  printttime(r'C:\QVD_DATA\NAYLIT\ISSUE_FG.xlsx')

class CLS_RM_REC_NAY(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    RM_REC_NAY()

def RM_REC_NAY():

  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
 
  sql = """ SELECT w.receive_Date, w.rec_type, w.item_code,
               (SELECT ITEM_DESC FROM SF5.FMIT_ITEM@SALESF5.WORLD SF5 WHERE SF5.ITEM_CODE=W.ITEM_CODE) ITEM_DESC,
              i.ITEM_CATEGORY,I.item_structure,
              w.po_no,w.po_price,w.total_roll, w.total_weight, w.gf_cost_kg,
              w.gf_cost
               FROM (               
               SELECT   trunc(w.receive_date) receive_date,w.item_code,  
                        W.FABRIC_TYPE REC_TYPE,w.po_no,w.item_price_k po_price,
                        COUNT(w.FABRIC_ID) total_roll,
                        SUM(FABRIC_WEIGHT) total_weight,     
                        SUM(w.fabric_weight) actual_rm_weight,
                        ROUND(SUM(w.gf_cost_kg*w.fabric_weight)/SUM(w.fabric_weight),6) gf_cost_kg,
                        ROUND(SUM(w.gf_cost_kg*w.fabric_weight),6)  gf_cost,
                        ROUND((SUM(FABRIC_WEIGHT) /0.985),6) STD_RM_WEIGHT
               FROM FCCS_bu2.fmit_gf_warehouse@SALESF5.WORLD w
               WHERE  w.receive_Date>= TO_DATE('01/01/2019','dd/mm/yyyy')+0.00000 
               GROUP BY trunc(w.receive_date), w.item_code, w.po_no,w.item_price_k,
               W.FABRIC_TYPE) w,fccs_bu2.FA_MASTER_GREIGE_ITEM_NAYLIT@SALESF5.WORLD I
               WHERE w.item_code=i.item_code(+)
        ORDER BY 1,2 """

  df = pd.read_sql_query(sql, conn)

  df.fillna("", inplace=True)

  df.to_excel(
      r'C:\QVD_DATA\NAYLIT\RM_REC_NAY.xlsx', index=False)
  conn.close()
  printttime(r'C:\QVD_DATA\NAYLIT\RM_REC_NAY.xlsx')

class CLS_RM_ISS_NAY(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    RM_ISS_NAY()

def RM_ISS_NAY():

  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
 
  sql = """ SELECT w.issue_date, w.pk_type,w.schedule_id, w.po_no, w.so_no,
        w.item_code,w.total_roll, w.total_net_weight, w.gf_cost, w.gf_cost_kg, 
        i.ITEM_CATEGORY,I.item_structure,w.customer_desc customer_name          
        FROM
        (                      
          SELECT  trunc(W.TRANSFER_DATE) ISSUE_DATE,PK_TYPE ,
                  H.SCHEDULE_ID,h.PO_NO,h.SO_NO,NVL (h.item_code, h.so_item_code) ITEM_CODE,
                  COUNT(D.FABRIC_ID) TOTAL_ROLL,
                  SUM(D.FABRIC_WEIGHT) TOTAL_NET_WEIGHT,
                  ROUND(SUM(h.avg_COST*D.FABRIC_WEIGHT),6) GF_COST,                                           
                  ROUND(SUM(h.avg_COST*D.FABRIC_WEIGHT)/SUM(D.FABRIC_WEIGHT),6) GF_COST_KG,
                  h.customer_desc
                  FROM  FCCS_bu2.FMIT_PK_HEADER@SALESF5.WORLD H,FCCS_bu2.FMIT_PK_DETAIL@SALESF5.WORLD D ,
                        FCCS_bu2.FMIT_GF_WAREHOUSE@SALESF5.WORLD W
                  WHERE H.PACKING_NO= D.PACKING_NO
                  AND D.PO_NO=W.PO_NO 
                  AND D.BILL_NO=W.BILL_NO
                  AND D.FABRIC_ID=W.FABRIC_ID
                  AND D.FABRIC_WEIGHT > 0
                  AND W.TRANSFER_DATE >= TO_DATE('01/01/2019','dd/mm/yyyy')+0.00000
                  AND H.PL_STATUS<>'CANCEL'
              GROUP BY trunc(W.TRANSFER_DATE) ,H.SCHEDULE_ID,h.PO_NO, h.SO_no ,PK_TYPE, 
                       NVL (h.item_code, h.so_item_code),h.customer_desc
          ) w ,fccs_bu2.FA_MASTER_GREIGE_ITEM_NAYLIT@SALESF5.WORLD I
          WHERE w.item_code=i.item_code(+)
          ORDER  BY  1,2 """

  df = pd.read_sql_query(sql, conn)

  df.fillna("", inplace=True)

  df.to_excel(
      r'C:\QVD_DATA\NAYLIT\RM_ISS_NAY.xlsx', index=False)
  conn.close()
  printttime(r'C:\QVD_DATA\NAYLIT\RM_ISS_NAY.xlsx')

class CLS_RM_MOVE_NAY(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    RM_MOVE_NAY()

def RM_MOVE_NAY():

  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
 
  sql = """ SELECT  ITEM_CODE,item_category,item_structure,Transaction_date,
           sum(rec_qty) rec_weigth,sum(issue_qty) iss_weight
from (
select  w.ITEM_CODE,i.item_category,I.item_structure,TRUNC(W.RECEIVE_DATE) Transaction_date,
        nvl(w.fabric_weight,0) Rec_QTY, 0 issue_qty
FROM  FCCS_bu2.FMIT_GF_WAREHOUSE@SALESF5.WORLD W,
      fccs_bu2.FA_MASTER_GREIGE_ITEM_NAYLIT@SALESF5.WORLD I
WHERE W.ITEM_CODE=I.ITEM_CODE(+)
AND W.RECEIVE_DATE <=trunc(sysdate)+0.99999
union all
select  w.ITEM_CODE,i.item_category,I.item_structure,TRUNC(W.TRANSFER_DATE) Transaction_date,
        0 Rec_QTY,nvl(w.fabric_weight,0) issue_qty
FROM  FCCS_bu2.FMIT_GF_WAREHOUSE@SALESF5.WORLD W,
      fccs_bu2.FA_MASTER_GREIGE_ITEM_NAYLIT@SALESF5.WORLD I
      ,FCCS_bu2.FMIT_PK_HEADER@SALESF5.WORLD P
WHERE W.ITEM_CODE=I.ITEM_CODE(+)
and w.packing_no=p.packing_no
and W.TRANSFER_DATE is not null
AND W.TRANSFER_DATE <=trunc(sysdate)+0.99999
)
GROUP BY ITEM_CODE,item_category,item_structure,Transaction_date """

  df = pd.read_sql_query(sql, conn)

  df.fillna("", inplace=True)

  df.to_excel(
      r'C:\QVD_DATA\NAYLIT\RM_MOVE_NAY.xlsx', index=False)
  conn.close()
  printttime(r'C:\QVD_DATA\NAYLIT\RM_MOVE_NAY.xlsx')


class CLS_FG_MOVE_NAY(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    FG_MOVE_NAY()

def FG_MOVE_NAY():

  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
 
  sql = """ select TRANSCATION_DATE,item_code,get_item_desc_f(item_code) ITEM_DESC,color_id,
       get_color_desc(color_id) color_desc,
       sum(r_roll) rec_roll,sum(r_qty) rec_qty,sum(r_yard) rec_yard,
       sum(i_roll) iss_roll,sum(i_qty) iss_qty,sum(i_yard) iss_yard
from (
    SELECT TRUNC(WH.RECEIVE_DATE)  TRANSCATION_DATE,
            wh.item_code,wh.color_id,
            count(wh.fabric_id) R_ROLL,
            SUM(NVL( WH.fabric_WEIGHT,0))  R_QTY,
            SUM(NVL( WH.yard,0))  R_yard,
            0 i_roll,0 i_qty,0 i_yard
     FROM fccs_bu2.dfwh_warehouse@salesf5.world WH
     WHERE WH.RECEIVE_DATE <=  trunc(sysdate)+0.99999  
     GROUP BY  TRUNC(WH.RECEIVE_DATE),WH.ITEM_CODE,wh.color_id
       union all
     SELECT trunc(decode(wh.fabric_status,'RETURN',wh.RETURN_DATE,wh.SHIPPING_DATE))  TRANSCATION_DATE,
            wh.item_code,wh.color_id,
            0 r_roll,0 r_qty,0 r_yard,
            count(wh.fabric_id) i_ROLL,
            SUM(NVL( WH.fabric_WEIGHT,0)) i_QTY,
            SUM(NVL( WH.yard,0)) i_yard
     FROM fccs_bu2.dfwh_warehouse@salesf5.world WH
     WHERE substr(wh.fabric_status,1,5)<>'STOCK'
     and trunc(decode(wh.fabric_status,'RETURN',wh.RETURN_DATE,wh.SHIPPING_DATE)) <=  trunc(sysdate)+0.99999  
      GROUP BY  trunc(decode(wh.fabric_status,'RETURN',wh.RETURN_DATE,wh.SHIPPING_DATE)),WH.ITEM_CODE,wh.color_id
     )
GROUP BY TRANSCATION_DATE,item_code,color_id """

  df = pd.read_sql_query(sql, conn)

  df.fillna("", inplace=True)

  df.to_excel(
      r'C:\QVD_DATA\NAYLIT\FG_MOVE_NAY.xlsx', index=False)
  conn.close()
  printttime(r'C:\QVD_DATA\NAYLIT\FG_MOVE_NAY.xlsx')

############################################
class CLS_FG_MOVE_OPEN_NAY(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    FG_MOVE_OPEN_NAY()

def FG_MOVE_OPEN_NAY():

  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
 
  sql = """  select * from NYIS.V_NYL_FG_MOVEMENT """

  df = pd.read_sql_query(sql, conn)

  df.fillna("", inplace=True)

  df.to_excel(
      r'C:\QVD_DATA\NAYLIT\FG_MOVE_OPEN_NAY.xlsx', index=False)
  conn.close()
  printttime(r'C:\QVD_DATA\NAYLIT\FG_MOVE_OPEN_NAY.xlsx')

############################################################

############################################################
class CLS_RM_MOVE_OPEN_NAY(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    RM_MOVE_OPEN_NAY()

def RM_MOVE_OPEN_NAY():

  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
 
  sql = """  select * from NYIS.V_NYL_RM_MOVEMENT """

  df = pd.read_sql_query(sql, conn)

  df.fillna("", inplace=True)

  df.to_excel(
      r'C:\QVD_DATA\NAYLIT\RM_MOVE_OPEN_NAY.xlsx', index=False)
  conn.close()
  printttime(r'C:\QVD_DATA\NAYLIT\RM_MOVE_OPEN_NAY.xlsx')
  
############################################################

printttime('Start')

threads = []

thread3 = CLS_PO_RM_TRACKING();thread3.start();threads.append(thread3)
thread6 = CLS_Inventory_FG_Ageing_Onhand();thread6.start();threads.append(thread6)
thread7 = CLS_FG_Issued();thread7.start();threads.append(thread7)
thread10 = CLS_FG_Receive();thread10.start();threads.append(thread10)
thread1 = CLS_NAYLIT_ORDER();thread1.start();threads.append(thread1)
thread5 = CLS_Wip_Batch_Dye_Prod();thread5.start();threads.append(thread5)
thread4 = CLS_Prod_Time_Att_Process();thread4.start();threads.append(thread4)
thread2 = CLS_NAYLIT_INVOICE();thread2.start();threads.append(thread2)


thread8 = CLS_OmFollowUpByProcessClosed();thread8.start();threads.append(thread8)
thread9 = CLS_OmFollowUpByProcessWIP();thread9.start();threads.append(thread9)


thread11 = CLS_Wip_BT_detail();thread11.start();threads.append(thread11)
thread12 = CLS_schedule_data();thread12.start();threads.append(thread12)

thread13 = CLS_REC_FG();thread13.start();threads.append(thread13)
thread14 = CLS_ISSUE_FG();thread14.start();threads.append(thread14)
thread15 = CLS_RM_REC_NAY();thread15.start();threads.append(thread15)
thread16 = CLS_RM_ISS_NAY();thread16.start();threads.append(thread16)
thread17 = CLS_RM_MOVE_NAY();thread17.start();threads.append(thread17)
thread18 = CLS_FG_MOVE_NAY();thread18.start();threads.append(thread18)
thread19 = CLS_FG_MOVE_OPEN_NAY();thread19.start();threads.append(thread19)
thread20 = CLS_RM_MOVE_OPEN_NAY();thread20.start();threads.append(thread20)

for t in threads:
    t.join()
print (allTxt)
sendLine(allTxt)



