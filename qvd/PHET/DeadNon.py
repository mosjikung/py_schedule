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
  sql =""" select RECEIVE_DATE RECEIVE_WH_DATE,
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
class CLS_DATA_SO_STD_COST(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_SO_STD_COST()


def DATA_SO_STD_COST():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_SO_STD_COST")
  print ("Start CLS DATA_SO_STD_COST")
  sql ="""  select  aa.so_no, aa.SO_NO_DATE, aa.RDD_date,aa.RDD_WEEK,
                    aa.RDD_MONTH,
                    aa.RDD_YEAR,
                    aa.EDD_WEEK,
                    aa.EDD_YEAR,
                    aa.EDD_WEEK2,
                    aa.EDD_YEAR2,                     
                    nvl(aa.EDD_WEEK2,EDD_WEEK) FINAL_WEEK,
                    nvl(aa.EDD_YEAR2,EDD_YEAR) FINAL_YEAR,
                    aa.customer_name,
                    aa.Buyyer, 
                    aa.GROUP_Team_name,
                    aa.Team_name,
                    aa.sale_name,
                    aa.line_id,
                    aa.DEV_FABRIC_GRP FABRIC_GROUP,
                    decode(aa.item_type,'A0','ผ้าดิบ','B0','ผ้าดิบ','ผ้าสี') item_type,
                    aa.item_code,
                    aa.color_code, 
                    aa.fob_code , aa.NYF_PR_SELLING Price,
                    aa.UOM,
                    aa.ORDERED_QUANTITY,
                    aa.ORDER_QTY_KG,
                    aa.SALE_AMOUNT,
                    aa.creme_cost,
                    decode(item_type,'A0',0,'B0',0,aa.yarn_cost) yarn_cost,
                    decode(item_type,'A0',0,'B0',0,aa.sun_cost) sun_cost,
                    decode(item_type,'A0',0,'B0',0,aa.print_cost) print_cost,      
                    decode(item_type,'A0',0,'B0',0,aa.STD_LOSS_DYE) STD_LOSS_DYE,
                    decode(item_type,'A0',0,'B0',0,aa.ploss_qn_cut) std_loss_so,
                    aa.RM_COST,
                    (GET_LOSS_YARN_COST(aa.ORDER_QTY_KG,decode(item_type,'A0',0,'B0',0,aa.STD_LOSS_DYE) , decode(item_type,'A0',0,'B0',0,aa.ploss_qn_cut), aa.RM_COST)-nvl(aa.creme_cost,0)) LOSS_YARN_COST,
                    (decode(item_type,'A0',0,'B0',0,aa.yarn_cost)+ aa.creme_cost+ decode(item_type,'A0',0,'B0',0,aa.sun_cost)+ decode(item_type,'A0',0,'B0',0,aa.print_cost)+ (GET_LOSS_YARN_COST(aa.ORDER_QTY_KG, decode(item_type,'A0',0,'B0',0,aa.STD_LOSS_DYE), decode(item_type,'A0',0,'B0',0,aa.ploss_qn_cut), aa.RM_COST)-nvl(aa.creme_cost,0))) TOTAL_COST  ,
                    (aa.SALE_AMOUNT -  (decode(item_type,'A0',0,'B0',0,aa.yarn_cost)+ aa.creme_cost+ decode(item_type,'A0',0,'B0',0,aa.sun_cost)+ decode(item_type,'A0',0,'B0',0,aa.print_cost)+(GET_LOSS_YARN_COST(aa.ORDER_QTY_KG, decode(item_type,'A0',0,'B0',0,aa.STD_LOSS_DYE), decode(item_type,'A0',0,'B0',0,aa.ploss_qn_cut), aa.RM_COST)-nvl(aa.creme_cost,0)))) cm_amount,
                    (case when nvl(aa.SALE_AMOUNT,0) - (decode(item_type,'A0',0,'B0',0,aa.yarn_cost)+ aa.creme_cost+ 
                    decode(item_type,'A0',0,'B0',0,aa.sun_cost)+ decode(item_type,'A0',0,'B0',0,aa.print_cost)+ 
                    (GET_LOSS_YARN_COST(aa.ORDER_QTY_KG, decode(item_type,'A0',0,'B0',0,aa.STD_LOSS_DYE), 
                    decode(item_type,'A0',0,'B0',0,aa.ploss_qn_cut), aa.RM_COST)-nvl(aa.creme_cost,0))) > 0 and nvl(aa.ORDER_QTY_KG,0)>0 then
                    round(((aa.SALE_AMOUNT -  (decode(item_type,'A0',0,'B0',0,aa.yarn_cost)+ aa.creme_cost+ decode(item_type,'A0',0,'B0',0,aa.sun_cost)+ 
                    decode(item_type,'A0',0,'B0',0,aa.print_cost)+ (GET_LOSS_YARN_COST(aa.ORDER_QTY_KG, decode(item_type,'A0',0,'B0',0,aa.STD_LOSS_DYE), 
                    decode(item_type,'A0',0,'B0',0,aa.ploss_qn_cut), aa.RM_COST)-nvl(aa.creme_cost,0))))/aa.ORDER_QTY_KG),2) 
                    else 0 end) cm_per_kg,
                    (case when nvl(aa.SALE_AMOUNT,0) > 0 and 
                    nvl(aa.SALE_AMOUNT,0) - (decode(item_type,'A0',0,'B0',0,aa.yarn_cost)+ aa.creme_cost+ 
                    decode(item_type,'A0',0,'B0',0,aa.sun_cost)+ decode(item_type,'A0',0,'B0',0,aa.print_cost)+ 
                    (GET_LOSS_YARN_COST(aa.ORDER_QTY_KG, decode(item_type,'A0',0,'B0',0,aa.STD_LOSS_DYE), 
                    decode(item_type,'A0',0,'B0',0,aa.ploss_qn_cut), aa.RM_COST)-nvl(aa.creme_cost,0))) > 0 then  
                    round((((aa.SALE_AMOUNT -  (decode(item_type,'A0',0,'B0',0,aa.yarn_cost)+ aa.creme_cost+ 
                    decode(item_type,'A0',0,'B0',0,aa.sun_cost)+ decode(item_type,'A0',0,'B0',0,aa.print_cost)+ 
                    (GET_LOSS_YARN_COST(aa.ORDER_QTY_KG, decode(item_type,'A0',0,'B0',0,aa.STD_LOSS_DYE), 
                    decode(item_type,'A0',0,'B0',0,aa.ploss_qn_cut), aa.RM_COST)-nvl(aa.creme_cost,0)))) / aa.SALE_AMOUNT)*100),2) 
                    else 0 end) cm_percent,
                    aa.so_qn,aa.sc_price,aa.sc_rmcost
              from(                          
                   select d.so_no, trunc(d.SO_NO_DATE) SO_NO_DATE , d.REQUESTED_DATE RDD_date,
                          to_char(d.REQUESTED_DATE,'YYYYWW') RDD_WEEK,to_char(d.REQUESTED_DATE,'MM') RDD_MONTH,
                          to_char(d.REQUESTED_DATE,'IYYY') RDD_YEAR,nvl(lpad(d.CUSTOMER_FG,2,'0'),'01') EDD_WEEK,
                          nvl(d.CUSTOMER_YEAR,to_char(trunc(sysdate),'IYYY')) EDD_YEAR,lpad(s.EDD_WEEK2,2,'0') EDD_WEEK2,
                          s.EDD_YEAR2,d.customer_id,c.customer_name customer_name,d.Buyyer,sl.Team_name Team_name,
                          sl.SALES_SEGMENT  GROUP_Team_name,d.sale_id,sl.sale_name sale_name,l.item_code,
                          l.color_code, l.OE_LINE_SHADE,d.fob_code ,(l.NYF_PR_SELLING*nvl(d.CONVERSION_RATE,1)) NYF_PR_SELLING, 
                          l.ORDERED_QUANTITY,(nvl((l.NYF_PR_SELLING*nvl(d.CONVERSION_RATE,1)),0)* l.ORDERED_QUANTITY) SALE_AMOUNT,
                          (case when instr(d.fob_code,'ผ้าพร้อมพิมพ์')> 0 then 90  else 0 end) std_cost_print,
                          nvl(GET_PLoss_qn_opt (d.so_no,s.order_number),0) ploss_qn_cut,  
                          d.so_qn,l.line_id,
                          round(decode(substr(l.uom,1,1),'Y', sf5.get_ordered_kgs(l.so_no,l.line_id,l.tubular_type,l.ORDERED_QUANTITY,l.item_code),
                               'S',(l.ORDERED_QUANTITY/4),'D',(l.ORDERED_QUANTITY/3),
                               'P',DECODE(NVL(B.WEI_PER_SET,0),0,(l.ORDERED_QUANTITY/12)/3,(L.ORDERED_QUANTITY*B.WEI_PER_SET)/1000) 
                               , l.ORDERED_QUANTITY),2) ORDER_QTY_KG,
                          (round(decode(substr(l.uom,1,1),'Y',sf5.get_ordered_kgs(l.so_no,l.line_id,l.tubular_type,l.ORDERED_QUANTITY,l.item_code),
                               'S',(l.ORDERED_QUANTITY/4),'D',(l.ORDERED_QUANTITY/3),
                               'P',DECODE(NVL(B.WEI_PER_SET,0),0,(l.ORDERED_QUANTITY/12)/3,(L.ORDERED_QUANTITY*B.WEI_PER_SET)/1000)
                              , l.ORDERED_QUANTITY),2)) * B.STD_MC_ENERGY yarn_cost,
                          (round(decode(substr(l.uom,1,1),'Y',sf5.get_ordered_kgs(l.so_no,l.line_id,l.tubular_type,l.ORDERED_QUANTITY,l.item_code),
                              'S',(l.ORDERED_QUANTITY/4),'D',(l.ORDERED_QUANTITY/3),
                              'P',DECODE(NVL(B.WEI_PER_SET,0),0,(l.ORDERED_QUANTITY/12)/3,(L.ORDERED_QUANTITY*B.WEI_PER_SET)/1000)
                             ,l.ORDERED_QUANTITY),2)) * B.RM_COST creme_cost,
                          (round(decode(substr(l.uom,1,1),'Y',sf5.get_ordered_kgs(l.so_no,l.line_id,l.tubular_type,l.ORDERED_QUANTITY,l.item_code),
                              'S',(l.ORDERED_QUANTITY/4),'D',(l.ORDERED_QUANTITY/3),
                              'P',DECODE(NVL(B.WEI_PER_SET,0),0,(l.ORDERED_QUANTITY/12)/3,(L.ORDERED_QUANTITY*B.WEI_PER_SET)/1000)
                             , l.ORDERED_QUANTITY),2)) * B.STD_DYESTFF sun_cost,
                          (round(decode(substr(l.uom,1,1),'Y',sf5.get_ordered_kgs(l.so_no,l.line_id,l.tubular_type,l.ORDERED_QUANTITY,l.item_code),
                             'S',(l.ORDERED_QUANTITY/4),'D',(l.ORDERED_QUANTITY/3),
                             'P',DECODE(NVL(B.WEI_PER_SET,0),0,(l.ORDERED_QUANTITY/12)/3,(L.ORDERED_QUANTITY*B.WEI_PER_SET)/1000)
                            , l.ORDERED_QUANTITY),2))  * (case when instr(d.fob_code,'ผ้าพร้อมพิมพ์')> 0 then 90  else 0 end) print_cost,   
                          B.STD_LOSS_DYE, B.RM_COST, l.uom,b.DEV_FABRIC_GRP, substr(l.item_code,length(l.item_code)-1,2) item_type
                          ,DECODE (SUBSTR (l.item_code, 1, 1),  'C',0,
                          nvl(fnd_GAP_SC ('PRICE',d.so_qn,substr(l.item_code,1,length(l.item_code)-2)),0)) sc_price,
                          DECODE (SUBSTR (l.item_code, 1, 1),  'C',0,
                          nvl(fnd_GAP_SC ('COST',d.so_qn,substr(l.item_code,1,length(l.item_code)-2)),0)) sc_rmcost,
                          DECODE (SUBSTR (l.item_code, 1, 1),  'C',0,
                          nvl(fnd_GAP_SC ('GAP',d.so_qn,substr(l.item_code,1,length(l.item_code)-2)),0)) sc_gap
                 from sf5.smit_so_header d,sf5.smit_so_line l,sf5.DUMMY_SO_HEADERS s, demo.v_item_dev_std_cost@REPLICA1.WORLD  B,
                      sf5.dfora_sale sl, sf5.dfora_customer c
                 where d.so_no=l.so_no
                      and d.so_no=s.ORA_ORDER_NUMBER(+)
                      and l.ORDERED_QUANTITY <> 0  
                      and substr(SO_STATUS,1,2) <> '00' and d.customer_id=c.nyf_cus_id and sl.sale_id=d.sale_id
                      and length(d.so_no) = 10
                      and to_char(d.so_no_date,'YYYY') >= '2019'
                      and l.item_code = b.ORA_ITEM_CODE
                 order by d.so_no,l.line_id) AA  """

  # _filename = r"C:\QVD_DATA\PRO_NYK\DATA_SO_STD_COST.xlsx"

  #Edit 2021-09-22 request by SPO
  _filename = r"C:\QVD_DATA\COST_SPO\DATA_SO_STD_COST.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_SO_STD_COST")
  sendLine("COMPLETE CLS_DATA_SO_STD_COST")

#############################################

###########################################
class CLS_DATA_FG_FABRIC_COST(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_FG_FABRIC_COST()


def DATA_FG_FABRIC_COST():
  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="DEMO", password="DEMO", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_FG_FABRIC_COST")
  sql =""" select P.PO_NO, P.SO_NO,P. LINE_ID, P.FABRIC_TYPE, P.ITEM_CODE, 
            (select l.item_code from demo.dfit_so_line l where l.so_no=p.so_no and l.line_id=p.line_id) item_shade,
            (select l.color_code from demo.dfit_so_line l where l.so_no=p.so_no and l.line_id=p.line_id) color_code,
             P.STK_SO_NO, P.STK_LINE_ID, P.UOM, P.INV_NO, P.SELLING_PRICE, 
             P.SELLING_UOM, I.SALE_ID, S.SALE_NAME, C.CUSTOMER_ID, C.CUSTOMER_NAME, P.QTY, 
             decode( P.SELLING_UOM,'PCS',demo.Master_Convert_Doz_Pcs(P.UNIT_QTY),p.UNIT_QTY) UNIT_QTY, 
             P.RM_COST_AMT, P.RM_COST_UOM, P.GF_COST_AMT, P.GF_COST_UOM, P.OVERH_KNIT_AMT, P.OVERH_KNIT_UOM, 
             DECODE(P.TYPE_COST,'OE',P.OE_OVERH_AMT,P.OVERH_COST_AMT) OVERH_COST_AMT,
             DECODE(P.TYPE_COST,'OE',P.OE_OVERH_COST,P.OVERH_COST_UOM) OVERH_COST_UOM,
             DECODE(P.TYPE_COST,'OE',P.OE_ENERGY_AMT,P.ENERGY_COST_AMT) ENERGY_COST_AMT,
             DECODE(P.TYPE_COST,'OE',P.OE_ENERGY_COST,P.ENERGY_COST_UOM) ENERGY_COST_UOM,
             DECODE(P.TYPE_COST,'OE',P.OE_DYESTAFF_AMT,P.DYESTAFF_COST_AMT) DYESTAFF_COST_AMT,
             DECODE(P.TYPE_COST,'OE',P.OE_DYESTAFF_COST,P.DYESTAFF_COST_UOM) DYESTAFF_COST_UOM,
             P.PRN_COST_AMT, P.PRN_COST_UOM, 
             P.OE_DYE_AMT, P.OE_DYE_UOM, P.BT_QTY,TRUNC(I.INV_DATE) INV_DATE,S.TEAM_NAME TEAM_SALE  ,P.TYPE_COST,
             ROUND(NVL(A.CONVERSION_RATE,1)*P.SELLING_PRICE*(decode( P.SELLING_UOM,'PCS',demo.Master_Convert_Doz_Pcs(P.UNIT_QTY),p.UNIT_QTY)) ,2) SALE_AMOUNT,A.BUYYER
            from  DEMO.V_PO_SO_ALL_FG_F P,DEMO.NYF_FG_M_INVOICE I,DEMO.DFORA_SALE S,DEMO.DFORA_CUSTOMER C,
                  DEMO.SMIT_SO_HEADER A
            where p.inv_no=i.inv_no AND P.SO_NO=A.SO_NO
                  AND I.SALE_ID=S.SALE_ID(+)
                  AND I.NYK_CUS_ID=C.NYF_CUS_ID(+) """

  # _filename = r"C:\QVD_DATA\PRO_NYK\DATA_FG_FABRIC_COST.xlsx"

  #Edit 2021-09-22 request by SPO
  _filename = r"C:\QVD_DATA\COST_SPO\DATA_FG_FABRIC_COST.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_FG_FABRIC_COST")
  sendLine("COMPLETE CLS_DATA_FG_FABRIC_COST")

#############################################

###########################################
class CLS_DATA_DUMMY_APP_TimeStep(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_DUMMY_APP_TimeStep()


def DATA_DUMMY_APP_TimeStep():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_FG_FABRIC_COST")
  sql =""" SELECT  ORDER_NUMBER RS_NO, SO_NO, FLOW_STATUS_CODE,ORDERED_DATE, DATA_REC_TYPE, FOB_POINT_CODE, ITEM_TYPE, 
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
                           STOCK_APPROVED,
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
                           KNIT_YEAR_ST, KNIT_WEEK_ST, KNIT_YEAR, KNIT_WEEK,
                           OM_AUTO_MAIL,
                           Get_Time_Minutes(OM_AUTO_MAIL,NVL(NVL(KNIT_FG_APPROVED,MAX_STAGE_2),OM_AUTO_MAIL)) AUTOMAIL_USED_TIME,
                           FLOOR( Get_Time_Minutes(OM_AUTO_MAIL,NVL(NVL(KNIT_FG_APPROVED,MAX_STAGE_2),OM_AUTO_MAIL))/60) AUTOMAIL_USED_HH,
                           MOD( Get_Time_Minutes(OM_AUTO_MAIL,NVL(NVL(KNIT_FG_APPROVED,MAX_STAGE_2),OM_AUTO_MAIL)),60) AUTOMAIL_USED_MM,  
                           OM_CONFIRM_DATE,
                           Get_Time_Minutes(OM_CONFIRM_DATE,NVL(NVL(OM_AUTO_MAIL,KNIT_FG_APPROVED),OM_CONFIRM_DATE)) OM_USED_TIME,
                           FLOOR(Get_Time_Minutes(OM_CONFIRM_DATE,NVL(NVL(OM_AUTO_MAIL,KNIT_FG_APPROVED),OM_CONFIRM_DATE))/60) OM_USED_HH,
                           MOD(Get_Time_Minutes(OM_CONFIRM_DATE,NVL(NVL(OM_AUTO_MAIL,KNIT_FG_APPROVED),OM_CONFIRM_DATE)),60) OM_USED_MM,  
                           GET_LAST_RS_STATUS(ORDER_NUMBER) CURENT_STEPS,
                           PURCHASE_YARN (ORDER_NUMBER) YARN_CONFIRM
                 FROM ( SELECT H.ORDER_NUMBER, H.ORA_ORDER_NUMBER SO_NO,H.FLOW_STATUS_CODE, H.ORDERED_DATE, H.DATA_REC_TYPE, 
                                        H.FOB_POINT_CODE, DECODE(H.ITEM_TYPE,'F','FABRIC','COLLAR') ITEM_TYPE,
                                        H.CUSTOMER_NUMBER,H.SALESREP_NUMBER SALE_ID,'TEAM_NAME', H.SALES_SEND_DATE,DUMMY_ITEM_TYPE ORDER_TYPE,
                                        (SELECT MAX(CONFIRM_DATE) FROM SF5.DUMMY_CONFIRM_ORDER C WHERE C.ORDER_NUMBER=H.ORDER_NUMBER
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
                                             H.KNIT_YEAR_ST, H.KNIT_WEEK_ST, H.KNIT_YEAR, H.KNIT_WEEK, H.OM_CONFIRM_DATE, H.OM_AUTO_MAIL
                          FROM SF5.DUMMY_SO_HEADERS H
                          WHERE FLOW_STATUS_CODE NOT IN ('CANCEL')
                         AND trunc(ORDERED_DATE) >= TO_DATE('01/01/2020','DD/MM/RRRR')
                      ) A
                   ORDER BY 1 """

  _filename = r"C:\QVDatacenter\SCM\FABRIC\DATA_DUMMY_APP_TimeStep.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_DUMMY_APP_TimeStep")
  sendLine("COMPLETE CLS_DATA_DUMMY_APP_TimeStep")

#############################################

###########################################
class CLS_DATA_STATUS_ORDER_NonSplit(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_STATUS_ORDER_NonSplit()


def DATA_STATUS_ORDER_NonSplit():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_FG_FABRIC_COST")           
  sql =""" select  a.*,
                   sf5.cal_Mps_dye_hr_new(a.ITEM_CODE,a.COLOR_CODE) Syd_Dye_HR,
                   sf5.CAL_KG_PER_HR(sf5.cal_Mps_dye_hr_new(a.ITEM_CODE,a.COLOR_CODE),a.COLOR_QTY) KG_HOUR
           from(SELECT C.CONFIRM_DATE,M.FG_YEAR,M.FG_WEEK,
                       NVL(SH.CUSTOMER_YEAR,M.FG_YEAR)  SO_FG_YEAR,
                       NVL(SH.CUSTOMER_FG,M.FG_WEEK)  SO_FG_WEEK,
                       TO_NUMBER(LTRIM(KP.KNIT_YEAR))  RESERVE_KNIT_YEAR, 
                       TO_NUMBER(LTRIM(KP.KNIT_WEEK))  RESERVE_KNIT_WEEK, 
                       NVL(KP.RESERVE_FG_YEAR,NVL(SH.CUSTOMER_YEAR,M.FG_YEAR))  RESERVE_FG_YEAR,
                       NVL(KP.RESERVE_FG_WEEK, NVL(SH.CUSTOMER_FG,M.FG_WEEK)) RESERVE_FG_WEEK,
                       KP.KNIT_QTY, KP.USED_KGS, KP.BAL_KGS,
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
                        M.SO_RESERVE,M.SO_TYPE,
                        decode(A.TUBULAR_TYPE,'1','อบกลม','2','อบผ่า',TUBULAR_TYPE) TUBULAR_TYPE,
                        NVL(SH.SO_STATUS,'99.Pending Order') SO_STATUS, 
                        NVL((SELECT  SUM(decode(UOM,'KG',(ORDERED_QUANTITY),
                        case when sf5.M_DEV_ITEM_YD_KG(A.ITEM_CODE,TUBULAR_TYPE) = 0 then ORDERED_QUANTITY else round(ORDERED_QUANTITY/sf5.M_DEV_ITEM_YD_KG(A.ITEM_CODE,TUBULAR_TYPE)) end
                       )) 
              FROM sf5.smit_so_line SO
              WHERE SO.so_no =M.ORA_ORDER_NUMBER and so.ITEM_CODE = A.ITEM_CODE and so.COLOR_CODE = A.COLOR_CODE),A.PORDER_QTY) ORDERED_KGS,
              GET_MINPOIN_DATE (M.ORDER_NUMBER, A.ITEM_CODE) MIN_PO_IN_DATE,
              GET_MAXPOIN_DATE (M.ORDER_NUMBER, A.ITEM_CODE) MAX_PO_IN_DATE
           FROM sf5.DUMMY_SO_HEADERS M  , sf5.DUMMY_CONFIRM_ORDER C, sf5.DFORA_SALE S,SF5.SMIT_SO_HEADER SH,
                (SELECT  L.ORDER_NUMBER, L.ORDERED_ITEM ITEM_CODE,
                   C.COMM_PLCOLOR COLOR_CODE ,C.COMM_DESC COLOR_DESC, 
                   C.ORDERED_KGS PORDER_QTY,L.TUBULAR_TYPE
           FROM sf5.DUMMY_SO_LINES L, sf5.DUMMY_COLOR_LINES C
           WHERE L.ORDER_NUMBER=C.ORDER_NUMBER  AND L.LINE_NUMBER=C.LINE_NUMBER            
           ) A ,
          (SELECT  ORDER_NUMBER, SO_NO, ITEM_CODE, KNIT_YEAR, KNIT_WEEK, KNIT_QTY, USED_KGS, NVL(KNIT_QTY, 0)-NVL(USED_KGS,0) BAL_KGS,
                              FG_YEARWEEK FG_WEEK,
                              CASE WHEN NVL(TO_NUMBER(SUBSTR(FG_RESERVE (TO_NUMBER(KNIT_YEAR)||LTRIM(TO_CHAR(KNIT_WEEK,'09')),FOB_POINT_CODE),1,4)),0)=0 THEN
                              TO_NUMBER (LTRIM(SUBSTR( FG_YEARWEEK,1,4))) ELSE
                              TO_NUMBER(SUBSTR(FG_RESERVE (TO_NUMBER(KNIT_YEAR)||LTRIM(TO_CHAR(KNIT_WEEK,'09')),FOB_POINT_CODE),1,4)) END  RESERVE_FG_YEAR,
                              CASE WHEN NVL(TO_NUMBER(SUBSTR(FG_RESERVE (TO_NUMBER(KNIT_YEAR)||LTRIM(TO_CHAR(KNIT_WEEK,'09')),FOB_POINT_CODE),5,2)),0)=0 THEN
                              TO_NUMBER (LTRIM(SUBSTR( FG_YEARWEEK,5,2))) ELSE
                              TO_NUMBER(SUBSTR(FG_RESERVE (TO_NUMBER(KNIT_YEAR)||LTRIM(TO_CHAR(KNIT_WEEK,'09')),FOB_POINT_CODE),5,2)) END RESERVE_FG_WEEK
            FROM SF5.DUMMY_MAIN_RESERVE_D) KP
           WHERE C.ORDER_NUMBER=M.ORDER_NUMBER
             AND M.ORDER_NUMBER=A.ORDER_NUMBER
             AND M.SALESREP_NUMBER = S.SALE_ID
             AND M.ORDER_NUMBER=KP.ORDER_NUMBER
             AND SUBSTR(A.ITEM_CODE,1,LENGTH(A.ITEM_CODE)-2)=SUBSTR(KP.ITEM_CODE,1,LENGTH(A.ITEM_CODE)-2)
             AND M.ORA_ORDER_NUMBER=SH.SO_NO(+)
             AND C.CONFIRM_TYPE='KNIT-FG CONFIRM DUMMY SO'
             AND M.SO_TYPE IN ('SO Dummy','SO Program')
             AND M.FLOW_STATUS_CODE <> 'CANCEL'
             and nvl(M.CLOSE_FLAG,'N') <> 'C'
             AND DATA_REC_TYPE = 'ORDERED'
           ORDER BY M.ORDER_NUMBER) A
          WHERE spit_batch_status ='NON SPIT'   
            and A.RESERVE_FG_YEAR >= 2020
     UNION
        select  a.*,
                sf5.cal_Mps_dye_hr_new(a.ITEM_CODE,a.COLOR_CODE) Syd_Dye_HR,
                sf5.CAL_KG_PER_HR(sf5.cal_Mps_dye_hr_new(a.ITEM_CODE,a.COLOR_CODE),a.COLOR_QTY) KG_HOUR
        from(SELECT C.CONFIRM_DATE,M.FG_YEAR,M.FG_WEEK,
             NVL(SH.CUSTOMER_YEAR, M.FG_YEAR)  SO_FG_YEAR,
             NVL(SH.CUSTOMER_FG, M.FG_WEEK)  SO_FG_WEEK,
             0  RESERVE_KNIT_YEAR,  0  RESERVE_KNIT_WEEK, 
             NVL(SH.CUSTOMER_YEAR, M.FG_YEAR ) RESERVE_FG_YEAR,  
             NVL(SH.CUSTOMER_FG, M.FG_WEEK ) RESERVE_FG_WEEK, 
             0  KNIT_QTY,  0 USED_KGS, 
            NVL(  (SELECT  SUM(decode(UOM,'KG',(ORDERED_QUANTITY),
                  case when sf5.M_DEV_ITEM_YD_KG(A.ITEM_CODE,TUBULAR_TYPE) = 0 then ORDERED_QUANTITY else round(ORDERED_QUANTITY/sf5.M_DEV_ITEM_YD_KG(A.ITEM_CODE,TUBULAR_TYPE)) end
                   )) 
              FROM sf5.smit_so_line SO
              WHERE SO.so_no =M.ORA_ORDER_NUMBER and so.ITEM_CODE = A.ITEM_CODE and so.COLOR_CODE = A.COLOR_CODE) , A.PORDER_QTY ) BAL_KGS,
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
             M.SO_RESERVE,M.SO_TYPE,
             decode(A.TUBULAR_TYPE,'1','อบกลม','2','อบผ่า',TUBULAR_TYPE) TUBULAR_TYPE,
            NVL(SH.SO_STATUS,'99.Pending Order') SO_STATUS, 
           NVL(  NVL( (SELECT  SUM(decode(UOM,'KG',(ORDERED_QUANTITY),
                  case when sf5.M_DEV_ITEM_YD_KG(A.ITEM_CODE,TUBULAR_TYPE) = 0 then ORDERED_QUANTITY else round(ORDERED_QUANTITY/sf5.M_DEV_ITEM_YD_KG(A.ITEM_CODE,TUBULAR_TYPE)) end
                   )) 
              FROM sf5.smit_so_line SO
              WHERE SO.so_no =M.ORA_ORDER_NUMBER and so.ITEM_CODE = A.ITEM_CODE and so.COLOR_CODE = A.COLOR_CODE) , A.PORDER_QTY) ,A.PORDER_QTY) ORDERED_KGS,
              GET_MINPOIN_DATE (M.ORDER_NUMBER, A.ITEM_CODE) MIN_PO_IN_DATE,
              GET_MAXPOIN_DATE (M.ORDER_NUMBER, A.ITEM_CODE) MAX_PO_IN_DATE
        FROM sf5.DUMMY_SO_HEADERS M  , sf5.DUMMY_CONFIRM_ORDER C, sf5.DFORA_SALE S,SF5.SMIT_SO_HEADER SH,
        (SELECT  L.ORDER_NUMBER, L.ORDERED_ITEM ITEM_CODE,
                   C.COMM_PLCOLOR COLOR_CODE ,C.COMM_DESC COLOR_DESC, 
                   C.ORDERED_KGS PORDER_QTY,L.TUBULAR_TYPE
        FROM sf5.DUMMY_SO_LINES L, sf5.DUMMY_COLOR_LINES C
        WHERE L.ORDER_NUMBER=C.ORDER_NUMBER  AND L.LINE_NUMBER=C.LINE_NUMBER            
        ) A 
       WHERE C.ORDER_NUMBER=M.ORDER_NUMBER
       AND M.ORDER_NUMBER=A.ORDER_NUMBER
       AND M.SALESREP_NUMBER = S.SALE_ID
       AND M.ORA_ORDER_NUMBER=SH.SO_NO(+)
       AND C.CONFIRM_TYPE='KNIT-FG CONFIRM DUMMY SO'
       AND M.SO_TYPE NOT  IN ('SO Dummy','SO Program')
       AND M.FLOW_STATUS_CODE <> 'CANCEL'
       and nvl(M.CLOSE_FLAG,'N') <> 'C'
       AND DATA_REC_TYPE = 'ORDERED'
     ORDER BY M.ORDER_NUMBER )  A
     WHERE spit_batch_status ='NON SPIT'   
       and A.SO_FG_YEAR >= 2020
     ORDER BY  13,18,6,7"""

  _filename = r"C:\QVDatacenter\SCM\FABRIC\DATA_STATUS_ORDER_NonSplit.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_STATUS_ORDER_NonSplit")
  sendLine("COMPLETE CLS_DATA_STATUS_ORDER_NonSplit")

#############################################

###########################################
class CLS_DATA_GF_Received_SPO(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_GF_Received_SPO()


def DATA_GF_Received_SPO():
  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="DEMO", password="DEMO", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_GF_Received_SPO")
  sql =""" SELECT TRUNC(ENTRY_DATE) ENTRY_DATE, 
       VENDOR_ID, VENDOR_NAME,
       PO_NO,
       SO_NO,
       LINE_ID SO_LINE_ID,
       BILL_NO, 
       YARN_LOT,
       ITEM_CODE, 
       GET_ITEM_DESC(ITEM_CODE) ITEM_DESC, 
       TOTAL_ROLL,
       TOTAL_WEIGHT,
       TOTAL_DOZ,
       ITEM_PRICE_K PRICE, --ค่าจ้างย้อม 
       Rm_Cost_Kg,
       Gf_Cost_Kg,
       (GF_COST_Kg * TOTAL_WEIGHT) AMOUNT_COST,
       Nvl(Closed_Pkwh_Flag,'N') Closed_Pkwh_Flag ,
       ENTRY_BY RECEIVE_BY
  FROM FMIT_GF_PKWH 
  WHERE TRUNC(ENTRY_DATE) > to_date('01/01/2017','DD/MM/YYYY') """

  _filename = r"C:\QVD_DATA\PRO_NYK\DATA_GF_Received_SPO.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_GF_Received_SPO")
  sendLine("COMPLETE CLS_DATA_GF_Received_SPO")

#############################################

###########################################
class CLS_DATA_GF_ISSUE_SPO(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_GF_ISSUE_SPO()


def DATA_GF_ISSUE_SPO():
  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="DEMO", password="DEMO", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_GF_ISSUE_SPO")
  sql =""" SELECT TRUNC(ENTRY_DATE) ENTRY_DATE,
                  CUSTOMER_ID VENDOR_ID, CUSTOMER_DESC VENDOR_NAME,
                  PO_NO,
                  SO_NO,
                  SO_LINE_ID,
                  BILL_NO,
                  YARN_LOT,
                  ITEM_CODE,
                  GET_ITEM_DESC(ITEM_CODE) ITEM_DESC, 
                  TOTAL_ROLL,
                  TOTAL_WEIGHT,
                  TOTAL_DOZ,
                  TOTAL_GF_COST,
                  case when TOTAL_WEIGHT <> 0 then round((TOTAL_GF_COST/TOTAL_WEIGHT),2) else 0 end AVG_COST,
                  UPDATE_BY ISSUE_BY          
            FROM FMIT_PK_HEADER
            WHERE nvl(pl_status,'N')<>'CANCEL'  
              AND TRUNC(ENTRY_DATE) > to_date('01/01/2017','DD/MM/YYYY') """

  _filename = r"C:\QVD_DATA\PRO_NYK\DATA_GF_ISSUE_SPO.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_GF_ISSUE_SPO")
  sendLine("COMPLETE CLS_DATA_GF_ISSUE_SPO")

#############################################

###########################################
class CLS_DATA_COLLAR_Received_SPO(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_COLLAR_Received_SPO()


def DATA_COLLAR_Received_SPO():
  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="DEMO", password="DEMO", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  print("start CLS_DATA_COLLAR_Received_SPO")
  sendLine("START CLS_DATA_COLLAR_Received_SPO")
  sql =""" SELECT TRUNC(ENTRY_DATE) ENTRY_DATE, 
       VENDOR_ID, VENDOR_NAME,
       PO_NO,
       SO_NO,
       LINE_ID SO_LINE_ID,
       BILL_NO, 
       YARN_LOT,
       ITEM_CODE, 
       GET_ITEM_DESC(ITEM_CODE) ITEM_DESC, 
       TOTAL_ROLL,
       TOTAL_WEIGHT,
       TOTAL_DOZ,
       ITEM_PRICE_K PRICE, --ค่าจ้างย้อม 
       Rm_Cost_Kg,
       Gf_Cost_Kg,
       (GF_COST_Kg * TOTAL_WEIGHT) AMOUNT_COST,
       Nvl(Closed_Pkwh_Flag,'N') Closed_Pkwh_Flag ,
       ENTRY_BY RECEIVE_BY
  FROM FMIT_CV_PKWH 
  WHERE TRUNC(ENTRY_DATE) > to_date('01/01/2017','DD/MM/YYYY') """

  _filename = r"C:\QVD_DATA\PRO_NYK\DATA_COLLAR_Received_SPO.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_COLLAR_Received_SPO")
  sendLine("COMPLETE CLS_DATA_COLLAR_Received_SPO")

#############################################

###########################################
class CLS_DATA_COLLAR_ISSUE_SPO(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_COLLAR_ISSUE_SPO()


def DATA_COLLAR_ISSUE_SPO():
  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="DEMO", password="DEMO", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_COLLAR_ISSUE_SPO")
  sql =""" SELECT TRUNC(ENTRY_DATE) ENTRY_DATE,
                  CUSTOMER_ID VENDOR_ID, CUSTOMER_DESC VENDOR_NAME,
                  PO_NO,
                  SO_NO,
                  SO_LINE_ID,
                  BILL_NO,
                  YARN_LOT,
                  ITEM_CODE,
                  GET_ITEM_DESC(ITEM_CODE) ITEM_DESC, 
                  TOTAL_ROLL,
                  TOTAL_WEIGHT,
                  TOTAL_DOZ,
                  TOTAL_GF_COST,
                  case when TOTAL_WEIGHT <> 0 then round((TOTAL_GF_COST/TOTAL_WEIGHT),2) else 0 end AVG_COST,
                  UPDATE_BY ISSUE_BY          
            FROM FMIT_PKCV_HEADER
            WHERE nvl(pl_status,'N')<>'CANCEL'  
              AND TRUNC(ENTRY_DATE) > to_date('01/01/2017','DD/MM/YYYY') """

  _filename = r"C:\QVD_DATA\PRO_NYK\DATA_COLLAR_ISSUE_SPO.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_COLLAR_ISSUE_SPO")
  sendLine("COMPLETE CLS_DATA_COLLAR_ISSUE_SPO")

#############################################

###########################################
class CLS_DATA_GAPQN_KGHR(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_GAPQN_KGHR()


def DATA_GAPQN_KGHR():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_GAPQN_KGHR")

  sql =""" select Q.*
           from DFIV_QN_RED_C_QVD Q """

  # sql =""" select Q.ORDER_NO,Q.LINE_ID,Q.YARN_COST,Q.DYE_LOSS,Q.QN_FUNCTION,Q.QN_PRINT,Q.TEAM_NAME,Q.CUSTOMER_END,
  #          Q.CUSTOMER_ID,Q.CUSTOMER_NAME,Q.ITEM_CODE,Q.GROUP_METERIAL,Q.DYE_TYPE,Q.FABRIC_SHADE,Q.PRICE_TYPE,
  #          Q.ENTRY_DATE,Q.KNIT_COST,Q.STD_LOADDYE,Q.ACT_LOADDYE,
  #          Q.TARGET_PRICE_AVG,Q.RATIO_AVG,Q.STD_DC_AVG,Q.STD_ENERGY_L,Q.STD_DYEHR_L,Q.GAP_VC_L,Q.GAP_KGHR_L,
  #          Q.TARGET_PRICE_M,Q.RATIO_QUA_M,Q.STD_DC_M,Q.STD_ENERGY_M,Q.STD_DYEHR_M,Q.GAP_VC_M,Q.GAP_KGHR_M,
  #          Q.TARGET_PRICE_D,Q.RATIO_QUA_D,Q.STD_DC_D,Q.STD_ENERGY_D,Q.STD_DYEHR_D,Q.GAP_VC_D,Q.GAP_KGHR_D,
  #          Q.TARGET_PRICE_S,Q.RATIO_QUA_S,Q.STD_DC_S,Q.STD_ENERGY_S,Q.STD_DYEHR_S,Q.GAP_VC_S,Q.GAP_KGHR_S,
  #          Q.tARGET_PRICE_W,Q.RATIO_QUA_W,Q.STD_DC_W,Q.STD_ENERGY_W,Q.STD_DYEHR_W,Q.GAP_VC_W,Q.GAP_KGHR_W
  #          from DFIV_QN_RED_C_QVD Q """

  # _filename = r"C:\QVD_DATA\PRO_NYK\CLS_DATA_GAPQN_KGHR.xlsx"

  #Edit 2021-09-22 request by SPO
  _filename = r"C:\QVD_DATA\COST_SPO\CLS_DATA_GAPQN_KGHR.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_GAPQN_KGHR")
  sendLine("COMPLETE CLS_DATA_GAPQN_KGHR")

#############################################

###########################################
class CLS_DATA_ITEM_TO_GAPQN(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_ITEM_TO_GAPQN()


def DATA_ITEM_TO_GAPQN():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_ITEM_TO_GAPQN")
  sql ="""  select i.DEV_CODE,i.ITEM_SHADE,i.ITEM_DESC,i.FABRIC_TYPE,i.FABRIC_SHADE,i.UNIT_PRICE,i.ITEM_CATEGORY,i.ITEM_STRUCTURE,
                  i.DYE_COST,i.CM,i.CM_COST,i.TECH_COST,i.OVH_COST,i.DYE_LOST,i.KNIT_COST,i.YARN_COST,
                  i.OPTION_CM_PRICE,i.DEV_WIDTH,i.DEV_GM2,i.DEV_GM_PER_YD,--i.DEV_PERC_SHR_YD,i.DEV_PERC_SCH_GM2,
                  i.YARD_PER_KG,i.COST_PER_YARD,i.DEV_GF_YD_KG,i.PRICE_CAL_YARD
            from ITEM_QN_PRICE i
            where substr(unit_price,1,1)<>'C' 
                and i.item_shade like 'F%' """

  # _filename = r"C:\QVD_DATA\PRO_NYK\CLS_DATA_ITEM_TO_GAPQN.xlsx"

  #Edit 2021-09-22 request by SPO
  _filename = r"C:\QVD_DATA\COST_SPO\CLS_DATA_ITEM_TO_GAPQN.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_ITEM_TO_GAPQN")
  sendLine("COMPLETE CLS_DATA_ITEM_TO_GAPQN")


###########################################
class CLS_DATA_BATCH_PRODUCTION_COST(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_BATCH_PRODUCTION_COST()


def DATA_BATCH_PRODUCTION_COST():
  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="DEMO", password="DEMO", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  sendLine("START CLS_DATA_BATCH_PRODUCTION_COST")

  sql =""" SELECT  CUSTOME_PO,BUYER,MPS_YEAR,MPS_WEEK,OU_CODE,BATCH_NO,SO_NO,LINE_ID,CUSTOMER_ID,CUSTOMER_NAME,SALE_ID,SALE_NAME,
              COLOR_CODE,COLOR_DESC,COLOR_SHADE,ITEM_CODE,ITEM_DESC,ITEM_PROCESS,ITEM_DEVELOP,UNIT_QTY,TOTAL_ROLL,TOTAL_QTY,
              BT_FG_KG,BT_SC_KG,BT_LO_KG,BT_RE_KG,METERIAL_GROUP,ACT_ADC_AMT,ACT_SCC_AMT,ACT_IPC_AMT,ACT_SED_AMT,
              STD_SCF_AMT,ACT_OHC_AMT,TOTAL_BATCH_COST,BT_CLOSED_ACTIVE,BT_CLOSED_DATE,SCHEDULE_ID,OE_PRICE_SELL,
              STD_FG_COST,STD_DYE_HR,STD_FG_COST_AMT,SDC_COST,SCC_COST,SED_COST,SCF_COST,OHC_PRE_COST,
              OHC_DYE_COST,OHC_FIN_COST,OHC_INS_COST,OHC_SHIP_COST,IPC_STEP_COST,IPC_MC_COST,IPC_DEPRE_COST,
              ACT_CHEFIN_COST,FG_INACTIVE_KG,STD_KNIT_COST,GF_COST_KG,ACT_DYE_LOAD,STD_DYE_LOAD,STD_DEV_LOSS,
              QN_NO,QN_YARN_COST,QN_EXTR_PRINT,STD_COLAB_DYE_KG,STD_COLAB_CHEMI_KG,FORMULA_NO,STD_FORMULA_FIN_KG
            FROM V_BT_COST_ACC """

  _filename = r"C:\QVD_DATA\COST_SPO\DATA_BATCH_PRODUCTION_COST.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  conn.close()
  print("COMPLETE CLS_DATA_BATCH_PRODUCTION_COST")
  sendLine("COMPLETE CLS_DATA_BATCH_PRODUCTION_COST")

#############################################


threads = []
thread1 = CLS_DATA_NYK_DEADNON();thread1.start();threads.append(thread1)
thread2 = CLS_DATA_SO_STD_COST();thread2.start();threads.append(thread2)
thread3 = CLS_DATA_FG_FABRIC_COST();thread3.start();threads.append(thread3)
thread4 = CLS_DATA_DUMMY_APP_TimeStep();thread4.start();threads.append(thread4)
thread5 = CLS_DATA_STATUS_ORDER_NonSplit();thread5.start();threads.append(thread5)
thread6 = CLS_DATA_GF_Received_SPO();thread6.start();threads.append(thread6)
thread7 = CLS_DATA_GF_ISSUE_SPO();thread7.start();threads.append(thread7)
thread8 = CLS_DATA_COLLAR_Received_SPO();thread8.start();threads.append(thread8)
thread9 = CLS_DATA_COLLAR_ISSUE_SPO();thread9.start();threads.append(thread9)
thread10 = CLS_DATA_GAPQN_KGHR();thread10.start();threads.append(thread10)
thread11 = CLS_DATA_ITEM_TO_GAPQN();thread11.start();threads.append(thread11)

# cancel thread12 = CLS_DATA_BATCH_PRODUCTION_COST();thread12.start();threads.append(thread12)

for t in threads:
    t.join()
print ("COMPLETE")

