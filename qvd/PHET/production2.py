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
########
# Create 01/03/2022
# Request Move By Chonlada Suksamer
# SQL By 
# Create By Krisada.R
# Remark Move from PHET/production.py
########

class CLS_NYK_DATA_MINING(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    NYK_DATA_MINING()


def NYK_DATA_MINING():
  my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  args = ['NYK','SF5SPO001','NYK_DATA_MINING.xlsx']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print("start NYK_DATA_MINING.xlsx")
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
       ,trunc(FAB_DATE) FAB_DATE, nyis.DEDUCT_DATE_OTP(a.dye_date) DYE_DATE, trunc(PL_DATE) PL_DATE
       ,trunc(QT_DATE) QT_DATE, nyis.DEDUCT_DATE_OTP(a.ship_date) SHIP_DATE, P_TIME, F_TIME, PRELAB_STATUS
       ,FIRST_BATCH, nyis.DEDUCT_DATE_OTP(a.SCH_CLOSED_DATE) SCH_CLOSED_DATE, SO_FOB_CODE,trunc(ENTRY_DATE) LAST_UPDATE, Status_WIP, Mps_Condition
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

  _filename = r"C:\QVD_DATA\PRO_NYK\NYK_DATA_MINING.xlsx"

  df = pd.read_sql_query(sql, conn)

  df.to_excel(_filename, index=False)
  args = ['NYK','SF5SPO001',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()
  print("COMPLETE NYK_DATA_MINING.xlsx")
  sendLine("COMPLETE NYK_DATA_MINING.xlsx")

#############################################


threads = []

thread1 = CLS_NYK_DATA_MINING(); thread1.start(); threads.append(thread1)


for t in threads:
    t.join()
print ("COMPLETE")

