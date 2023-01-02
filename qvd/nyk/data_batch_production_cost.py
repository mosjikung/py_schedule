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
# 10/02/2022
# Request By Sirada Sekekul
# SQL By SUNICHASA.P
# Create By Krisada.R
########
class CLS_DATA_BATCH_PRODUCTION_COST_2021(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_BATCH_PRODUCTION_COST_2021()


def DATA_BATCH_PRODUCTION_COST_2021():
  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="DEMO", password="DEMO", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  args = ['NYK','NYKSPO006','DATA_BATCH_PRODUCTION_COST_2021.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print("START CLS_DATA_BATCH_PRODUCTION_COST_2021")
  sendLine("START CLS_DATA_BATCH_PRODUCTION_COST_2021")

  sql =""" SELECT  CUSTOME_PO,BUYER,MPS_YEAR,MPS_WEEK,OU_CODE,BATCH_NO, SCH_CLOSE_DATE,SO_NO,LINE_ID,CUSTOMER_ID,CUSTOMER_NAME,SALE_ID,SALE_NAME,
              COLOR_CODE,COLOR_DESC,COLOR_SHADE,ITEM_CODE,ITEM_DESC,ITEM_PROCESS,ITEM_DEVELOP,UNIT_QTY,TOTAL_ROLL,TOTAL_QTY,
              BT_FG_KG,BT_SC_KG,BT_LO_KG,BT_RE_KG,METERIAL_GROUP,ACT_ADC_AMT,ACT_SCC_AMT,ACT_IPC_AMT,ACT_SED_AMT,
              STD_SCF_AMT,ACT_OHC_AMT,TOTAL_BATCH_COST,BT_CLOSED_ACTIVE,BT_CLOSED_DATE,SCHEDULE_ID,OE_PRICE_SELL,
              STD_FG_COST,STD_DYE_HR,STD_FG_COST_AMT,SDC_COST,SCC_COST,SED_COST,SCF_COST,OHC_PRE_COST,
              OHC_DYE_COST,OHC_FIN_COST,OHC_INS_COST,OHC_SHIP_COST,IPC_STEP_COST,IPC_MC_COST,IPC_DEPRE_COST,
              ACT_CHEFIN_COST,FG_INACTIVE_KG,STD_KNIT_COST,GF_COST_KG,ACT_DYE_LOAD,STD_DYE_LOAD,STD_DEV_LOSS,
              QN_NO,QN_YARN_COST,QN_EXTR_PRINT,STD_COLAB_DYE_KG,STD_COLAB_CHEMI_KG,FORMULA_NO,STD_FORMULA_FIN_KG,
              SO_UOM, SELL_PER_UOM, YD_PER_KG, SELL_PER_KG, SO_CLOSED_DATE
            FROM V_BT_COST_ACC
            WHERE TRUNC(BT_CLOSED_DATE) < TO_DATE(01/01/2022','DD/MM/RRRR')
             """

  df = pd.read_sql_query(sql, conn)

  # _filename = r"C:\QVD_DATA\COST_SPO\DATA_BATCH_PRODUCTION_COST_2021.xlsx" 
  # df.to_excel(_filename, index=False)

  _filename = r"C:\QVD_DATA\COST_SPO\DATA_BATCH_PRODUCTION_COST_2021.csv"
  df.to_csv(_filename, index=False,encoding='utf-8-sig')
  args = ['NYK','NYKSPO006',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()
  print("COMPLETE CLS_DATA_BATCH_PRODUCTION_COST_2021")
  sendLine("COMPLETE CLS_DATA_BATCH_PRODUCTION_COST_2021")

#############################################


###########################################
########
# 10/02/2022
# Request By Sirada Sekekul
# SQL By SUNICHASA.P
# Create By Krisada.R
########
class CLS_DATA_BATCH_PRODUCTION_COST(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    DATA_BATCH_PRODUCTION_COST()


def DATA_BATCH_PRODUCTION_COST():
  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="DEMO", password="DEMO", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  args = ['NYK','NYKSPO005','DATA_BATCH_PRODUCTION_COST.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print("START CLS_DATA_BATCH_PRODUCTION_COST")
  sendLine("START CLS_DATA_BATCH_PRODUCTION_COST")

  sql =""" SELECT  CUSTOME_PO,BUYER,MPS_YEAR,MPS_WEEK,OU_CODE,BATCH_NO, SCH_CLOSE_DATE,SO_NO,LINE_ID,CUSTOMER_ID,CUSTOMER_NAME,SALE_ID,SALE_NAME,
              COLOR_CODE,COLOR_DESC,COLOR_SHADE,ITEM_CODE,ITEM_DESC,ITEM_PROCESS,ITEM_DEVELOP,UNIT_QTY,TOTAL_ROLL,TOTAL_QTY,
              BT_FG_KG,BT_SC_KG,BT_LO_KG,BT_RE_KG,METERIAL_GROUP,ACT_ADC_AMT,ACT_SCC_AMT,ACT_IPC_AMT,ACT_SED_AMT,
              STD_SCF_AMT,ACT_OHC_AMT,TOTAL_BATCH_COST,BT_CLOSED_ACTIVE,BT_CLOSED_DATE,SCHEDULE_ID,OE_PRICE_SELL,
              STD_FG_COST,STD_DYE_HR,STD_FG_COST_AMT,SDC_COST,SCC_COST,SED_COST,SCF_COST,OHC_PRE_COST,
              OHC_DYE_COST,OHC_FIN_COST,OHC_INS_COST,OHC_SHIP_COST,IPC_STEP_COST,IPC_MC_COST,IPC_DEPRE_COST,
              ACT_CHEFIN_COST,FG_INACTIVE_KG,STD_KNIT_COST,GF_COST_KG,ACT_DYE_LOAD,STD_DYE_LOAD,STD_DEV_LOSS,
              QN_NO,QN_YARN_COST,QN_EXTR_PRINT,STD_COLAB_DYE_KG,STD_COLAB_CHEMI_KG,FORMULA_NO,STD_FORMULA_FIN_KG,
              SO_UOM, SELL_PER_UOM, YD_PER_KG, SELL_PER_KG, SO_CLOSED_DATE
            FROM V_BT_COST_ACC
            WHERE TRUNC(BT_CLOSED_DATE) >= TO_DATE('01/01/2022','DD/MM/RRRR')
             """

  df = pd.read_sql_query(sql, conn)

  # _filename = r"C:\QVD_DATA\COST_SPO\DATA_BATCH_PRODUCTION_COST.xlsx" 
  # df.to_excel(_filename, index=False)

  _filename = r"C:\QVD_DATA\COST_SPO\DATA_BATCH_PRODUCTION_COST.csv"
  df.to_csv(_filename, index=False,encoding='utf-8-sig')
  args = ['NYK','NYKSPO005',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()
  print("COMPLETE CLS_DATA_BATCH_PRODUCTION_COST")
  sendLine("COMPLETE CLS_DATA_BATCH_PRODUCTION_COST")

#############################################


threads = []

#thread1 = CLS_DATA_BATCH_PRODUCTION_COST_2021();thread1.start();threads.append(thread1)
thread2 = CLS_DATA_BATCH_PRODUCTION_COST();thread2.start();threads.append(thread2)

for t in threads:
    t.join()
print ("COMPLETE")

