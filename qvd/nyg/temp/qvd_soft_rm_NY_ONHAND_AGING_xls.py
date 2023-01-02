import cx_Oracle
import csv
import os
from pathlib import Path
import requests
from datetime import datetime, timedelta
import threading
import time
import numpy as np
import pandas as pd
import asyncio


oracle_client = "C:\instantclient_19_5"
os.environ["ORACLE_HOME"] = oracle_client
os.environ["PATH"] = oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.TH8TISASCII"

time_start = datetime.now()


def printttime(txt):
  dateTimeObj = datetime.now()
  timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
  now = datetime.now()
  duration = now - time_start
  print(timestampStr + ' ' + str(duration.total_seconds()) + ' ' + txt)


###########################################

class CLS_FC_PO(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
  def run(self):
    FC_PO()


def FC_PO():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="NYGM", password="NYGM",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  printttime('SO_FC_PO Start')

  sql = """
SELECT A.OU_CODE OUCODE,C.DOC_TYPE,  
        A.DOC_TYPE, C.DOC_DESC, 
        A.DOC_NO,  
        TRUNC(A.DOC_DATE) DOC_DATE,  
        a.po_no_doc, 
        nvl(A.SO_NO_DOC,GET_PO_SO_ALLOT_PO_CONCAT(a.ou_code,a.po_year,a.po_no)) SO_NO_DOC,  
        A.SO_YEAR,  
        A.SO_NO,  
        A.CREATE_BY,OE.USER_DESC(A.CREATE_BY)USER_NAME, 
        B.ITEM_SEQ,  
        B.ITEM_COLOR, 
        B.ITEM_SIZE, 
        RM.GET_GROUP_CODE(B.ITEM_SEQ) GROUP_CODE,  
       RM.GROUP_DESC(a.ou_code,RM.GET_GROUP_CODE(B.ITEM_SEQ))  GROUP_DESC,  
        RM.GET_ITEM_CODE(B.ITEM_SEQ) ITEM_CODE,  
        RM.ITEM_SEQ_DESC(B.ITEM_SEQ) ITEM_NAME,  
        B.QTY Receive_QTY, 
        B.INV_QTY PO_INV_QTY, 
        B.SUOM_CODE PO_SUOM_CODE, 
        b.RATIO_STOCK, 
        b.RATIO, 
        B.ITEM_QTY QTY_STOCK, 
        B.UOM_CODE STOCK_UOM_CODE,  
        B.AMT AMT_BATH, 
        B.PRICE_PER_UNIT PO_PRICE_PER_UNIT , 
        B.POST_AMT PO_POST_AMT, 
        B.BATCH_NO,  
        A.DOC_STATUS,  
        A.WH_CODE, RM.WARE_DESC(A.OU_CODE,A.WH_CODE) WH_DESC, 
        B.REC_SEQ  
      FROM RM_TRAN_HEAD A, 
        RM_TRAN_DETAIL B,  
        RM_DOCUMENT C  
      WHERE A.OU_CODE ='N03'   
        AND TRUNC(A.DOC_DATE) BETWEEN TO_DATE('01/01/2021', 'DD/MM/YYYY')  
         AND TO_DATE('31/12/2021', 'DD/MM/YYYY')        
        AND A.DOC_STATUS <> 'C'  
        AND C.DOC_TYPE  ='R'           
        AND C.DOC_CODE NOT IN ('ITRN', 'IADJ','IAJD','RADJ','REXS','ROPS','RTRN')  
        AND A.DOC_TYPE = C.DOC_CODE  
        AND A.OU_CODE = B.OU_CODE  
        AND A.DOC_TYPE = B.DOC_TYPE  
        AND A.DOC_NO = B.DOC_NO  
      ORDER BY A.WH_CODE,C.DOC_TYPE, A.DOC_TYPE,A.DOC_DATE,A.DOC_NO,b.item_seq,B.ITEM_COLOR, B.ITEM_SIZE, B.BATCH_NO

  """

  df = pd.read_sql_query(sql, conn)
  ##df.to_csv(r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM2.csv',encoding='utf-8-sig')
  ##df.to_excel(
  ##    r'C:\Qlikview_Report\PRINT_EMB\NY_PO_NYG_RM.xlsx', index=False)
  df.to_csv(
      r'C:\Qlikview_Report\INVENTORY\Receive2021_02082021.csv', index=False,encoding='utf-8-sig')


  conn.close()

  printttime('FC_PO Complete')

  ##################################################################################

threads = []

##thread1 = CLS_qvd_soft_rm_po_bom() ;thread1.start() ;threads.append(thread1)
thread2 = CLS_FC_PO() ;thread2.start() ;threads.append(thread2)


for t in threads:
    t.join()



