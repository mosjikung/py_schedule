
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
class CLS_GET_PO_RM_PO_UPD(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_PO_RM_PO_UPD()
        
def GET_PO_RM_PO_UPD():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        print('Start Run  Procerdures QVD_INS_BILL_OF_MATERIAL_UPD ')
        sendLine("Start Procerdures QVD_INS_BILL_OF_MATERIAL_UPD")
        cursor.callproc('QVD_INS_BILL_OF_MATERIAL_UPD')            
     except Exception  as err:
        print('Can not run Procedures1',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run  Procerdures QVD_INS_BILL_OF_MATERIAL_UPD Done')
        sendLine("Run Procedures  QVD_INS_BILL_OF_MATERIAL_UPD Done")
     finally:
        cursor.close()
 finally:
     conn.close()



#############################################
threads = []

thread1 = CLS_GET_PO_RM_PO_UPD();thread1.start();threads.append(thread1) 


for t in threads:
    t.join()
print ("COMPLETE")

