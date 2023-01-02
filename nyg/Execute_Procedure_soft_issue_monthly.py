
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
class CLS_AUTO_ALERT_ISSUE_MONTHLY(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        AUTO_ALERT_ISSUE_MONTHLY()
        
def AUTO_ALERT_ISSUE_MONTHLY():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft GRW Error',err)
      sendLine("Connection Database Soft GRW Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures Auto Mail Outstanding PO  (GRW)")
        cursor.callproc('NYG_ALERT_STOCK_ISSUE_MONTHLY')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  Auto Mail Outstanding PO  (GRW) complete')
        sendLine("Run Procedures  Auto Mail Outstanding PO  (GRW) complete")
     finally:
        cursor.close()
 finally:
     conn.close()

'''###########################################
class CLS_AUTO_ALERT_PENDING_PO_NYG(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        AUTO_ALERT_PENDING_PO_NYG()
        
def AUTO_ALERT_PENDING_PO_NYG():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures Auto Mail Outstanding PO  (NYG)")
        cursor.callproc('NYG_Daily_release_bom2')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  Auto Mail Outstanding PO  (NYG) complete')
        sendLine("Run Procedures  Auto Mail Outstanding PO  (NYG) complete")
     finally:
        cursor.close()
 finally:
     conn.close() '''
threads = []

thread1 = CLS_AUTO_ALERT_ISSUE_MONTHLY();thread1.start();threads.append(thread1) 



for t in threads:
    t.join()
print ("COMPLETE")

