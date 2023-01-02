
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
class CLS_AUTO_ALERT_APPPOALL(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        AUTO_ALERT_APPPOALL()
        
def AUTO_ALERT_APPPOALL():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.110.6",port=1521,sid="ORCL")
  conn = cx_Oracle.connect(user="trm", password="trm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft TRM Error',err)
      sendLine("Connection Database Soft TRM Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Auto email notify P/E on soft square system")
        cursor.callproc('NYG_DAILY_RELEASE_BOM2')
         
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  Auto email notify P/E on soft square system')
        sendLine("Run Procedures  Auto email notify P/E on soft square system")
     finally:
        cursor.close()
 finally:
     conn.close()

#############################################
threads = []

thread1 = CLS_AUTO_ALERT_APPPOALL();thread1.start();threads.append(thread1) 



for t in threads:
    t.join()
print ("COMPLETE")

