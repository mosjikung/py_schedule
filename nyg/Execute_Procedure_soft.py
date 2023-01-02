
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
class CLS_AUTO_ALERT_PENDING_PO_GRW(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        AUTO_ALERT_PENDING_PO_GRW()
        
def AUTO_ALERT_PENDING_PO_GRW():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.87",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft GRW Error',err)
      sendLine("Connection Database Soft GRW Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures Auto Mail Outstanding PO  (GRW)")
        cursor.callproc('NYG_Daily_release_bom2')
       
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

###########################################
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
     conn.close()

###########################################
class CLS_AUTO_UPD_CHECK_ONHAND_GRW(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        AUTO_UPD_CHECK_ONHAND_GRW()
        
def AUTO_UPD_CHECK_ONHAND_GRW():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.87",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft GRW Error',err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start CHECK_ONHAND_GRW ")
        cursor.callproc('AUTO_UPD_RM_PHY_STOCK_DETAIL')
       
     except Exception  as err:
        print('Can not run Procedures',err)
     else:
        print('Run Procedures  CHECK_ONHAND_GRW complete')
        sendLine("Run Procedures  CHECK_ONHAND_GRW  complete")
     finally:
        cursor.close()
 finally:
     conn.close()
###########################################
class CLS_AUTO_UPD_CHECK_ONHAND_NYG(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        AUTO_UPD_CHECK_ONHAND_NYG()
        
def AUTO_UPD_CHECK_ONHAND_NYG():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures AUTO_UPD_CHECK_ONHAND_NYG")
        cursor.callproc('AUTO_UPD_RM_PHY_STOCK_DETAIL')
               
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  AUTO_UPD_CHECK_ONHAND_NYG complete')
        sendLine("Run Procedures  AUTO_UPD_CHECK_ONHAND_NYG complete")
     finally:
        cursor.close()
 finally:
     conn.close()

###########################################
class CLS_AUTO_UPD_CHECK_ONHAND_TRM(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        AUTO_UPD_CHECK_ONHAND_TRM()
        
def AUTO_UPD_CHECK_ONHAND_TRM():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft TRM Error',err)
      sendLine("Connection Database Soft TRM Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures AUTO_UPD_CHECK_ONHAND_TRM")
        cursor.callproc('TRM.AUTO_UPD_RM_PHY_STOCK_DETAIL@TRM.WORLD')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  AUTO_UPD_CHECK_ONHAND_TRM complete')
        sendLine("Run Procedures  AUTO_UPD_CHECK_ONHAND_TRM complete")
     finally:
        cursor.close()
 finally:
     conn.close()

###########################################
class CLS_AUTO_UPD_PO_EARLY(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        AUTO_UPD_PO_EARLY()
        
def AUTO_UPD_PO_EARLY():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        print('Start Run Procedures  AUTO_UPD_PO_EARLY NYG ')
        sendLine("Start Procerdures AUTO_UPD_PO_EARLY NYG")
        cursor.callproc('AUTO_UPD_PO_EARLY')
               
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  AUTO_UPD_PO_EARLY complete NYG')
        sendLine("Run Procedures  AUTO_UPD_PO_EARLY complete NYG")
     finally:
        cursor.close()
 finally:
     conn.close()     
###########################################
class CLS_AUTO_UPD_PO_EARLY_GRW(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        AUTO_UPD_PO_EARLY_GRW()
        
def AUTO_UPD_PO_EARLY_GRW():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.87",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        print('Start Run Procedures  AUTO_UPD_PO_EARLY GRW ')
        sendLine("Start Procerdures AUTO_UPD_PO_EARLY GRW")
        cursor.callproc('AUTO_UPD_PO_EARLY')
               
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  AUTO_UPD_PO_EARLY complete GRW')
        sendLine("Run Procedures  AUTO_UPD_PO_EARLY complete GRW")
     finally:
        cursor.close()
 finally:
     conn.close()  
###########################################
class CLS_AUTO_MANAGE_AUTO_EMAIL_ACTIVE(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        AUTO_MANAGE_AUTO_EMAIL_ACTIVE()
        
def AUTO_MANAGE_AUTO_EMAIL_ACTIVE():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        print('Start Run Procedures  MANAGE_AUTO_EMAIL_ACTIVE NYG ')
        sendLine("Start Procerdures MANAGE_AUTO_EMAIL_ACTIVE")
        cursor.callproc('AUTO_MANAGE_AUTO_EMAIL_ACTIVE')
               
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  MANAGE_AUTO_EMAIL_ACTIVE complete NYG')
        sendLine("Run Procedures  MANAGE_AUTO_EMAIL_ACTIVE complete NYG")
     finally:
        cursor.close()
 finally:
     conn.close()  

###########################################
class CLS_AUTO_NYG_ALERT_STOCK_TRANSFER(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        AUTO_NYG_ALERT_STOCK_TRANSFER()
        
def AUTO_NYG_ALERT_STOCK_TRANSFER():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        print('Start Run Procedures  NYG_ALERT_STOCK_TRANSFER ')
        sendLine("Start Procerdures _NYG_ALERT_STOCK_TRANSFER")
        cursor.callproc('NYG_ALERT_STOCK_TRANSFER')
               
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  NYG_ALERT_STOCK_TRANSFERcomplete NYG')
        sendLine("Run Procedures  NYG_ALERT_STOCK_TRANSFER complete NYG")
     finally:
        cursor.close()
 finally:
     conn.close()                   

###########################################
class CLS_AUTO_NYG_ALERT_STOCK_OVER_ISSUE(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        AUTO_NYG_ALERT_STOCK_OVER_ISSUE()
        
def AUTO_NYG_ALERT_STOCK_OVER_ISSUE():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        print('Start Run Procedures  NYG_ALERT_NYG_ALERT_STOCK_ISSUE OVER 3 DAY  ')
        sendLine("Start Procerdures NYG_ALERT_NYG_ALERT_STOCK_ISSUE OVER 3 DAY ")
        cursor.callproc('NYG_ALERT_STOCK_ISSUE')
               
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  NYG_ALERT_NYG_ALERT_STOCK_ISSUE OVER 3 DAY complete NYG')
        sendLine("Run Procedures  NYG_ALERT_NYG_ALERT_STOCK_ISSUE OVER 3 DAY  complete NYG")
     finally:
        cursor.close()
 finally:
     conn.close()   
###########################################
class CLS_AUTO_NYG_ALERT_STOCK_PR(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        AUTO_NYG_ALERT_STOCK_PR()
        
def AUTO_NYG_ALERT_STOCK_PR():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        print('Start Run Procedures  NYG_ALERT_NYG_ALERT_STOCK_PR  ')
        sendLine("Start Procerdures NYG_ALERT_NYG_ALERT_STOCK_PR ")
        cursor.callproc('NYG_ALERT_STOCK_PR')
               
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  NYG_ALERT_NYG_ALERT_STOCK_PR complete NYG')
        sendLine("Run Procedures  NYG_ALERT_NYG_ALERT_STOCK_PR  complete NYG")
     finally:
        cursor.close()
 finally:
     conn.close()
###########################################
class CLS_AUTO_NYG_ALERT_SML(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        AUTO_NYG_ALERT_SML()
        
def AUTO_NYG_ALERT_SML():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        print('Start Run Procedures  NYG_ALERT_NYG_ALERT_SML  ')
        sendLine("Start Procerdures NYG_ALERT_NYG_ALERT_SML ")
        cursor.callproc('NYG_ALERT_PO_SML')
               
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  NYG_ALERT_NYG_ALERT_SML complete NYG')
        sendLine("Run Procedures  NYG_ALERT_NYG_ALERT_SML  complete NYG")
     finally:
        cursor.close()
 finally:
     conn.close()

###########################################
class CLS_AUTO_NYG_ALERT_STOCK_FG(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        AUTO_NYG_ALERT_STOCK_FG()
        
def AUTO_NYG_ALERT_STOCK_FG():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        print('Start Run Procedures  NYG_ALERT_NYG_ALERT_STOCK_FG  ')
        sendLine("Start Procerdures NYG_ALERT_NYG_ALERT_STOCK_FG ")
        cursor.callproc('NYG_ALERT_STOCK_FG')
               
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  NYG_ALERT_NYG_ALERT_STOCK_FG complete NYG')
        sendLine("Run Procedures  NYG_ALERT_NYG_ALERT_STOCK_FG  complete NYG")
     finally:
        cursor.close()
 finally:
     conn.close() 
###########################################
class CLS_AUTO_CLOSE_PO_STATUS(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        AUTO_CLOSE_PO_STATUS()
        
def AUTO_CLOSE_PO_STATUS():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        print('Start Run Procedures  AUTO_CLOSE_PO_STATUS  ')
        sendLine("Start Procerdures AUTO_CLOSE_PO_STATUS ")
        cursor.callproc('AUTO_CLOSE_PO_STATUS')
               
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  AUTO_CLOSE_PO_STATUS complete NYG')
        sendLine("Run Procedures  AUTO_CLOSE_PO_STATUS  complete NYG")
     finally:
        cursor.close()
 finally:
     conn.close()  

#############################################
threads = []

thread1 = CLS_AUTO_ALERT_PENDING_PO_NYG();thread1.start();threads.append(thread1) 
thread5 = CLS_AUTO_ALERT_PENDING_PO_GRW();thread5.start();threads.append(thread5) 
thread2 = CLS_AUTO_UPD_CHECK_ONHAND_GRW();thread2.start();threads.append(thread2)
thread3 = CLS_AUTO_UPD_CHECK_ONHAND_NYG();thread3.start();threads.append(thread3)
thread4 = CLS_AUTO_UPD_CHECK_ONHAND_TRM();thread4.start();threads.append(thread4)
thread5 = CLS_AUTO_UPD_PO_EARLY();thread5.start();threads.append(thread5)
thread6 = CLS_AUTO_UPD_PO_EARLY_GRW();thread6.start();threads.append(thread6)
thread7 = CLS_AUTO_MANAGE_AUTO_EMAIL_ACTIVE();thread7.start();threads.append(thread7)
thread8 = CLS_AUTO_NYG_ALERT_STOCK_TRANSFER();thread8.start();threads.append(thread8)
thread9 = CLS_AUTO_NYG_ALERT_STOCK_OVER_ISSUE();thread9.start();threads.append(thread9)
thread10 = CLS_AUTO_NYG_ALERT_STOCK_PR();thread10.start();threads.append(thread10)
thread11 = CLS_AUTO_NYG_ALERT_STOCK_FG();thread11.start();threads.append(thread11)
thread12 = CLS_AUTO_NYG_ALERT_SML();thread12.start();threads.append(thread12)
thread13 = CLS_AUTO_CLOSE_PO_STATUS();thread13.start();threads.append(thread13)



for t in threads:
    t.join()
print ("COMPLETE")

