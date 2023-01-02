
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


def sendLine(txt,err):
  url = 'https://notify-api.line.me/api/notify'
  token = 'ZE6d4wFQO2qQiSMAMqecrPbj6R3nhj0y1STqOJ6xQ1s'
  headers = {'content-type':'application/x-www-form-urlencoded','Authorization':'Bearer ' + token}
  msg = txt + err
  requests.post(url,headers=headers,data = {'message':msg})



###########################################
class CLS_AUTO_Update_Cost_WCSG1(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        AUTO_Update_Cost_WCSG1()
        
def AUTO_Update_Cost_WCSG1():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.82",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database WCS Error',err)
      ##sendLine("Connection Database WCS Error",err)
 else:
     try:    
        cursor = conn.cursor()
        ##sendLine("Start Procerdures Auto Update Cost WCS  (WCS G1,G3,G5)")

        args = ['G1','G1WCSACC0002','AUTO_UPD_COST_FAB_BY_CUT']
        result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
        cursor.callproc('AUTO_UPD_COST_FAB_BY_CUT')
        args = ['G1','G1WCSACC0002',cursor.rowcount]
        result_args = cursor.callproc('QVD_RUN_upd_LOG', args)

     except Exception  as err:
        print('Can not run Procedures',err)
        ##sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  Auto Update Cost  (WCS G1) complete')
        ##sendLine("Run Procedures  Auto Update Cost WCS  (WCS G1,G3,G5) complete")
     finally:
        cursor.close()
 finally:
     conn.close()

###########################################
class CLS_AUTO_Update_Cost_WCSG3(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        AUTO_Update_Cost_WCSG3()
        
def AUTO_Update_Cost_WCSG3():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.82",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg3", password="nyg3", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database WCS Error',err)
      ##sendLine("Connection Database WCS Error",err)
 else:
     try:    
        cursor = conn.cursor()


        args = ['G3','G1WCSACC0002','AUTO_UPD_COST_FAB_BY_CUT']
        result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
        cursor.callproc('AUTO_UPD_COST_FAB_BY_CUT')
        args = ['G3','G1WCSACC0002',cursor.rowcount]
        result_args = cursor.callproc('QVD_RUN_upd_LOG', args)

     except Exception  as err:
        print('Can not run Procedures',err)
        ##sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  Auto Update Cost  (WCS G1,G3,G5) complete')
        ##sendLine("Run Procedures  Auto Update Cost WCS  (WCS G1,G3,G5) complete")
     finally:
        cursor.close()
 finally:
     conn.close()

###########################################
class CLS_AUTO_Update_Cost_WCSG2(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        AUTO_Update_Cost_WCSG2()
        
def AUTO_Update_Cost_WCSG2():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.84",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg_pho", password="nyg_pho", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database WCS Error',err)
      ##sendLine("Connection Database WCS Error",err)
 else:
     try:    
        cursor = conn.cursor()
        ##sendLine("Start Procerdures Auto Update Cost WCS  (WCS G2)")

        
        args = ['G2','G1WCSACC0002','AUTO_UPD_COST_FAB_BY_CUT']
        result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
        cursor.callproc('AUTO_UPD_COST_FAB_BY_CUT')        
        args = ['G2','G1WCSACC0002',cursor.rowcount]
        result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
             
     except Exception  as err:
        print('Can not run Procedures',err)
        ##sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  Auto Update Cost  (WCS G2) complete')
        ##sendLine("Run Procedures  Auto Update Cost WCS  (WCS G2) complete")
     finally:
        cursor.close()
 finally:
     conn.close()   

###########################################
class CLS_AUTO_Update_Cost_WCSG4(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        AUTO_Update_Cost_WCSG4()
        
def AUTO_Update_Cost_WCSG4():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.86",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg4", password="nyg4", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database WCS Error',err)
      ##sendLine("Connection Database WCS Error",err)
 else:
     try:    
        cursor = conn.cursor()
        ##sendLine("Start Procerdures Auto Update Cost WCS  (WCS G4)")

        args = ['G4','G1WCSACC0002','AUTO_UPD_COST_FAB_BY_CUT']
        result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
        cursor.callproc('AUTO_UPD_COST_FAB_BY_CUT')
        args = ['G4','G1WCSACC0002',cursor.rowcount]
        result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
             
     except Exception  as err:
        print('Can not run Procedures',err)
        ##sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  Auto Update Cost  (WCS G4) complete')
        ##sendLine("Run Procedures  Auto Update Cost WCS  (WCS G4) complete")
     finally:
        cursor.close()
 finally:
     conn.close()

###########################################
class CLS_AUTO_Update_Cost_WCSG5(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        AUTO_Update_Cost_WCSG5()
        
def AUTO_Update_Cost_WCSG5():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.110.6",port=1521,sid="ORCL")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database WCS Error',err)
      ##sendLine("Connection Database WCS Error",err)
 else:
     try:    
        cursor = conn.cursor()
        ##sendLine("Start Procerdures Auto Update Cost WCS  (WCS G4)")

        args = ['G5','G1WCSACC0002','AUTO_UPD_COST_FAB_BY_CUT']
        result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
        cursor.callproc('AUTO_UPD_COST_FAB_BY_CUT')
        args = ['G5','G1WCSACC0002',cursor.rowcount]
        result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
             
     except Exception  as err:
        print('Can not run Procedures',err)
        ##sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  Auto Update Cost  (WCS G5) complete')
        ##sendLine("Run Procedures  Auto Update Cost WCS  (WCS G4) complete")
     finally:
        cursor.close()
 finally:
     conn.close()

###########################################

class CLS_AUTO_Update_Cost_WCSG6(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        AUTO_Update_Cost_WCSG6()
        
def AUTO_Update_Cost_WCSG6():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="VN")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database WCS Error',err)
      ##sendLine("Connection Database WCS Error",err)
 else:
     try:    
        cursor = conn.cursor()
        ##sendLine("Start Procerdures Auto Update Cost WCS  (WCS NYV)")

        args = ['G6','G1WCSACC0002','AUTO_UPD_COST_FAB_BY_CUT']
        result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
        cursor.callproc('AUTO_UPD_COST_FAB_BY_CUT')
        args = ['G6','G1WCSACC0002',cursor.rowcount]
        result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
             
     except Exception  as err:
        print('Can not run Procedures',err)
        ##sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  Auto Update Cost  (WCS NYV) complete')
        ##sendLine("Run Procedures  Auto Update Cost WCS  (WCS NYV) complete")
     finally:
        cursor.close()
 finally:
     conn.close()

###########################################
class CLS_AUTO_Update_Cost_WCSG7(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        AUTO_Update_Cost_WCSG7()
        
def AUTO_Update_Cost_WCSG7():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.87",port=1521,sid="nytg")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database WCS Error',err)
      ##sendLine("Connection Database WCS Error",err)
 else:
     try:    
        cursor = conn.cursor()
        ##sendLine("Start Procerdures Auto Update Cost WCS  (WCS GRW)")

        args = ['G7','G1WCSACC0002','AUTO_UPD_COST_FAB_BY_CUT']
        result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
        cursor.callproc('AUTO_UPD_COST_FAB_BY_CUT')
        args = ['G7','G1WCSACC0002',cursor.rowcount]
        result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
             
     except Exception  as err:
        print('Can not run Procedures',err)
        ##sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  Auto Update Cost  (WCS GRW) complete')
        ##sendLine("Run Procedures  Auto Update Cost WCS  (WCS GRW) complete")
     finally:
        cursor.close()
 finally:
     conn.close()     

###########################################


threads = []
thread1 = CLS_AUTO_Update_Cost_WCSG1();thread1.start();threads.append(thread1) 
thread7 = CLS_AUTO_Update_Cost_WCSG3();thread7.start();threads.append(thread7) 
thread2 = CLS_AUTO_Update_Cost_WCSG2();thread2.start();threads.append(thread2)
thread3 = CLS_AUTO_Update_Cost_WCSG4();thread3.start();threads.append(thread3)
thread6 = CLS_AUTO_Update_Cost_WCSG5();thread6.start();threads.append(thread6)
thread4 = CLS_AUTO_Update_Cost_WCSG6();thread4.start();threads.append(thread4)
thread5 = CLS_AUTO_Update_Cost_WCSG7();thread5.start();threads.append(thread5)




for t in threads:
    t.join()
print ("COMPLETE")

