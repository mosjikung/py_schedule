
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
class CLS_GET_PO_RM1(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_PO_RM1()
        
def GET_PO_RM1():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="vn", password="vn", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYV Error',err)
      sendLine("Connection Database Soft NYV Error",err)
 else:
     try:    
        cursor = conn.cursor()
        print('Start Run  QVD_INS_BILL_OF_MATERIALS_M01  ')
        sendLine("Start Procerdures QVD_INS_BILL_OF_MATERIALS_M01")
        cursor.callproc('QVD_INS_BILL_OF_MATERIALS_M01')      
     except Exception  as err:
        print('Can not run Procedures1',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run  Procerdures QVD_INS_BILL_OF_MATERIALS_M01 Done')
        sendLine("Run Procedures QVD_INS_BILL_OF_MATERIALS_M01 Done")
     finally:
        cursor.close()
 finally:
     conn.close()


###########################################
class CLS_GET_PO_RM2(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_PO_RM2()
        
def GET_PO_RM2():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="vn", password="vn", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYV Error',err)
      sendLine("Connection Database Soft NYV Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures QVD_INS_BILL_OF_MATERIALS_M02")
        cursor.callproc('QVD_INS_BILL_OF_MATERIALS_M02')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures QVD_INS_BILL_OF_MATERIALS_M02 Done')
        sendLine("Run Procedures QVD_INS_BILL_OF_MATERIALS_M02 Done")
     finally:
        cursor.close()
 finally:
     conn.close()     

###########################################
class CLS_GET_PO_RM3(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_PO_RM3()
        
def GET_PO_RM3():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="vn", password="vn", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYV Error',err)
      sendLine("Connection Database Soft NYV Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures QVD_INS_BILL_OF_MATERIALS_M03")
        cursor.callproc('QVD_INS_BILL_OF_MATERIALS_M03')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  QVD_INS_BILL_OF_MATERIALS_M03 Done')
        sendLine("Run Procedures QVD_INS_BILL_OF_MATERIALS_M03 Done")
     finally:
        cursor.close()
 finally:
     conn.close()  
###########################################
class CLS_GET_PO_RM4(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_PO_RM4()
        
def GET_PO_RM4():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="vn", password="vn", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYV Error',err)
      sendLine("Connection Database Soft NYV Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures QVD_INS_BILL_OF_MATERIALS_M04")
        cursor.callproc('QVD_INS_BILL_OF_MATERIALS_M04')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  QVD_INS_BILL_OF_MATERIALS_M04 Done')
        sendLine("Run Procedures  QVD_INS_BILL_OF_MATERIALS_M04 Done")
     finally:
        cursor.close()
 finally:
     conn.close()      
###########################################
class CLS_GET_PO_RM5(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_PO_RM5()
        
def GET_PO_RM5():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="vn", password="vn", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYV Error',err)
      sendLine("Connection Database Soft NYV Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures QVD_INS_BILL_OF_MATERIALS_M05")
        cursor.callproc('QVD_INS_BILL_OF_MATERIALS_M05')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  QVD_INS_BILL_OF_MATERIALS_M05 Done')
        sendLine("Run Procedures  QVD_INS_BILL_OF_MATERIALS_M05 Done")
     finally:
        cursor.close()
 finally:
     conn.close()    
###########################################
class CLS_GET_PO_RM6(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_PO_RM6()
        
def GET_PO_RM6():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="vn", password="vn", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYV Error',err)
      sendLine("Connection Database Soft NYV Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures QVD_INS_BILL_OF_MATERIALS_M06")
        cursor.callproc('QVD_INS_BILL_OF_MATERIALS_M06')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  QVD_INS_BILL_OF_MATERIALS_M06 Done')
        sendLine("Run Procedures  QVD_INS_BILL_OF_MATERIALS_M06 Done")
     finally:
        cursor.close()
 finally:
     conn.close()  
###########################################
class CLS_GET_PO_RM7(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_PO_RM7()
        
def GET_PO_RM7():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="vn", password="vn", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYV Error',err)
      sendLine("Connection Database Soft NYV Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures QVD_INS_BILL_OF_MATERIALS_M07 ")
        cursor.callproc('QVD_INS_BILL_OF_MATERIALS_M07')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  QVD_INS_BILL_OF_MATERIALS_M07 Done')
        sendLine("Run Procedures QVD_INS_BILL_OF_MATERIALS_M07 Done")
     finally:
        cursor.close()
 finally:
     conn.close()      
###########################################
class CLS_GET_PO_RM8(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_PO_RM8()
        
def GET_PO_RM8():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="vn", password="vn", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYV Error',err)
      sendLine("Connection Database Soft NYV Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures QVD_INS_BILL_OF_MATERIALS_M08")
        cursor.callproc('QVD_INS_BILL_OF_MATERIALS_M08')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  QVD_INS_BILL_OF_MATERIALS_M08 Done')
        sendLine("Run Procedures  QVD_INS_BILL_OF_MATERIALS_M08 Done")
     finally:
        cursor.close()
 finally:
     conn.close()    
###########################################
class CLS_GET_PO_RM9(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_PO_RM9()
        
def GET_PO_RM9():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="vn", password="vn", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYV Error',err)
      sendLine("Connection Database Soft NYV Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures QVD_INS_BILL_OF_MATERIALS_M09")
        cursor.callproc('QVD_INS_BILL_OF_MATERIALS_M09')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run ProceduresQVD_INS_BILL_OF_MATERIALS_M09 Done')
        sendLine("Run Procedures  QVD_INS_BILL_OF_MATERIALS_M09 Done")
     finally:
        cursor.close()
 finally:
     conn.close()  

     ###########################################
class CLS_GET_PO_RM10(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_PO_RM10()
        
def GET_PO_RM10():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="vn", password="vn", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYV Error',err)
      sendLine("Connection Database Soft NYV Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures QVD_INS_BILL_OF_MATERIALS_M10")
        cursor.callproc('QVD_INS_BILL_OF_MATERIALS_M10')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  QVD_INS_BILL_OF_MATERIALS_M10 Done')
        sendLine("Run Procedures  QVD_INS_BILL_OF_MATERIALS_M10 Done")
     finally:
        cursor.close()
 finally:
     conn.close()
###########################################
class CLS_GET_PO_RM11(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_PO_RM11()
        
def GET_PO_RM11():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="vn", password="vn", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYV Error',err)
      sendLine("Connection Database Soft NYV Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures QVD_INS_BILL_OF_MATERIALS_M11")
        cursor.callproc('QVD_INS_BILL_OF_MATERIALS_M11')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  QVD_INS_BILL_OF_MATERIALS_M11 Done')
        sendLine("Run Procedures  QVD_INS_BILL_OF_MATERIALS_M11 Done")
     finally:
        cursor.close()
 finally:
     conn.close()   
###########################################
class CLS_GET_PO_RM12(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_PO_RM12()
        
def GET_PO_RM12():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="vn", password="vn", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYV Error',err)
      sendLine("Connection Database Soft NYV Error",err)
 else:
     try:    
        cursor = conn.cursor()
        print('Start Run Procedures QVD_INS_BILL_OF_MATERIALS_M12')
        sendLine("Start  Run Procerdures  QVD_INS_BILL_OF_MATERIALS_M12")
        cursor.callproc('QVD_INS_BILL_OF_MATERIALS_M12')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  QVD_INS_BILL_OF_MATERIALS_M12 DONE')
        sendLine("Run Procedures  QVD_INS_BILL_OF_MATERIALS_M12 DONE")
     finally:
        cursor.close()
 finally:
     conn.close()       


#############################################
threads = []

thread1 = CLS_GET_PO_RM1();thread1.start();threads.append(thread1) 
thread2 = CLS_GET_PO_RM2();thread2.start();threads.append(thread2)
thread3 = CLS_GET_PO_RM3();thread3.start();threads.append(thread3) 
thread4 = CLS_GET_PO_RM4();thread4.start();threads.append(thread4)  
thread5 = CLS_GET_PO_RM5();thread5.start();threads.append(thread5) 
thread6 = CLS_GET_PO_RM6();thread6.start();threads.append(thread6) 
thread7 = CLS_GET_PO_RM7();thread7.start();threads.append(thread7) 
thread8 = CLS_GET_PO_RM8();thread8.start();threads.append(thread8) 
thread9 = CLS_GET_PO_RM9();thread9.start();threads.append(thread9) 
thread10 = CLS_GET_PO_RM10();thread10.start();threads.append(thread10) 
thread11 = CLS_GET_PO_RM11();thread11.start();threads.append(thread11) 
thread12 = CLS_GET_PO_RM12();thread12.start();threads.append(thread12) 

for t in threads:
    t.join()
print ("NYV COMPLETE")

