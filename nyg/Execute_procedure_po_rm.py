
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
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        print('Start Run  Procerdures get PO RM BOM Soft M01 ')
        sendLine("Start Procerdures get PO RM BOM Soft NYG1")
        cursor.callproc('AUTO_UPD_NY_PO_NYG_RM2001')      
     except Exception  as err:
        print('Can not run Procedures1',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run  Procerdures get PO RM BOM Soft M01 Done')
        sendLine("Run Procedures  Procerdures get PO RM BOM Soft M01 Done")
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
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG2")
        cursor.callproc('AUTO_UPD_NY_PO_NYG_RM2002')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  Procerdures get PO RM BOM Soft NYG2')
        sendLine("Run Procedures  Procerdures get PO RM BOM Soft NYG2")
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
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG3")
        cursor.callproc('AUTO_UPD_NY_PO_NYG_RM2003')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  Procerdures get PO RM BOM Soft NYG3')
        sendLine("Run Procedures  Procerdures get PO RM BOM Soft NYG3")
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
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG4")
        cursor.callproc('AUTO_UPD_NY_PO_NYG_RM2004')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  Procerdures get PO RM BOM Soft NYG4')
        sendLine("Run Procedures  Procerdures get PO RM BOM Soft NYG4")
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
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG5")
        cursor.callproc('AUTO_UPD_NY_PO_NYG_RM2005')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  Procerdures get PO RM BOM Soft NYG5')
        sendLine("Run Procedures  Procerdures get PO RM BOM Soft NYG5")
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
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG6")
        cursor.callproc('AUTO_UPD_NY_PO_NYG_RM2006')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  Procerdures get PO RM BOM Soft NYG6')
        sendLine("Run Procedures  Procerdures get PO RM BOM Soft NYG6")
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
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG7")
        cursor.callproc('AUTO_UPD_NY_PO_NYG_RM2007')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  Procerdures get PO RM BOM Soft NYG7')
        sendLine("Run Procedures  Procerdures get PO RM BOM Soft NYG7")
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
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG8")
        cursor.callproc('AUTO_UPD_NY_PO_NYG_RM2008')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  Procerdures get PO RM BOM Soft NYG8')
        sendLine("Run Procedures  Procerdures get PO RM BOM Soft NYG8")
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
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG9")
        cursor.callproc('AUTO_UPD_NY_PO_NYG_RM2009')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  Procerdures get PO RM BOM Soft NYG9')
        sendLine("Run Procedures  Procerdures get PO RM BOM Soft NYG9")
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
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG10")
        cursor.callproc('AUTO_UPD_NY_PO_NYG_RM2010')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  Procerdures get PO RM BOM Soft NYG10')
        sendLine("Run Procedures  Procerdures get PO RM BOM Soft NYG10")
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
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG11")
        cursor.callproc('AUTO_UPD_NY_PO_NYG_RM2011')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  Procerdures get PO RM BOM Soft NYG11')
        sendLine("Run Procedures  Procerdures get PO RM BOM Soft NYG11")
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
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        print('Start Run Procedures PO RM BOM Soft M12')
        sendLine("Start  Run Procerdures  PO RM BOM Soft M12")
        cursor.callproc('AUTO_UPD_NY_PO_NYG_RM2012')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures   PO RM BOM Soft M12 DONE')
        sendLine("Run Procedures   PO RM BOM Soft M12 DONE")
     finally:
        cursor.close()
 finally:
     conn.close()       

###########################################
class CLS_GET_PO_RM_D(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_PO_RM_D()
        
def GET_PO_RM_D():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        print('Run start Procerdures PO RM BOM Soft NYG Direct')
        sendLine("Start Procerdures PO RM BOM Soft NYG Dirrect")
        cursor.callproc('AUTO_UPD_NY_PO_NYG_RM_DIRECT')
       
     except Exception  as err:
        print('Can not run Procedures',err)
        sendLine("Can not run Procedures Error",err)
     else:
        print('Run Procedures  Procerdures get PO RM BOM Soft NYG Direct Done')
        sendLine("Run Procedures  Procerdures get PO RM BOM Soft Direct Done")
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
thread13 = CLS_GET_PO_RM_D();thread13.start();threads.append(thread13) 

for t in threads:
    t.join()
print ("COMPLETE")

