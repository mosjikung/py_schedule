
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

##BU TRM###

class CLS_GET_PO_RM1(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_PO_RM1()
        
def GET_PO_RM1():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.110.6",port=1521,sid="ORCL")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        print('Start Run  Procerdures get PO RM BOM Soft M01 ')
        sendLine("Start Procerdures get PO RM BOM Soft NYG1")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M01')      
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
  my_dsn = cx_Oracle.makedsn("192.168.110.6",port=1521,sid="ORCL")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG2")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M02')
       
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
  my_dsn = cx_Oracle.makedsn("192.168.110.6",port=1521,sid="ORCL")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG3")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M03')
       
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
  my_dsn = cx_Oracle.makedsn("192.168.110.6",port=1521,sid="ORCL")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG4")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M04')
       
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
  my_dsn = cx_Oracle.makedsn("192.168.110.6",port=1521,sid="ORCL")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG5")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M05')
       
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
  my_dsn = cx_Oracle.makedsn("192.168.110.6",port=1521,sid="ORCL")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG6")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M06')
       
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
  my_dsn = cx_Oracle.makedsn("192.168.110.6",port=1521,sid="ORCL")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG7")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M07')
       
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
  my_dsn = cx_Oracle.makedsn("192.168.110.6",port=1521,sid="ORCL")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG8")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M08')
       
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
  my_dsn = cx_Oracle.makedsn("192.168.110.6",port=1521,sid="ORCL")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG9")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M09')
       
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
  my_dsn = cx_Oracle.makedsn("192.168.110.6",port=1521,sid="ORCL")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG10")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M10')
       
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
  my_dsn = cx_Oracle.makedsn("192.168.110.6",port=1521,sid="ORCL")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG11")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M11')
       
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
  my_dsn = cx_Oracle.makedsn("192.168.110.6",port=1521,sid="ORCL")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        print('Start Run Procedures PO RM BOM Soft M12')
        sendLine("Start  Run Procerdures  PO RM BOM Soft M12")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M12')
       
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

##BU GW######
class CLS_GET_WCS_GW_RM1(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_GW_RM1()
        
def GET_WCS_GW_RM1():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.87",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        print('Start Run  Procerdures get PO RM BOM Soft M01 ')
        sendLine("Start Procerdures get PO RM BOM Soft NYG1")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M01')      
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
class CLS_GET_WCS_GW_RM2(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_GW_RM2()
        
def GET_WCS_GW_RM2():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.87",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG2")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M02')
       
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
class CLS_GET_WCS_GW_RM3(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_GW_RM3()
        
def GET_WCS_GW_RM3():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.87",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG3")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M03')
       
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
class CLS_GET_WCS_GW_RM4(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_GW_RM4()
        
def GET_WCS_GW_RM4():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.87",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG4")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M04')
       
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
class CLS_GET_WCS_GW_RM5(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_GW_RM5()
        
def GET_WCS_GW_RM5():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.87",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG5")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M05')
       
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
class CLS_GET_WCS_GW_RM6(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_GW_RM6()
        
def GET_WCS_GW_RM6():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.87",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG6")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M06')
       
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
class CLS_GET_WCS_GW_RM7(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_GW_RM7()
        
def GET_WCS_GW_RM7():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.87",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG7")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M07')
       
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
class CLS_GET_WCS_GW_RM8(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_GW_RM8()
        
def GET_WCS_GW_RM8():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.87",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG8")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M08')
       
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
class CLS_GET_WCS_GW_RM9(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_GW_RM9()
        
def GET_WCS_GW_RM9():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.87",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG9")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M09')
       
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
class CLS_GET_WCS_GW_RM10(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_GW_RM10()
        
def GET_WCS_GW_RM10():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.87",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG10")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M10')
       
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
class CLS_GET_WCS_GW_RM11(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_GW_RM11()
        
def GET_WCS_GW_RM11():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.87",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG11")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M11')
       
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
class CLS_GET_WCS_GW_RM12(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_GW_RM12()
        
def GET_WCS_GW_RM12():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.87",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        print('Start Run Procedures PO RM BOM Soft M12')
        sendLine("Start  Run Procerdures  PO RM BOM Soft M12")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M12')
       
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


#############################################

## BU NYG ###
class CLS_GET_WCS_NYG_RM1(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYG_RM1()
        
def GET_WCS_NYG_RM1():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.82",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        print('Start Run  Procerdures get PO RM BOM Soft M01 ')
        sendLine("Start Procerdures get PO RM BOM Soft NYG1")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M01')      
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
class CLS_GET_WCS_NYG_RM2(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYG_RM2()
        
def GET_WCS_NYG_RM2():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.82",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG2")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M02')
       
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
class CLS_GET_WCS_NYG_RM3(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYG_RM3()
        
def GET_WCS_NYG_RM3():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.82",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG3")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M03')
       
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
class CLS_GET_WCS_NYG_RM4(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYG_RM4()
        
def GET_WCS_NYG_RM4():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.82",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG4")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M04')
       
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
class CLS_GET_WCS_NYG_RM5(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYG_RM5()
        
def GET_WCS_NYG_RM5():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.82",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG5")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M05')
       
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
class CLS_GET_WCS_NYG_RM6(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYG_RM6()
        
def GET_WCS_NYG_RM6():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.82",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG6")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M06')
       
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
class CLS_GET_WCS_NYG_RM7(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYG_RM7()
        
def GET_WCS_NYG_RM7():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.82",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG7")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M07')
       
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
class CLS_GET_WCS_NYG_RM8(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYG_RM8()
        
def GET_WCS_NYG_RM8():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.82",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG8")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M08')
       
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
class CLS_GET_WCS_NYG_RM9(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYG_RM9()
        
def GET_WCS_NYG_RM9():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.82",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG9")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M09')
       
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
class CLS_GET_WCS_NYG_RM10(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYG_RM10()
        
def GET_WCS_NYG_RM10():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.82",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG10")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M10')
       
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
class CLS_GET_WCS_NYG_RM11(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYG_RM11()
        
def GET_WCS_NYG_RM11():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.82",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG11")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M11')
       
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
class CLS_GET_WCS_NYG_RM12(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYG_RM12()
        
def GET_WCS_NYG_RM12():
 try:
  my_dsn = cx_Oracle.makedsn("172.16.6.82",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        print('Start Run Procedures PO RM BOM Soft M12')
        sendLine("Start  Run Procerdures  PO RM BOM Soft M12")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M12')
       
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


#############################################

## BU NYV ##
###########################################
class CLS_GET_WCS_NYV_RM1(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYV_RM1()
        
def GET_WCS_NYV_RM1():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        print('Start Run  Procerdures get PO RM BOM Soft M01 ')
        sendLine("Start Procerdures get PO RM BOM Soft NYG1")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M01')      
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
class CLS_GET_WCS_NYV_RM2(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYV_RM2()
        
def GET_WCS_NYV_RM2():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG2")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M02')
       
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
class CLS_GET_WCS_NYV_RM3(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYV_RM3()
        
def GET_WCS_NYV_RM3():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG3")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M03')
       
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
class CLS_GET_WCS_NYV_RM4(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYV_RM4()
        
def GET_WCS_NYV_RM4():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG4")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M04')
       
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
class CLS_GET_WCS_NYV_RM5(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYV_RM5()
        
def GET_WCS_NYV_RM5():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG5")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M05')
       
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
class CLS_GET_WCS_NYV_RM6(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYV_RM6()
        
def GET_WCS_NYV_RM6():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG6")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M06')
       
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
class CLS_GET_WCS_NYV_RM7(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYV_RM7()
        
def GET_WCS_NYV_RM7():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG7")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M07')
       
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
class CLS_GET_WCS_NYV_RM8(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYV_RM8()
        
def GET_WCS_NYV_RM8():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG8")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M08')
       
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
class CLS_GET_WCS_NYV_RM9(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYV_RM9()
        
def GET_WCS_NYV_RM9():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG9")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M09')
       
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
class CLS_GET_WCS_NYV_RM10(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYV_RM10()
        
def GET_WCS_NYV_RM10():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG10")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M10')
       
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
class CLS_GET_WCS_NYV_RM11(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYV_RM11()
        
def GET_WCS_NYV_RM11():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        sendLine("Start Procerdures get PO RM BOM Soft NYG11")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M11')
       
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
class CLS_GET_WCS_NYV_RM12(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        GET_WCS_NYV_RM12()
        
def GET_WCS_NYV_RM12():
 try:
  my_dsn = cx_Oracle.makedsn("192.168.101.34",port=1521,sid="vn")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn)
 except Exception  as err:
      print('Connection Database Soft NYG Error',err)
      sendLine("Connection Database Soft NYG Error",err)
 else:
     try:    
        cursor = conn.cursor()
        print('Start Run Procedures PO RM BOM Soft M12')
        sendLine("Start  Run Procerdures  PO RM BOM Soft M12")
        cursor.callproc('QVD_INS_WW_ALL_STEP_NYG_M12')
       
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


#############################################


threads = []
##TRM
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
##GW
thread13 = CLS_GET_WCS_GW_RM1();thread13.start();threads.append(thread13) 
thread14 = CLS_GET_WCS_GW_RM2();thread14.start();threads.append(thread14)
thread15 = CLS_GET_WCS_GW_RM3();thread15.start();threads.append(thread15) 
thread16 = CLS_GET_WCS_GW_RM4();thread16.start();threads.append(thread16)  
thread17 = CLS_GET_WCS_GW_RM5();thread17.start();threads.append(thread17) 
thread18 = CLS_GET_WCS_GW_RM6();thread18.start();threads.append(thread18) 
thread19 = CLS_GET_WCS_GW_RM7();thread19.start();threads.append(thread19) 
thread20 = CLS_GET_WCS_GW_RM8();thread20.start();threads.append(thread20) 
thread21 = CLS_GET_WCS_GW_RM9();thread21.start();threads.append(thread21) 
thread22 = CLS_GET_WCS_GW_RM10();thread22.start();threads.append(thread22) 
thread23 = CLS_GET_WCS_GW_RM11();thread23.start();threads.append(thread23) 
thread24 = CLS_GET_WCS_GW_RM12();thread24.start();threads.append(thread24) 

##NYG
thread25 = CLS_GET_WCS_NYG_RM1();thread25.start();threads.append(thread25) 
thread26 = CLS_GET_WCS_NYG_RM2();thread26.start();threads.append(thread26)
thread27 = CLS_GET_WCS_NYG_RM3();thread27.start();threads.append(thread27) 
thread28 = CLS_GET_WCS_NYG_RM4();thread28.start();threads.append(thread28)  
thread29 = CLS_GET_WCS_NYG_RM5();thread29.start();threads.append(thread29) 
thread30 = CLS_GET_WCS_NYG_RM6();thread30.start();threads.append(thread30) 
thread31 = CLS_GET_WCS_NYG_RM7();thread31.start();threads.append(thread31) 
thread32 = CLS_GET_WCS_NYG_RM8();thread32.start();threads.append(thread32) 
thread33 = CLS_GET_WCS_NYG_RM9();thread33.start();threads.append(thread33) 
thread34 = CLS_GET_WCS_NYG_RM10();thread34.start();threads.append(thread34) 
thread35 = CLS_GET_WCS_NYG_RM11();thread35.start();threads.append(thread35) 
thread36 = CLS_GET_WCS_NYG_RM12();thread36.start();threads.append(thread36) 

##NYV
thread37 = CLS_GET_WCS_NYV_RM1();thread37.start();threads.append(thread37) 
thread38 = CLS_GET_WCS_NYV_RM2();thread38.start();threads.append(thread38)
thread39 = CLS_GET_WCS_NYV_RM3();thread39.start();threads.append(thread39) 
thread40 = CLS_GET_WCS_NYV_RM4();thread40.start();threads.append(thread40)  
thread41 = CLS_GET_WCS_NYV_RM5();thread41.start();threads.append(thread41) 
thread42 = CLS_GET_WCS_NYV_RM6();thread42.start();threads.append(thread42) 
thread43 = CLS_GET_WCS_NYV_RM7();thread43.start();threads.append(thread43) 
thread44 = CLS_GET_WCS_NYV_RM8();thread44.start();threads.append(thread44) 
thread45 = CLS_GET_WCS_NYV_RM9();thread45.start();threads.append(thread45) 
thread46 = CLS_GET_WCS_NYV_RM10();thread46.start();threads.append(thread46) 
thread47 = CLS_GET_WCS_NYV_RM11();thread47.start();threads.append(thread47) 
thread48 = CLS_GET_WCS_NYV_RM12();thread48.start();threads.append(thread48) 

for t in threads:
    t.join()
print ("COMPLETE NOW")

