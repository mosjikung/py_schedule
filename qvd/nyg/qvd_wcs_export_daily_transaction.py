import cx_Oracle
import csv
import os
from pathlib import Path
import requests
from datetime import datetime
import threading
import time


oracle_client = "C:\instantclient_19_5"
os.environ["ORACLE_HOME"]=oracle_client
os.environ["PATH"]=oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"]="AMERICAN_AMERICA.TH8TISASCII"



########################################### WCS_DAILY_EXPORT_ALLBU k'nok sirada

class CLS_WCS_DAILY_EXPORT_ALLBU(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        WCS_DAILY_EXPORT_ALLBU()
        
def WCS_DAILY_EXPORT_ALLBU():
  my_dsn = cx_Oracle.makedsn("172.16.6.82",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="nyg1", password="nyg1", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  args = ['G1','G1WCSSPO0002','WCS_CLOSE_SOPRO_ALLBU.csv']
  result_args = cursor.callproc('QVD_RUN_INS_LOG', args)
  print("sart  V_QVD_DAILY_EXPORT_ALLBU")
  cursor.execute("""
        select *
        from V_QVD_DAILY_EXPORT_ALLBU
  """)
  
  _csv = r"C:\QVD_DATA\COM_GARMENT\NYG\DAILY_EXPORT_ALLBU.csv"

  with open(_csv, "w", newline='', encoding='utf-8-sig') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  args = ['G1','G1WCSSPO0002',cursor.rowcount]
  result_args = cursor.callproc('QVD_RUN_upd_LOG', args)
  conn.close()
  print("WCS_DAILY_EXPORT_ALLBU Done")  
#############################################
threads = []

thread5 = CLS_WCS_DAILY_EXPORT_ALLBU();thread5.start();threads.append(thread5)



for t in threads:
    t.join()
print ("COMPLETE")

