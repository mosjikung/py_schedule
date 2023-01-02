# call_Readiness.bat
import cx_Oracle
import csv
import os
from pathlib import Path
import requests
from datetime import datetime
import threading
import time
# import smtplib
# from email.message import EmailMessage
# from email.mime.multipart import MIMEMultipart
# from email.mime.text import MIMEText


oracle_client = "C:\instantclient_19_5"
os.environ["ORACLE_HOME"]=oracle_client
os.environ["PATH"]=oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"]="AMERICAN_AMERICA.TH8TISASCII"


time_start  = datetime.now() 
allTxt = ''
allHtml = ''
py_file = 'TEMP_FC_WEIGHT_KG.py'



class CLS_TEMP_FC_WEIGHT_KG(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        TEMP_FC_WEIGHT_KG()
        
def TEMP_FC_WEIGHT_KG():
  my_dsn = cx_Oracle.makedsn("172.16.6.80",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="CSERVICE", password="CSERVICE", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
#   cursor.execute("""DELETE FROM OE_SO_READINESS_PO_V_TMP2""")

#   conn.commit()

  cursor.execute("""SELECT M.* FROM PROJECT_TASK_STEP_ITEM M """)

  for row in cursor:
      print(row)


      

  conn.close()
  print('TEMP_FC_WEIGHT_KG')

TEMP_FC_WEIGHT_KG()