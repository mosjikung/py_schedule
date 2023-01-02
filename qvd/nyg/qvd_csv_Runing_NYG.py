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


  ###########################################

class CLS_Loss_Gain(threading.Thread):
      def __init__(self):
        threading.Thread.__init__(self)
      def run(self):
        Loss_Gain()
        
def Loss_Gain():
  my_dsn = cx_Oracle.makedsn("172.16.6.74",port=1521,sid="NYTG")
  conn = cx_Oracle.connect(user="SF5", password="OMSF5", dsn=my_dsn, encoding = "UTF-8", nencoding = "UTF-8")
  cursor = conn.cursor()
  cursor.execute("""  
SELECT  H.ORDER_NUMBER RS_NUMBER, H.ORA_ORDER_NUMBER SO_NO,C.R12_LINE_ID LINE_ID,
        H.DUMMY_ITEM_TYPE ORDER_TYPE,H.FLOW_STATUS_CODE,H.FOB_POINT_CODE FOB_CODE,
        H.ORDERED_DATE SO_NO_DATE,
        H.CUST_PO_NUMBER PO_NO, H.CUSTOMER_NUMBER CUSTOMER_ID,
        (SELECT CUSTOMER_NAME FROM DFORA_CUSTOMER C WHERE C.CUSTOMER_ID=H.CUSTOMER_NUMBER) CUSTOMER_NAME,
        H.SALESREP_NUMBER, (SELECT SALE_NAME FROM DFORA_SALE S WHERE S.SALE_ID=H.SALESREP_NUMBER) SALE_NAME,
        H.ATTRIBUTE2 END_BUYER,
        L.ORDERED_ITEM ITEM_CODE,
        (SELECT ITEM_DESC FROM FMIT_ITEM F WHERE F.ITEM_CODE=L.ORDERED_ITEM) ITEM_DESC,
        (SELECT ITEM_CATEGORY FROM FMIT_ITEM F WHERE F.ITEM_CODE=L.ORDERED_ITEM) ITEM_CATEGORY,
        L.O_ITEM_REFERENCE,L.TUBULAR_TYPE, L.FABRIC_WIDE, L.FABRIC_GM2,L.O_YARN_COUNT,
        C.COMM_PLCOLOR COLOR_CODE,(SELECT COLOR_DESC FROM DFORA_COLOR CL WHERE CL.COLOR_CODE=C.COMM_PLCOLOR) COLOR_DESC,
        (SELECT PL_COLORLAB FROM SMIT_SO_LINE L WHERE L.SO_NO=H.ORA_ORDER_NUMBER AND L.LINE_ID=C.R12_LINE_ID) SO_COLOR,
        C.ORDER_CUSTOMER CUSTOMER_ORDER,
        C.ORDER_PROD_QTY ,--PRODUCTION_QTY_OLD,
        C.ORDER_PROD_ROLL ,--PRODUCTION_ROLL_OLD,
        C.WKPER_LOSS PERCENT_LOSS,
        C.RD_COLOR
FROM DUMMY_SO_HEADERS H, DUMMY_SO_LINES L, DUMMY_COLOR_LINES C
WHERE H.ORDER_NUMBER=L.ORDER_NUMBER
AND L.ORDER_NUMBER=C.ORDER_NUMBER
AND L.LINE_NUMBER=C.LINE_NUMBER
AND H.FLOW_STATUS_CODE<>'CANCEL'  
AND NVL(H.CLOSE_FLAG,'N')<>'C'
""")
  
  _csv = r"C:\QVD_DATA\COM_GARMENT\ALLBU\Loss_Gain.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers
    csv_writer.writerows(cursor)
  conn.close()





threads = []

thread1 = CLS_Loss_Gain();thread1.start();threads.append(thread1)






for t in threads:
    t.join()
print ("COMPLETE")

