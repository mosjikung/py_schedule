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
os.environ["ORACLE_HOME"] = oracle_client
os.environ["PATH"] = oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.TH8TISASCII"


def SALE_ORDER_OE():
  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="DEMO", password="DEMO",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  sql = """ SELECT  s.CUSTOMER_YEAR||lpad(s.CUSTOMER_FG,2,'0') SO_FG_WEEK, a.ou_code,a.batch_no, c.total_roll,c.TOTAL_QTY,c.unit_qty,
         c.start_date ,c.end_date,
         c.step_no ,GET_STEP_NAME(c.step_no) step_name,c.machine_no,
         Null machine_group,Null machine_group_name,Null machine_type,Null machine_type_name,
         0 cap_time_prod,a.process_line,c.std_use_hr std_dye_use_hr,
         a.so_no,a.line_id,a.customer_id,a.customer_name,a.sale_id,a.sale_name,a.color_code,a.color_desc,a.color_shade,
         a.item_code,a.item_desc,a.schedule_id,a.po_no,a.buyyer,
         decode(a.job_type,'N','NORMAL','D','INTERNAL','R','EXTERNAL') job_type,
         decode(a.tubular_type,'1','อบกลม','อบผ่า') tubular_type,a.width,a.weight_g,a.weight_y,a.job_no,A.ITEM_PROCESS
     FROM DFIT_BTDATA a,
          DFBT_MONITOR c,smit_so_header s
     WHERE a.so_no=s.so_no
     and c.start_date is not null
     and c.end_date is not null
     and a.status IN ('2','8')
     AND a.ou_code=c.OU_CODE
     AND a.batch_no=c.batch_no
     --ND c.method_cont IN ('1','2','4','6','7')
     AND c.active_flag='N'
     And c.machine_no Is Null
     AND to_number(to_char(trunc(c.end_date),'YYYY')) = 2020 """

  df = pd.read_sql_query(sql, conn)

  # df.to_excel(r'c:\qvd_data\pro_nyk\BATCH_STEP_ALL_2020.xlsx', index=False)
  df.to_csv(r'c:\qvd_data\pro_nyk\BATCH_STEP_ALL_2020.csv', index=False)



SALE_ORDER_OE()
