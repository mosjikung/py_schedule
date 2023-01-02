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

my_db = cx_Oracle.makedsn("172.16.6.80", port=1521, sid="NYTG")
my_db_user = 'MISOS'
my_db_pwd = 'MISOS'




def WR_V_HEADER():

  conn = cx_Oracle.connect(user=my_db_user, password=my_db_pwd,
                           dsn=my_db, encoding="UTF-8", nencoding="UTF-8")

  sql = """ SELECT M.* FROM WR_V_HEADER M  """

  df = pd.read_sql_query(sql, conn)

  df.to_excel(r'C:\QVD_DATA\PRO_NYK\WR_V_HEADER.xlsx', engine='xlsxwriter', index=False)


def WR_V_DETAIL():
    
  conn = cx_Oracle.connect(user=my_db_user, password=my_db_pwd,
                           dsn=my_db, encoding="UTF-8", nencoding="UTF-8")

  sql = """ SELECT D.* FROM WR_V_DETAIL D """

  df = pd.read_sql_query(sql, conn)

  df.to_excel(r'C:\QVD_DATA\PRO_NYK\WR_V_DETAIL.xlsx', engine='xlsxwriter', index=False)


WR_V_HEADER()
WR_V_DETAIL()

