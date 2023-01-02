import cx_Oracle
import csv
import os
from pathlib import Path
import requests
from datetime import datetime
import threading
import time
import numpy as np
import pandas as pd

oracle_client = "C:\instantclient_19_5"
os.environ["ORACLE_HOME"] = oracle_client
os.environ["PATH"] = oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.TH8TISASCII"


def insertLine(loc, line, rnk):
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  cursor.execute("""INSERT INTO CONTROL_BOARD_WIP (LOC, LINE, RNK)
                    VALUES ('{loc}', '{line}', {rnk})  """.format(loc=loc, line=line,rnk=rnk))

  conn.commit()

  conn.close()

###########################################

# Start G1
class CLS_upd_g1(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        upd_g1()


def upd_g1():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  cursor = conn.cursor()

  df = pd.read_sql_query(
      "SELECT LINE FROM CONTROL_BOARD_WIP WHERE LOC = 'G1' ", conn)

  for index, row in df.iterrows():
    sql = """ UPDATE CONTROL_BOARD_WIP M SET UPD_DATE = SYSDATE
              ,SM2 = NVL((SELECT D.QTY FROM NYG1.WIP_SM2_SUMMARY@WCS.WORLD D WHERE D.MC_LINE = M.LINE),0)
              ,SM3 = NVL((SELECT D.QTY FROM NYG1.WIP_SM3_SUMMARY@WCS.WORLD D WHERE D.MC_LINE = M.LINE),0)
             ,WIP_SEW = NVL(NYG1.get_wip_sew@WCS.WORLD(M.LINE),0)
              ,QC = NVL(( SELECT B.QC_QTY FROM NYG1.VB_QC_OUTPUT_V@WCS.WORLD B WHERE B.PROD_LINE = M.LINE),0)
              ,FN = ( SELECT NVL(SUM(NVL(C.FN_QTY,0)),0)
                    FROM NYG1.VB_FN_INPUT_V@WCS.WORLD C, NYG1.VB_PRODUCTION_ON_LINE@WCS.WORLD A
                    WHERE C.PROD_LINE = A.PROD_LINE
                    AND C.PROD_LINE = M.LINE)
              ,WAIT = (SELECT D.REQ_HR FROM NYG1.INFO_BOARD_REQ_Q@WCS.WORLD D WHERE D.LINE_CODE = M.LINE)
              ,WAIT_MIN = NVL((SELECT round(D.REQ_MIN,1) FROM NYG1.INFO_BOARD_REQ_Q@WCS.WORLD D WHERE D.LINE_CODE = M.LINE),0)
              WHERE M.LINE = '{line}'
              AND LOC = 'G1'   """.format(line=row['LINE'])
    cursor.execute(sql)
    conn.commit()

  conn.close()
  print('Update WIP Control Board for G1 Complete')

# End G1

# Start G2
class CLS_upd_g2(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
    def run(self):
        upd_g2()

def upd_g2():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  cursor = conn.cursor()

  df = pd.read_sql_query("SELECT LINE FROM CONTROL_BOARD_WIP WHERE LOC = 'G2' ", conn)

  for index, row in df.iterrows():
    sql = """ UPDATE CONTROL_BOARD_WIP M SET UPD_DATE = SYSDATE
              ,SM2 = NVL((SELECT D.QTY FROM NYG_PHO.WIP_SM2_SUMMARY@NYG2_170.WORLD D WHERE D.MC_LINE = M.LINE),0)
              ,SM3 = NVL((SELECT D.QTY FROM NYG_PHO.WIP_SM3_SUMMARY@NYG2_170.WORLD D WHERE D.MC_LINE = M.LINE),0)
              ,WIP_SEW = NVL(NYG_PHO.get_wip_sew@NYG2_170.WORLD(M.LINE),0)
              ,QC = NVL(( SELECT B.QC_QTY FROM NYG_PHO.VB_QC_OUTPUT_V@NYG2_170.WORLD B WHERE B.PROD_LINE = M.LINE),0)
              ,FN = ( SELECT NVL(SUM(NVL(C.FN_QTY,0)),0)
                    FROM NYG_PHO.VB_FN_INPUT_V@NYG2_170.WORLD C, NYG_PHO.VB_PRODUCTION_ON_LINE@NYG2_170.WORLD A
                    WHERE C.PROD_LINE = A.PROD_LINE
                    AND C.PROD_LINE = M.LINE)
              ,WAIT = (SELECT D.REQ_HR FROM NYG_PHO.INFO_BOARD_REQ_Q@NYG2_170.WORLD D WHERE D.LINE_CODE = M.LINE)
              ,WAIT_MIN = NVL((SELECT round(D.REQ_MIN,1) FROM NYG_PHO.INFO_BOARD_REQ_Q@NYG2_170.WORLD D WHERE D.LINE_CODE = M.LINE),0)
              WHERE M.LINE = '{line}'
              AND LOC = 'G2' """.format(line=row['LINE'])
    cursor.execute(sql)
    conn.commit()

  conn.close()
  print('Update WIP Control Board for G2 Complete')

# End G2

# Start G3
class CLS_upd_g3(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        upd_g3()


def upd_g3():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  cursor = conn.cursor()

  df = pd.read_sql_query(
      "SELECT LINE FROM CONTROL_BOARD_WIP WHERE LOC = 'G3' ", conn)

  for index, row in df.iterrows():
    sql = """ UPDATE CONTROL_BOARD_WIP M SET UPD_DATE = SYSDATE
              ,SM2 = NVL((SELECT D.QTY FROM NYG3.WIP_SM2_SUMMARY@WCS.WORLD D WHERE D.MC_LINE = M.LINE),0)
              ,SM3 = NVL((SELECT D.QTY FROM NYG3.WIP_SM3_SUMMARY@WCS.WORLD D WHERE D.MC_LINE = M.LINE),0)
             ,WIP_SEW = NVL(NYG3.get_wip_sew@WCS.WORLD(M.LINE),0)
              ,QC = NVL(( SELECT B.QC_QTY FROM NYG3.VB_QC_OUTPUT_V@WCS.WORLD B WHERE B.PROD_LINE = M.LINE),0)
              ,FN = ( SELECT NVL(SUM(NVL(C.FN_QTY,0)),0)
                    FROM NYG3.VB_FN_INPUT_V@WCS.WORLD C, NYG3.VB_PRODUCTION_ON_LINE@WCS.WORLD A
                    WHERE C.PROD_LINE = A.PROD_LINE
                    AND C.PROD_LINE = M.LINE)
              ,WAIT = (SELECT D.REQ_HR FROM NYG3.INFO_BOARD_REQ_Q@WCS.WORLD D WHERE D.LINE_CODE = M.LINE)
              ,WAIT_MIN = NVL((SELECT round(D.REQ_MIN,1) FROM NYG3.INFO_BOARD_REQ_Q@WCS.WORLD D WHERE D.LINE_CODE = M.LINE),0)
              WHERE M.LINE = '{line}'
              AND LOC = 'G3'  """.format(line=row['LINE'])
    cursor.execute(sql)
    conn.commit()

  conn.close()
  print('Update WIP Control Board for G3 Complete')
# End G3

# Start G4
class CLS_upd_g4(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        upd_g4()

def upd_g4():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  cursor = conn.cursor()

  df = pd.read_sql_query(
      "SELECT LINE FROM CONTROL_BOARD_WIP WHERE LOC = 'G4' ", conn)

  for index, row in df.iterrows():
    sql = """ UPDATE CONTROL_BOARD_WIP M SET UPD_DATE = SYSDATE
              ,SM2 = NVL((SELECT D.QTY FROM NYG4.WIP_SM2_SUMMARY@NYG4.WORLD D WHERE D.MC_LINE = M.LINE),0)
              ,SM3 = NVL((SELECT D.QTY FROM NYG4.WIP_SM3_SUMMARY@NYG4.WORLD D WHERE D.MC_LINE = M.LINE),0)
              ,WIP_SEW = NVL(NYG4.get_wip_sew@NYG4.WORLD(M.LINE),0)
              ,QC = NVL(( SELECT B.QC_QTY FROM NYG4.VB_QC_OUTPUT_V@NYG4.WORLD B WHERE B.PROD_LINE = M.LINE),0)
              ,FN = ( SELECT NVL(SUM(NVL(C.FN_QTY,0)),0)
                    FROM NYG4.VB_FN_INPUT_V@NYG4.WORLD C, NYG4.VB_PRODUCTION_ON_LINE@NYG4.WORLD A
                    WHERE C.PROD_LINE = A.PROD_LINE
                    AND C.PROD_LINE = M.LINE)
              ,WAIT = (SELECT D.REQ_HR FROM NYG4.INFO_BOARD_REQ_Q@NYG4.WORLD D WHERE D.LINE_CODE = M.LINE)
              ,WAIT_MIN = NVL((SELECT round(D.REQ_MIN,1) FROM NYG4.INFO_BOARD_REQ_Q@NYG4.WORLD D WHERE D.LINE_CODE = M.LINE),0)
              WHERE M.LINE = '{line}'
              AND LOC = 'G4' """.format(line=row['LINE'])
    cursor.execute(sql)
    conn.commit()

  conn.close()
  print('Update WIP Control Board for G4 Complete')

# End G4

# Start G5
class CLS_upd_g5(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        upd_g5()

def upd_g5():
  my_dsn = cx_Oracle.makedsn("192.168.110.6", port=1521, sid="ORCL")
  conn = cx_Oracle.connect(user="trm", password="trm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  cursor = conn.cursor()

  sql = """ begin AUTO_UPD_WIP_CTRL_BOARD_TRM; end; """
  cursor.execute(sql)
  # conn.commit()
  conn.close()

  print('Update WIP Control Board for G5 Complete')

# End G5

# Start G6
class CLS_upd_g6(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        upd_g6()

def upd_g6():
  my_dsn = cx_Oracle.makedsn("192.168.101.34", port=1521, sid="vn")
  conn = cx_Oracle.connect(user="vn", password="vn",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")

  cursor = conn.cursor()

  sql = """ begin AUTO_UPD_WIP_CTRL_BOARD_NYV; end; """
  cursor.execute(sql)
  # conn.commit()
  conn.close()

  print('Update WIP Control Board for G6 Complete')

# End G6


# upd_g2()
# upd_g4()

threads = []

thread0 = CLS_upd_g1()
thread0.start()
threads.append(thread0)

thread1 = CLS_upd_g2()
thread1.start()
threads.append(thread1)

thread2 = CLS_upd_g3()
thread2.start()
threads.append(thread2)

thread3 = CLS_upd_g4()
thread3.start()
threads.append(thread3)

thread4 = CLS_upd_g5()
thread4.start()
threads.append(thread4)

thread5 = CLS_upd_g6()
thread5.start()
threads.append(thread5)



for t in threads:
    t.join()


# for x in range(1, 39):
#   insertLine('G4', '{:03d}'.format(x), x)
 
