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


def purchase(uid):
  my_dsn = cx_Oracle.makedsn("172.16.6.80", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="webcontrol", password="webcontrol",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  # cursor.execute("""DELETE FROM CTL_USER_READINESS_NYG WHERE USER_ID = '{uid}' """.format(uid=uid))
  # conn.commit()

  try:
    cursor.execute(
      """INSERT INTO CTL_USER_SYSTEM (USER_ID, SYSCODE, ROLE_ID, USER_LEVEL, ACTIVE) VALUES ('{uid}','CTRNYG','USER',1,'Y')""".format(uid=uid))
    conn.commit()
  except:
    print('Error Line 29')

  try:
    cursor.execute("""INSERT INTO CTL_USER_READINESS_NYG (USER_ID, READINESS, LISTTYPE, RNK) VALUES ('{uid}','ALL','GroupFAB',1)""".format(uid=uid))
    conn.commit()
  except:
    print('Error Line 33')

  try:
    cursor.execute("""INSERT INTO CTL_USER_READINESS_NYG (USER_ID, READINESS, LISTTYPE, RNK) VALUES ('{uid}','NIKE','GroupFAB',2)""".format(uid=uid))
    conn.commit()
  except:
    print('Error Line 39')

  try:
    cursor.execute("""INSERT INTO CTL_USER_READINESS_NYG (USER_ID, READINESS, LISTTYPE, RNK) VALUES ('{uid}','PROCUREMENT','GroupFAB',3)""".format(uid=uid))
    conn.commit()
  except:
    print('Error Line 45')

  try:
    cursor.execute("""INSERT INTO CTL_USER_READINESS_NYG (USER_ID, READINESS, LISTTYPE, RNK) VALUES ('{uid}','SCM VI','GroupFAB',4)""".format(uid=uid))
    conn.commit()
  except:
    print('Error Line 51')

  try:
    cursor.execute("""INSERT INTO CTL_USER_READINESS_NYG (USER_ID, READINESS, LISTTYPE, RNK) VALUES ('{uid}','PROCUREMENT','GroupPACK',2)""".format(uid=uid))
    conn.commit()
  except:
    print('Error Line 57')

  try:
    cursor.execute("""INSERT INTO CTL_USER_READINESS_NYG (USER_ID, READINESS, LISTTYPE, RNK) VALUES ('{uid}','PROCUREMENT','GroupSEW',2)""".format(uid=uid))
    conn.commit()
  except:
    print('Error Line 63')

  try:
    cursor.execute("""INSERT INTO CTL_USER_READINESS_NYG (USER_ID, READINESS, LISTTYPE, RNK) VALUES ('{uid}','FAB','Process',3)""".format(uid=uid))
    conn.commit()
  except:
    print('Error Line 69')

  try:
    cursor.execute("""INSERT INTO CTL_USER_READINESS_NYG (USER_ID, READINESS, LISTTYPE, RNK) VALUES ('{uid}','Pack','Process',5)""".format(uid=uid))
    conn.commit()
  except:
    print('Error Line 75')

  try:
    cursor.execute("""INSERT INTO CTL_USER_READINESS_NYG (USER_ID, READINESS, LISTTYPE, RNK) VALUES ('{uid}','Sew','Process',4)""".format(uid=uid))
    conn.commit()
  except:
    print('Error Line 81')

  try:
    cursor.execute("""INSERT INTO CTL_USER_READINESS_NYG (USER_ID, READINESS, LISTTYPE, RNK) VALUES ('{uid}','Sourcing/SCM-VI','Response',3)""".format(uid=uid))
    conn.commit()
  except:
    print('Error Line 87')

  try:
    cursor.execute("""INSERT INTO CTL_USER_READINESS_NYG (USER_ID, READINESS, LISTTYPE, RNK) VALUES ('{uid}','Sourcing','Response',2)""".format(uid=uid))
    conn.commit()
  except:
    print('Error Line 93')

  try:
    cursor.execute("""INSERT INTO CTL_USER_READINESS_NYG (USER_ID, READINESS, LISTTYPE, RNK) VALUES ('{uid}','NIKE','GroupPACK',2)""".format(uid=uid))
    conn.commit()
  except:
    print('Error Line 102')

  try:
    cursor.execute("""INSERT INTO CTL_USER_READINESS_NYG (USER_ID, READINESS, LISTTYPE, RNK) VALUES ('{uid}','NIKE','GroupSEW',2)""".format(uid=uid))
    conn.commit()
  except:
    print('Error Line 108')

def getUID(mailid):
  my_dsn = cx_Oracle.makedsn("172.16.6.80", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="webcontrol", password="webcontrol",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()
  sql = """ SELECT USER_ID FROM CTL_USER WHERE USER_ACTIVE = 'Y' AND (USER_MAIL = '{}' OR APPROVE5 = '{}') """.format(mailid, mailid);

  dfData = pd.read_sql_query(sql, conn)

  for index, row in dfData.iterrows():
    # print(row['USER_ID'],mailid)
    return row['USER_ID']


def aa():
  my_dsn = cx_Oracle.makedsn("172.16.6.80", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="webcontrol", password="webcontrol",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  sql = """ SELECT DISTINCT USER_ID
            FROM CTL_USER_READINESS_NYG
            where readiness in ('ALL','NIKE','PROCUREMENT') and listtype in ('GroupFAB','GroupPACK','GroupSEW') and  listtype not in ('Group')
            ORDER BY 1 """

  dfData = pd.read_sql_query(sql, conn)

  for index, row in dfData.iterrows():
    print(row['USER_ID'])
    purchase(row['USER_ID'])

aa()

# mails = ['vorakarn.p@nanyangtextile.com',
#          'kanteera.m@nanyangtextile.com',
#          'uraiporn.m@nanyangtextile.com',
#          'pitsinee.p@nanyangtextile.com',
#          'pattamaporn.k@nanyangtextile.com',
#          'rattanakorn.p@nanyangtextile.com',
#          'rapeepan.s@nanyangtextile.com',
#          'nion.i@nanyangtextile.com',
#          'chamaiporn.m@nanyangtextile.com',
#          'thiranuch.k@nanyangtextile.com',
#          'ny.nguyen@nanyangtextile.com',
#          'hanna.tran@nanyangtextile.com',
#          'atchariya.m@nanyangtextile.com',
#          'thanyarat.j@nanyangtextile.com']
# for i in mails:
#   uid = getUID(i)
#   if (uid!=None):
#     print(uid)
#     purchase(uid)

