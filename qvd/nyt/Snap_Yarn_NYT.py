import pymssql
import sys
import datetime as dt
from datetime import date, datetime, timedelta
import cx_Oracle
import os

server = "172.16.0.62"
user = "sa"
password = "P@ssw0rd"

oracle_client = "C:\instantclient_19_5"
os.environ["ORACLE_HOME"]=oracle_client
os.environ["PATH"]=oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"]="AMERICAN_AMERICA.TH8TISASCII"


def yarn_stock_aging():
    sql = """SELECT [SITE] SITE,[WHSE] WHSE,[UF_WDATE] UF_WDATE,[ITEM_GROUP] ITEM_GROUP
    ,[DATE_DIFF] DATE_DIFF,[PRODUCT_CODE] PRODUCT_CODE,[ITEM] ITEM,[ITEM_DESC] ITEM_DESC
    ,[LOT] LOT,[QTY_ON_HAND] QTY_ON_HAND,[STD_UNIT_COST] STD_UNIT_COST
    FROM NYT.dbo.V_FG_STOCK_AGING"""
    my_dsn = cx_Oracle.makedsn("172.16.6.74", port=1521,sid="NYTG")
    connOra = cx_Oracle.connect(user="NYIS", password="NYIS", dsn=my_dsn, encoding= "UTF-8", nencoding = "UTF-8")
    cursorOra = connOra.cursor()

    conn = pymssql.connect(server, user, password, "NYT")
    cursor = conn.cursor()
    cursor.execute(sql)
    for SITE,WHSE,UF_WDATE,ITEM_GROUP,DATE_DIFF,PRODUCT_CODE,ITEM,ITEM_DESC,LOT,QTY_ON_HAND,STD_UNIT_COST in cursor:
        print(SITE,WHSE,UF_WDATE,ITEM_GROUP,DATE_DIFF,PRODUCT_CODE,ITEM,ITEM_DESC,LOT,QTY_ON_HAND,STD_UNIT_COST)
        try:
            cursorOra.execute("""Insert into SNAP_YARN_NYT(FREEZE_YEAR,FREEZE_WEEK,FREEZE_DATE,SITE,WHSE,UF_WDATE,ITEM_GROUP,DATE_DIFF,PRODUCT_CODE,ITEM,ITEM_DESC,LOT,QTY_ON_HAND,STD_UNIT_COST)
            values(TO_CHAR(SYSDATE,'YYYY'),TO_CHAR(SYSDATE,'WW'),SYSDATE,:PSITE,:PWHSE,:PUF_WDATE,:PITEM_GROUP,:PDATE_DIFF,:PPRODUCT_CODE,:PITEM,:PITEM_DESC,:PLOT,:PQTY_ON_HAND,:PSTD_UNIT_COST)""",
            {'PSITE' :SITE,'PWHSE' :WHSE,'PUF_WDATE' :UF_WDATE,'PITEM_GROUP' :ITEM_GROUP,'PDATE_DIFF' :DATE_DIFF,'PPRODUCT_CODE' :PRODUCT_CODE,'PITEM' :ITEM,'PITEM_DESC' :ITEM_DESC,'PLOT' :LOT,'PQTY_ON_HAND' :QTY_ON_HAND,'PSTD_UNIT_COST' :STD_UNIT_COST})
            connOra.commit()
        except cx_Oracle.DatabaseError as e:
            None


yarn_stock_aging()