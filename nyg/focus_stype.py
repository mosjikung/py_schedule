import xlrd
import cx_Oracle
import os
# Give the location of the file

oracle_client = "C:\instantclient_19_5"
os.environ["ORACLE_HOME"] = oracle_client
os.environ["PATH"] = oracle_client+os.pathsep+os.environ["PATH"]
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.TH8TISASCII"


loc = (r"C:\IT_ONLY\FOCUS STYLE LIST.xlsx")

# To open Workbook

my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
conn = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
cursor = conn.cursor()
cursor.execute("""DELETE FROM CONTROL_STYLE_FOCUS""")
conn.commit()


wb = xlrd.open_workbook(loc)
sheet = wb.sheet_by_index(0)

for i in range(sheet.nrows):
    if (i!=0):
      # print(sheet.cell_value(i, 0))
      sql = """ INSERT INTO CONTROL_STYLE_FOCUS (STYLE_CODE) VALUES (UPPER('{}')) """.format(
          sheet.cell_value(i, 0))
      # print(sql)
      cursor.execute(sql)
      conn.commit()


conn.close()


# def CONTROL_WIP_READINESS(style_code):
#   my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
#   conn = cx_Oracle.connect(user="nygm", password="nygm",
#                            dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
#   cursor = conn.cursor()
#   cursor.execute("""DELETE FROM CONTROL_STYLE_FOCUS""")

#   conn.commit()

#   cursor.execute("""INSERT INTO CONTROL_STYLE_FOCUS ('{}') """.format(style_code))


#   conn.commit()
#   conn.close()
