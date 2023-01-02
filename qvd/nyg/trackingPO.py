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


class CLS_TRACKING_PO_SUB(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      TRACKING_PO_SUB()


def TRACKING_PO_SUB():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  sql = """SELECT S.SO_YEAR || '-' || S.SO_NO|| '-' || S.SUB_NO|| '-' || S.CUS_PO_NO AS PO_ITEM_KEY0,
S.OU_CODE, 
S.SHIP_DATE SHIP_DATE_SUB
from OE_SO_SUB_GAC_V S
WHERE S.OU_CODE = 'N03'
AND S.SO_YEAR >= 18 AND S.SO_YEAR < 99 """

  cursor.execute(sql)

  _csv = r"C:\QVDatacenter\SCM\GARMENT\NYG\TRACKING_PO_SUB.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)

  conn.close()
  print("COMPLETE TRACKING_PO_SUB")


class CLS_TRACKING_PO_ITEM(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      TRACKING_PO_ITEM()


def TRACKING_PO_ITEM():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  sql = """SELECT A.SO_YEAR || '-' || A.SO_NO|| '-' || A.SUB_NO|| '-' || A.CUS_PO_NO|| '-' || A.COL_CODE AS PO_ITEM_KEY, A.ITEM_NO 
FROM OE_SO_GAC_H A 
WHERE A.SO_YEAR >= 18  AND A.SO_YEAR < 99 """

  cursor.execute(sql)

  _csv = r"C:\QVDatacenter\SCM\GARMENT\NYG\TRACKING_PO_ITEM.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)

  conn.close()
  print("COMPLETE TRACKING_PO_ITEM")



class CLS_TRACKING_PO_HEADER_G1(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      TRACKING_PO_HEADER_G1()

def TRACKING_PO_HEADER_G1():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  sql = """select 'NYG1' BU,a.cus_po_no,a.SO_NO, a.SO_YEAR, a.SUB_NO, a.SO_NO_DOC, a.STYLE_REF, a.SHIP_DATE, a.CUST_CODE, a.COL_FC, a.SIZE_FC, a.QTY,
(select nvl(sum(b.cut_qty),0) from nyg1.bb_wip_cut_1@wcs.world b where a.so_no = b.so_no and a.so_year = b.so_year and a.sub_no = b.SUB and a.COL_FC = b.COLOR_FC and a.SIZE_FC = b.SIZE_FC) cut_qty,
(select nvl(sum(c.WW_IN_QTY),0) from nyg1.dfit_ep_detail@wcs.world c where a.SO_NO = c.SO_NO and a.SO_YEAR = c.SO_YEAR and a.SUB_NO = c.SUB and a.COL_FC = c.SO_COLOR and a.SIZE_FC = c.SO_SIZE and c.WW_STEP_NAME = 'Z' and c.WW_OUT_DATE is not null) sm2_qty,
(select nvl(sum(c11.WW_OUT_QTY),0) from nyg1.dfit_ep_detail@wcs.world c11 where a.SO_NO = c11.SO_NO and a.SO_YEAR = c11.SO_YEAR and a.SUB_NO = c11.SUB and a.COL_FC = c11.SO_COLOR and a.SIZE_FC = c11.SO_SIZE and c11.WW_STEP_NAME = 'P' and c11.WW_OUT_DATE is not null) pout_qty,
(select nvl(sum(c1.WW_IN_QTY),0) from nyg1.dfit_ep_detail@wcs.world c1 where a.SO_NO = c1.SO_NO and a.SO_YEAR = c1.SO_YEAR and a.SUB_NO = c1.SUB and a.COL_FC = c1.SO_COLOR and a.SIZE_FC = c1.SO_SIZE and c1.WW_STEP_NAME = 'P' and c1.WW_IN_DATE is not null) pin_qty,
(select nvl(sum(c21.WW_OUT_QTY),0) from nyg1.dfit_ep_detail@wcs.world c21 where a.SO_NO = c21.SO_NO and a.SO_YEAR = c21.SO_YEAR and a.SUB_NO = c21.SUB and a.COL_FC = c21.SO_COLOR and a.SIZE_FC = c21.SO_SIZE and c21.WW_STEP_NAME = 'E' and c21.WW_OUT_DATE is not null) eout_qty,
(select nvl(sum(c2.WW_IN_QTY),0) from nyg1.dfit_ep_detail@wcs.world c2 where a.SO_NO = c2.SO_NO and a.SO_YEAR = c2.SO_YEAR and a.SUB_NO = c2.SUB and a.COL_FC = c2.SO_COLOR and a.SIZE_FC = c2.SO_SIZE and c2.WW_STEP_NAME = 'E' and c2.WW_IN_DATE is not null) ein_qty,
(select nvl(sum(c31.WW_OUT_QTY),0) from nyg1.dfit_ep_detail@wcs.world c31 where a.SO_NO = c31.SO_NO and a.SO_YEAR = c31.SO_YEAR and a.SUB_NO = c31.SUB and a.COL_FC = c31.SO_COLOR and a.SIZE_FC = c31.SO_SIZE and c31.WW_STEP_NAME = 'H' and c31.WW_OUT_DATE is not null) hout_qty,
(select nvl(sum(c3.WW_IN_QTY),0) from nyg1.dfit_ep_detail@wcs.world c3 where a.SO_NO = c3.SO_NO and a.SO_YEAR = c3.SO_YEAR and a.SUB_NO = c3.SUB and a.COL_FC = c3.SO_COLOR and a.SIZE_FC = c3.SO_SIZE and c3.WW_STEP_NAME = 'H' and c3.WW_IN_DATE is not null) hin_qty,
(select nvl(sum(c31.WW_OUT_QTY),0) from nyg1.dfit_ep_detail@wcs.world c31 where a.SO_NO = c31.SO_NO and a.SO_YEAR = c31.SO_YEAR and a.SUB_NO = c31.SUB and a.COL_FC = c31.SO_COLOR and a.SIZE_FC = c31.SO_SIZE and c31.WW_STEP_NAME = 'B' and c31.WW_OUT_DATE is not null) Bout_qty,
(select nvl(sum(c3.WW_IN_QTY),0) from nyg1.dfit_ep_detail@wcs.world c3 where a.SO_NO = c3.SO_NO and a.SO_YEAR = c3.SO_YEAR and a.SUB_NO = c3.SUB and a.COL_FC = c3.SO_COLOR and a.SIZE_FC = c3.SO_SIZE and c3.WW_STEP_NAME = 'B' and c3.WW_IN_DATE is not null) Bin_qty,
(select nvl(sum(d.WW_IN_QTY),0) from nyg1.dfit_ep_detail@wcs.world d where a.SO_NO = d.SO_NO and a.SO_YEAR = d.SO_YEAR and a.SUB_NO = d.SUB and a.COL_FC = d.SO_COLOR and a.SIZE_FC = d.SO_SIZE and d.WW_STEP_NAME = 'M' and d.WW_OUT_DATE is not null) sm3_qty,
(select nvl(sum(e.WW_IN_QTY),0) from nyg1.dfit_ep_detail@wcs.world e where a.SO_NO = e.SO_NO and a.SO_YEAR = e.SO_YEAR and a.SUB_NO = e.SUB and a.COL_FC = e.SO_COLOR and a.SIZE_FC = e.SO_SIZE and e.WW_STEP_NAME = 'S' and e.WW_OUT_DATE is not null) sm4_qty,
(select nvl(sum(f.QTY),0) from nyg1.dfit_bb_loading@wcs.world f where a.SO_NO = f.SO_NO and a.so_year = f.SO_YEAR and a.SUB_NO = f.SUB_NO and a.COL_FC = f.COLOR_FC and a.SIZE_FC = f.SIZE_FC) load_qty,
(select nvl(sum(m.wait_fn_qty),0) from nyg1.dfit_bb_loading@wcs.world m where a.SO_NO = m.SO_NO and a.SO_YEAR = m.SO_YEAR and a.SUB_NO = m.SUB_NO and a.COL_FC = m.COLOR_FC and a.SIZE_FC = m.SIZE_FC and m.WAIT_FN = 'Y') qc_qty,
(select nvl(sum(n.QTY),0) from nyg1.dfit_bb_defect@wcs.world n where a.SO_NO = n.SO_NO and a.SO_YEAR = n.so_year and a.SUB_NO = n.SUB_NO and a.COL_FC = n.COLOR_FC and a.SIZE_FC = n.SIZE_FC and n.REASON_CODE not in ('S001', 'S002', 'S003', 'S005', 'S006', 'S099') and n.DEFECT_TYPE = 'S') qc_minus,
(select nvl(sum(g.BUNDLE_QTY),0) from nyg1.dfit_bb_process@wcs.world g where a.SO_NO = g.SO_NO and a.so_year = g.SO_YEAR and a.SUB_NO = g.SUB_NO and a.COL_FC = g.COLOR_FC and a.SIZE_FC = g.BUNDLE_SIZE) fn_qty,
(select nvl(sum(h.QTY),0) from nyg1.dfit_bb_defect@wcs.world h where a.SO_NO = h.SO_NO and a.SO_YEAR = h.so_year and a.SUB_NO = h.SUB_NO and a.COL_FC = h.COLOR_FC and a.SIZE_FC = h.SIZE_FC and h.REASON_CODE not in ('S001', 'S002', 'S003', 'S005', 'S006', 'S099')) defect_qty,
(select nvl(sum(i.QTY),0) from nyg1.oe_pre_pack_assort@wcs.world i where a.so_no = i.so_no and a.SO_year = i.so_year and a.sub_no = i.SUB_NO and a.COL_FC = i.COL_FC and a.SIZE_FC = i.SIZE_FC and i.ACTUAL_WEIGHT is not null ) scan_pack,
(select nvl(sum(j.PCS_QTY),0) from nyg1.wip_fg_by_sub@wcs.world j where a.SO_NO = j.SO_NO and a.so_year = j.SO_YEAR and a.SUB_NO = j.SUB_NO and a.COL_FC = j.COLOR_FC and a.SIZE_FC = j.SIZE_FC and j.DOC_TYPE not in ('A GRADE', 'B GRADE', 'SHIPMENT', 'SCRAP')) fg_qty,
(select nvl(sum(k.PCS_QTY),0) from nyg1.wip_fg_by_sub@wcs.world k where a.SO_NO = k.SO_NO and a.so_year = k.SO_YEAR and a.SUB_NO = k.SUB_NO and a.COL_FC = k.COLOR_FC and a.SIZE_FC = k.SIZE_FC and k.issue_type not in ('EXPORT', 'SCRAP', 'ADJUST') ) fg_minus,
(select nvl(sum(l.PCS_QTY),0) from nyg1.dfit_fg_issue_detail@wcs.world l where a.SO_NO = l.SO_NO and a.SO_YEAR = l.SO_YEAR and a.SUB_NO = l.SUB_NO and a.COL_FC = l.COLOR_FC and a.SIZE_FC = l.SIZE_FC and l.ISSUE_TYPE in ('EXPORT')) export_qty
from nyg1.oe_so_sub_detail_v@wcs.world a where a.so_year>18 AND a.SO_YEAR < 99 """

  cursor.execute(sql)

  _csv = r"C:\QVDatacenter\SCM\GARMENT\NYG\TRACKING_PO_HEADER_G1.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)

  conn.close()
  print("COMPLETE TRACKING_PO_HEADER_G1")


class CLS_TRACKING_PO_HEADER_G2(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      TRACKING_PO_HEADER_G2()

def TRACKING_PO_HEADER_G2():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  sql = """select 'NYG2' BU,a.cus_po_no,a.SO_NO, a.SO_YEAR, a.SUB_NO, a.SO_NO_DOC, a.STYLE_REF, a.SHIP_DATE, a.CUST_CODE, a.COL_FC, a.SIZE_FC, a.QTY,
(select nvl(sum(b.cut_qty),0) from NYG_PHO.bb_wip_cut_1@NYG2_170.world b where a.so_no = b.so_no and a.so_year = b.so_year and a.sub_no = b.SUB and a.COL_FC = b.COLOR_FC and a.SIZE_FC = b.SIZE_FC) cut_qty,
(select nvl(sum(c.WW_IN_QTY),0) from NYG_PHO.dfit_ep_detail@NYG2_170.world c where a.SO_NO = c.SO_NO and a.SO_YEAR = c.SO_YEAR and a.SUB_NO = c.SUB and a.COL_FC = c.SO_COLOR and a.SIZE_FC = c.SO_SIZE and c.WW_STEP_NAME = 'Z' and c.WW_OUT_DATE is not null) sm2_qty,
(select nvl(sum(c11.WW_OUT_QTY),0) from NYG_PHO.dfit_ep_detail@NYG2_170.world c11 where a.SO_NO = c11.SO_NO and a.SO_YEAR = c11.SO_YEAR and a.SUB_NO = c11.SUB and a.COL_FC = c11.SO_COLOR and a.SIZE_FC = c11.SO_SIZE and c11.WW_STEP_NAME = 'P' and c11.WW_OUT_DATE is not null) pout_qty,
(select nvl(sum(c1.WW_IN_QTY),0) from NYG_PHO.dfit_ep_detail@NYG2_170.world c1 where a.SO_NO = c1.SO_NO and a.SO_YEAR = c1.SO_YEAR and a.SUB_NO = c1.SUB and a.COL_FC = c1.SO_COLOR and a.SIZE_FC = c1.SO_SIZE and c1.WW_STEP_NAME = 'P' and c1.WW_IN_DATE is not null) pin_qty,
(select nvl(sum(c21.WW_OUT_QTY),0) from NYG_PHO.dfit_ep_detail@NYG2_170.world c21 where a.SO_NO = c21.SO_NO and a.SO_YEAR = c21.SO_YEAR and a.SUB_NO = c21.SUB and a.COL_FC = c21.SO_COLOR and a.SIZE_FC = c21.SO_SIZE and c21.WW_STEP_NAME = 'E' and c21.WW_OUT_DATE is not null) eout_qty,
(select nvl(sum(c2.WW_IN_QTY),0) from NYG_PHO.dfit_ep_detail@NYG2_170.world c2 where a.SO_NO = c2.SO_NO and a.SO_YEAR = c2.SO_YEAR and a.SUB_NO = c2.SUB and a.COL_FC = c2.SO_COLOR and a.SIZE_FC = c2.SO_SIZE and c2.WW_STEP_NAME = 'E' and c2.WW_IN_DATE is not null) ein_qty,
(select nvl(sum(c31.WW_OUT_QTY),0) from NYG_PHO.dfit_ep_detail@NYG2_170.world c31 where a.SO_NO = c31.SO_NO and a.SO_YEAR = c31.SO_YEAR and a.SUB_NO = c31.SUB and a.COL_FC = c31.SO_COLOR and a.SIZE_FC = c31.SO_SIZE and c31.WW_STEP_NAME = 'H' and c31.WW_OUT_DATE is not null) hout_qty,
(select nvl(sum(c3.WW_IN_QTY),0) from NYG_PHO.dfit_ep_detail@NYG2_170.world c3 where a.SO_NO = c3.SO_NO and a.SO_YEAR = c3.SO_YEAR and a.SUB_NO = c3.SUB and a.COL_FC = c3.SO_COLOR and a.SIZE_FC = c3.SO_SIZE and c3.WW_STEP_NAME = 'H' and c3.WW_IN_DATE is not null) hin_qty,
(select nvl(sum(c31.WW_OUT_QTY),0) from NYG_PHO.dfit_ep_detail@NYG2_170.world c31 where a.SO_NO = c31.SO_NO and a.SO_YEAR = c31.SO_YEAR and a.SUB_NO = c31.SUB and a.COL_FC = c31.SO_COLOR and a.SIZE_FC = c31.SO_SIZE and c31.WW_STEP_NAME = 'B' and c31.WW_OUT_DATE is not null) Bout_qty,
(select nvl(sum(c3.WW_IN_QTY),0) from NYG_PHO.dfit_ep_detail@NYG2_170.world c3 where a.SO_NO = c3.SO_NO and a.SO_YEAR = c3.SO_YEAR and a.SUB_NO = c3.SUB and a.COL_FC = c3.SO_COLOR and a.SIZE_FC = c3.SO_SIZE and c3.WW_STEP_NAME = 'B' and c3.WW_IN_DATE is not null) Bin_qty,
(select nvl(sum(d.WW_IN_QTY),0) from NYG_PHO.dfit_ep_detail@NYG2_170.world d where a.SO_NO = d.SO_NO and a.SO_YEAR = d.SO_YEAR and a.SUB_NO = d.SUB and a.COL_FC = d.SO_COLOR and a.SIZE_FC = d.SO_SIZE and d.WW_STEP_NAME = 'M' and d.WW_OUT_DATE is not null) sm3_qty,
(select nvl(sum(e.WW_IN_QTY),0) from NYG_PHO.dfit_ep_detail@NYG2_170.world e where a.SO_NO = e.SO_NO and a.SO_YEAR = e.SO_YEAR and a.SUB_NO = e.SUB and a.COL_FC = e.SO_COLOR and a.SIZE_FC = e.SO_SIZE and e.WW_STEP_NAME = 'S' and e.WW_OUT_DATE is not null) sm4_qty,
(select nvl(sum(f.QTY),0) from NYG_PHO.dfit_bb_loading@NYG2_170.world f where a.SO_NO = f.SO_NO and a.so_year = f.SO_YEAR and a.SUB_NO = f.SUB_NO and a.COL_FC = f.COLOR_FC and a.SIZE_FC = f.SIZE_FC) load_qty,
(select nvl(sum(m.wait_fn_qty),0) from NYG_PHO.dfit_bb_loading@NYG2_170.world m where a.SO_NO = m.SO_NO and a.SO_YEAR = m.SO_YEAR and a.SUB_NO = m.SUB_NO and a.COL_FC = m.COLOR_FC and a.SIZE_FC = m.SIZE_FC and m.WAIT_FN = 'Y') qc_qty,
(select nvl(sum(n.QTY),0) from NYG_PHO.dfit_bb_defect@NYG2_170.world n where a.SO_NO = n.SO_NO and a.SO_YEAR = n.so_year and a.SUB_NO = n.SUB_NO and a.COL_FC = n.COLOR_FC and a.SIZE_FC = n.SIZE_FC and n.REASON_CODE not in ('S001', 'S002', 'S003', 'S005', 'S006', 'S099') and n.DEFECT_TYPE = 'S') qc_minus,
(select nvl(sum(g.BUNDLE_QTY),0) from NYG_PHO.dfit_bb_process@NYG2_170.world g where a.SO_NO = g.SO_NO and a.so_year = g.SO_YEAR and a.SUB_NO = g.SUB_NO and a.COL_FC = g.COLOR_FC and a.SIZE_FC = g.BUNDLE_SIZE) fn_qty,
(select nvl(sum(h.QTY),0) from NYG_PHO.dfit_bb_defect@NYG2_170.world h where a.SO_NO = h.SO_NO and a.SO_YEAR = h.so_year and a.SUB_NO = h.SUB_NO and a.COL_FC = h.COLOR_FC and a.SIZE_FC = h.SIZE_FC and h.REASON_CODE not in ('S001', 'S002', 'S003', 'S005', 'S006', 'S099')) defect_qty,
(select nvl(sum(i.QTY),0) from NYG_PHO.oe_pre_pack_assort@NYG2_170.world i where a.so_no = i.so_no and a.SO_year = i.so_year and a.sub_no = i.SUB_NO and a.COL_FC = i.COL_FC and a.SIZE_FC = i.SIZE_FC and i.ACTUAL_WEIGHT is not null ) scan_pack,
(select nvl(sum(j.PCS_QTY),0) from NYG_PHO.wip_fg_by_sub@NYG2_170.WORLD j where a.SO_NO = j.SO_NO and a.so_year = j.SO_YEAR and a.SUB_NO = j.SUB_NO and a.COL_FC = j.COLOR_FC and a.SIZE_FC = j.SIZE_FC and j.DOC_TYPE not in ('A GRADE', 'B GRADE', 'SHIPMENT', 'SCRAP')) fg_qty,
(select nvl(sum(k.PCS_QTY),0) from NYG_PHO.wip_fg_by_sub@NYG2_170.WORLD k where a.SO_NO = k.SO_NO and a.so_year = k.SO_YEAR and a.SUB_NO = k.SUB_NO and a.COL_FC = k.COLOR_FC and a.SIZE_FC = k.SIZE_FC and k.ISSUE_TYPE not in ('EXPORT', 'SCRAP', 'ADJUST') ) fg_minus,
(select nvl(sum(l.PCS_QTY),0) from NYG_PHO.dfit_fg_issue_detail@NYG2_170.WORLD l where a.SO_NO = l.SO_NO and a.SO_YEAR = l.SO_YEAR and a.SUB_NO = l.SUB_NO and a.COL_FC = l.COLOR_FC and a.SIZE_FC = l.SIZE_FC and l.ISSUE_TYPE in ('EXPORT')) export_qty
from NYG_PHO.oe_so_sub_detail_v@NYG2_170.world a where a.so_year >18 AND a.SO_YEAR < 99 """

  cursor.execute(sql)

  _csv = r"C:\QVDatacenter\SCM\GARMENT\NYG\TRACKING_PO_HEADER_G2.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)

  conn.close()
  print("COMPLETE TRACKING_PO_HEADER_G2")


class CLS_TRACKING_PO_HEADER_G3(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      TRACKING_PO_HEADER_G3()

def TRACKING_PO_HEADER_G3():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  sql = """select 'NYG3' BU,a.cus_po_no,a.SO_NO, a.SO_YEAR, a.SUB_NO, a.SO_NO_DOC, a.STYLE_REF, a.SHIP_DATE, a.CUST_CODE, a.COL_FC, a.SIZE_FC, a.QTY,
(select nvl(sum(b.cut_qty),0) from nyg3.bb_wip_cut_1@wcs.world b where a.so_no = b.so_no and a.so_year = b.so_year and a.sub_no = b.SUB and a.COL_FC = b.COLOR_FC and a.SIZE_FC = b.SIZE_FC) cut_qty,
(select nvl(sum(c.WW_IN_QTY),0) from nyg3.dfit_ep_detail@wcs.world c where a.SO_NO = c.SO_NO and a.SO_YEAR = c.SO_YEAR and a.SUB_NO = c.SUB and a.COL_FC = c.SO_COLOR and a.SIZE_FC = c.SO_SIZE and c.WW_STEP_NAME = 'Z' and c.WW_OUT_DATE is not null) sm2_qty,
(select nvl(sum(c11.WW_OUT_QTY),0) from nyg3.dfit_ep_detail@wcs.world c11 where a.SO_NO = c11.SO_NO and a.SO_YEAR = c11.SO_YEAR and a.SUB_NO = c11.SUB and a.COL_FC = c11.SO_COLOR and a.SIZE_FC = c11.SO_SIZE and c11.WW_STEP_NAME = 'P' and c11.WW_OUT_DATE is not null) pout_qty,
(select nvl(sum(c1.WW_IN_QTY),0) from nyg3.dfit_ep_detail@wcs.world c1 where a.SO_NO = c1.SO_NO and a.SO_YEAR = c1.SO_YEAR and a.SUB_NO = c1.SUB and a.COL_FC = c1.SO_COLOR and a.SIZE_FC = c1.SO_SIZE and c1.WW_STEP_NAME = 'P' and c1.WW_IN_DATE is not null) pin_qty,
(select nvl(sum(c21.WW_OUT_QTY),0) from nyg3.dfit_ep_detail@wcs.world c21 where a.SO_NO = c21.SO_NO and a.SO_YEAR = c21.SO_YEAR and a.SUB_NO = c21.SUB and a.COL_FC = c21.SO_COLOR and a.SIZE_FC = c21.SO_SIZE and c21.WW_STEP_NAME = 'E' and c21.WW_OUT_DATE is not null) eout_qty,
(select nvl(sum(c2.WW_IN_QTY),0) from nyg3.dfit_ep_detail@wcs.world c2 where a.SO_NO = c2.SO_NO and a.SO_YEAR = c2.SO_YEAR and a.SUB_NO = c2.SUB and a.COL_FC = c2.SO_COLOR and a.SIZE_FC = c2.SO_SIZE and c2.WW_STEP_NAME = 'E' and c2.WW_IN_DATE is not null) ein_qty,
(select nvl(sum(c31.WW_OUT_QTY),0) from nyg3.dfit_ep_detail@wcs.world c31 where a.SO_NO = c31.SO_NO and a.SO_YEAR = c31.SO_YEAR and a.SUB_NO = c31.SUB and a.COL_FC = c31.SO_COLOR and a.SIZE_FC = c31.SO_SIZE and c31.WW_STEP_NAME = 'H' and c31.WW_OUT_DATE is not null) hout_qty,
(select nvl(sum(c3.WW_IN_QTY),0) from nyg3.dfit_ep_detail@wcs.world c3 where a.SO_NO = c3.SO_NO and a.SO_YEAR = c3.SO_YEAR and a.SUB_NO = c3.SUB and a.COL_FC = c3.SO_COLOR and a.SIZE_FC = c3.SO_SIZE and c3.WW_STEP_NAME = 'H' and c3.WW_IN_DATE is not null) hin_qty,
(select nvl(sum(c31.WW_OUT_QTY),0) from nyg3.dfit_ep_detail@wcs.world c31 where a.SO_NO = c31.SO_NO and a.SO_YEAR = c31.SO_YEAR and a.SUB_NO = c31.SUB and a.COL_FC = c31.SO_COLOR and a.SIZE_FC = c31.SO_SIZE and c31.WW_STEP_NAME = 'B' and c31.WW_OUT_DATE is not null) Bout_qty,
(select nvl(sum(c3.WW_IN_QTY),0) from nyg3.dfit_ep_detail@wcs.world c3 where a.SO_NO = c3.SO_NO and a.SO_YEAR = c3.SO_YEAR and a.SUB_NO = c3.SUB and a.COL_FC = c3.SO_COLOR and a.SIZE_FC = c3.SO_SIZE and c3.WW_STEP_NAME = 'B' and c3.WW_IN_DATE is not null) Bin_qty,
(select nvl(sum(d.WW_IN_QTY),0) from nyg3.dfit_ep_detail@wcs.world d where a.SO_NO = d.SO_NO and a.SO_YEAR = d.SO_YEAR and a.SUB_NO = d.SUB and a.COL_FC = d.SO_COLOR and a.SIZE_FC = d.SO_SIZE and d.WW_STEP_NAME = 'M' and d.WW_OUT_DATE is not null) sm3_qty,
(select nvl(sum(e.WW_IN_QTY),0) from nyg3.dfit_ep_detail@wcs.world e where a.SO_NO = e.SO_NO and a.SO_YEAR = e.SO_YEAR and a.SUB_NO = e.SUB and a.COL_FC = e.SO_COLOR and a.SIZE_FC = e.SO_SIZE and e.WW_STEP_NAME = 'S' and e.WW_OUT_DATE is not null) sm4_qty,
(select nvl(sum(f.QTY),0) from nyg3.dfit_bb_loading@wcs.world f where a.SO_NO = f.SO_NO and a.so_year = f.SO_YEAR and a.SUB_NO = f.SUB_NO and a.COL_FC = f.COLOR_FC and a.SIZE_FC = f.SIZE_FC) load_qty,
(select nvl(sum(m.wait_fn_qty),0) from nyg3.dfit_bb_loading@wcs.world m where a.SO_NO = m.SO_NO and a.SO_YEAR = m.SO_YEAR and a.SUB_NO = m.SUB_NO and a.COL_FC = m.COLOR_FC and a.SIZE_FC = m.SIZE_FC and m.WAIT_FN = 'Y') qc_qty,
(select nvl(sum(n.QTY),0) from nyg3.dfit_bb_defect@wcs.world n where a.SO_NO = n.SO_NO and a.SO_YEAR = n.so_year and a.SUB_NO = n.SUB_NO and a.COL_FC = n.COLOR_FC and a.SIZE_FC = n.SIZE_FC and n.REASON_CODE not in ('S001', 'S002', 'S003', 'S005', 'S006', 'S099') and n.DEFECT_TYPE = 'S') qc_minus,
(select nvl(sum(g.BUNDLE_QTY),0) from nyg3.dfit_bb_process@wcs.world g where a.SO_NO = g.SO_NO and a.so_year = g.SO_YEAR and a.SUB_NO = g.SUB_NO and a.COL_FC = g.COLOR_FC and a.SIZE_FC = g.BUNDLE_SIZE) fn_qty,
(select nvl(sum(h.QTY),0) from nyg3.dfit_bb_defect@wcs.world h where a.SO_NO = h.SO_NO and a.SO_YEAR = h.so_year and a.SUB_NO = h.SUB_NO and a.COL_FC = h.COLOR_FC and a.SIZE_FC = h.SIZE_FC and h.REASON_CODE not in ('S001', 'S002', 'S003', 'S005', 'S006', 'S099')) defect_qty,
(select nvl(sum(i.QTY),0) from nyg3.oe_pre_pack_assort@wcs.world i where a.so_no = i.so_no and a.SO_year = i.so_year and a.sub_no = i.SUB_NO and a.COL_FC = i.COL_FC and a.SIZE_FC = i.SIZE_FC and i.ACTUAL_WEIGHT is not null ) scan_pack,
(select nvl(sum(j.PCS_QTY),0) from nyg3.wip_fg_by_sub@wcs.world j where a.SO_NO = j.SO_NO and a.so_year = j.SO_YEAR and a.SUB_NO = j.SUB_NO and a.COL_FC = j.COLOR_FC and a.SIZE_FC = j.SIZE_FC and j.DOC_TYPE not in ('A GRADE', 'B GRADE', 'SHIPMENT', 'SCRAP')) fg_qty,
(select nvl(sum(k.PCS_QTY),0) from nyg3.wip_fg_by_sub@wcs.world k where a.SO_NO = k.SO_NO and a.so_year = k.SO_YEAR and a.SUB_NO = k.SUB_NO and a.COL_FC = k.COLOR_FC and a.SIZE_FC = k.SIZE_FC and k.ISSUE_TYPE not in ('EXPORT', 'SCRAP', 'ADJUST') ) fg_minus,
(select nvl(sum(l.PCS_QTY),0) from nyg3.dfit_fg_issue_detail@wcs.world l where a.SO_NO = l.SO_NO and a.SO_YEAR = l.SO_YEAR and a.SUB_NO = l.SUB_NO and a.COL_FC = l.COLOR_FC and a.SIZE_FC = l.SIZE_FC and l.ISSUE_TYPE in ('EXPORT')) export_qty
from nyg3.oe_so_sub_detail_v@wcs.world a where a.so_year>18 AND a.SO_YEAR < 99 """

  cursor.execute(sql)

  _csv = r"C:\QVDatacenter\SCM\GARMENT\NYG\TRACKING_PO_HEADER_G3.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)

  conn.close()
  print("COMPLETE TRACKING_PO_HEADER_G3")


class CLS_TRACKING_PO_HEADER_G4(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      TRACKING_PO_HEADER_G4()

def TRACKING_PO_HEADER_G4():
  my_dsn = cx_Oracle.makedsn("172.16.6.83", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="nygm", password="nygm",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  sql = """select 'NYG4' BU,a.cus_po_no,a.SO_NO, a.SO_YEAR, a.SUB_NO, a.SO_NO_DOC, a.STYLE_REF, a.SHIP_DATE, a.CUST_CODE, a.COL_FC, a.SIZE_FC, a.QTY,
(select nvl(sum(b.cut_qty),0) from nyg4.bb_wip_cut_1@NYG4.world b where a.so_no = b.so_no and a.so_year = b.so_year and a.sub_no = b.SUB and a.COL_FC = b.COLOR_FC and a.SIZE_FC = b.SIZE_FC) cut_qty,
(select nvl(sum(c.WW_IN_QTY),0) from nyg4.dfit_ep_detail@NYG4.world c where a.SO_NO = c.SO_NO and a.SO_YEAR = c.SO_YEAR and a.SUB_NO = c.SUB and a.COL_FC = c.SO_COLOR and a.SIZE_FC = c.SO_SIZE and c.WW_STEP_NAME = 'Z' and c.WW_OUT_DATE is not null) sm2_qty,
(select nvl(sum(c11.WW_OUT_QTY),0) from nyg4.dfit_ep_detail@NYG4.world c11 where a.SO_NO = c11.SO_NO and a.SO_YEAR = c11.SO_YEAR and a.SUB_NO = c11.SUB and a.COL_FC = c11.SO_COLOR and a.SIZE_FC = c11.SO_SIZE and c11.WW_STEP_NAME = 'P' and c11.WW_OUT_DATE is not null) pout_qty,
(select nvl(sum(c1.WW_IN_QTY),0) from nyg4.dfit_ep_detail@NYG4.world c1 where a.SO_NO = c1.SO_NO and a.SO_YEAR = c1.SO_YEAR and a.SUB_NO = c1.SUB and a.COL_FC = c1.SO_COLOR and a.SIZE_FC = c1.SO_SIZE and c1.WW_STEP_NAME = 'P' and c1.WW_IN_DATE is not null) pin_qty,
(select nvl(sum(c21.WW_OUT_QTY),0) from nyg4.dfit_ep_detail@NYG4.world c21 where a.SO_NO = c21.SO_NO and a.SO_YEAR = c21.SO_YEAR and a.SUB_NO = c21.SUB and a.COL_FC = c21.SO_COLOR and a.SIZE_FC = c21.SO_SIZE and c21.WW_STEP_NAME = 'E' and c21.WW_OUT_DATE is not null) eout_qty,
(select nvl(sum(c2.WW_IN_QTY),0) from nyg4.dfit_ep_detail@NYG4.world c2 where a.SO_NO = c2.SO_NO and a.SO_YEAR = c2.SO_YEAR and a.SUB_NO = c2.SUB and a.COL_FC = c2.SO_COLOR and a.SIZE_FC = c2.SO_SIZE and c2.WW_STEP_NAME = 'E' and c2.WW_IN_DATE is not null) ein_qty,
(select nvl(sum(c31.WW_OUT_QTY),0) from nyg4.dfit_ep_detail@NYG4.world c31 where a.SO_NO = c31.SO_NO and a.SO_YEAR = c31.SO_YEAR and a.SUB_NO = c31.SUB and a.COL_FC = c31.SO_COLOR and a.SIZE_FC = c31.SO_SIZE and c31.WW_STEP_NAME = 'H' and c31.WW_OUT_DATE is not null) hout_qty,
(select nvl(sum(c3.WW_IN_QTY),0) from nyg4.dfit_ep_detail@NYG4.world c3 where a.SO_NO = c3.SO_NO and a.SO_YEAR = c3.SO_YEAR and a.SUB_NO = c3.SUB and a.COL_FC = c3.SO_COLOR and a.SIZE_FC = c3.SO_SIZE and c3.WW_STEP_NAME = 'H' and c3.WW_IN_DATE is not null) hin_qty,
(select nvl(sum(c31.WW_OUT_QTY),0) from nyg4.dfit_ep_detail@NYG4.world c31 where a.SO_NO = c31.SO_NO and a.SO_YEAR = c31.SO_YEAR and a.SUB_NO = c31.SUB and a.COL_FC = c31.SO_COLOR and a.SIZE_FC = c31.SO_SIZE and c31.WW_STEP_NAME = 'B' and c31.WW_OUT_DATE is not null) Bout_qty,
(select nvl(sum(c3.WW_IN_QTY),0) from nyg4.dfit_ep_detail@NYG4.world c3 where a.SO_NO = c3.SO_NO and a.SO_YEAR = c3.SO_YEAR and a.SUB_NO = c3.SUB and a.COL_FC = c3.SO_COLOR and a.SIZE_FC = c3.SO_SIZE and c3.WW_STEP_NAME = 'B' and c3.WW_IN_DATE is not null) Bin_qty,
(select nvl(sum(d.WW_IN_QTY),0) from nyg4.dfit_ep_detail@NYG4.world d where a.SO_NO = d.SO_NO and a.SO_YEAR = d.SO_YEAR and a.SUB_NO = d.SUB and a.COL_FC = d.SO_COLOR and a.SIZE_FC = d.SO_SIZE and d.WW_STEP_NAME = 'M' and d.WW_OUT_DATE is not null) sm3_qty,
(select nvl(sum(e.WW_IN_QTY),0) from nyg4.dfit_ep_detail@NYG4.world e where a.SO_NO = e.SO_NO and a.SO_YEAR = e.SO_YEAR and a.SUB_NO = e.SUB and a.COL_FC = e.SO_COLOR and a.SIZE_FC = e.SO_SIZE and e.WW_STEP_NAME = 'S' and e.WW_OUT_DATE is not null) sm4_qty,
(select nvl(sum(f.QTY),0) from nyg4.dfit_bb_loading@NYG4.world f where a.SO_NO = f.SO_NO and a.so_year = f.SO_YEAR and a.SUB_NO = f.SUB_NO and a.COL_FC = f.COLOR_FC and a.SIZE_FC = f.SIZE_FC) load_qty,
(select nvl(sum(m.wait_fn_qty),0) from nyg4.dfit_bb_loading@NYG4.world m where a.SO_NO = m.SO_NO and a.SO_YEAR = m.SO_YEAR and a.SUB_NO = m.SUB_NO and a.COL_FC = m.COLOR_FC and a.SIZE_FC = m.SIZE_FC and m.WAIT_FN = 'Y') qc_qty,
(select nvl(sum(n.QTY),0) from nyg4.dfit_bb_defect@NYG4.world n where a.SO_NO = n.SO_NO and a.SO_YEAR = n.so_year and a.SUB_NO = n.SUB_NO and a.COL_FC = n.COLOR_FC and a.SIZE_FC = n.SIZE_FC and n.REASON_CODE not in ('S001', 'S002', 'S003', 'S005', 'S006', 'S099') and n.DEFECT_TYPE = 'S') qc_minus,
(select nvl(sum(g.BUNDLE_QTY),0) from nyg4.dfit_bb_process@NYG4.world g where a.SO_NO = g.SO_NO and a.so_year = g.SO_YEAR and a.SUB_NO = g.SUB_NO and a.COL_FC = g.COLOR_FC and a.SIZE_FC = g.BUNDLE_SIZE) fn_qty,
(select nvl(sum(h.QTY),0) from nyg4.dfit_bb_defect@NYG4.world h where a.SO_NO = h.SO_NO and a.SO_YEAR = h.so_year and a.SUB_NO = h.SUB_NO and a.COL_FC = h.COLOR_FC and a.SIZE_FC = h.SIZE_FC and h.REASON_CODE not in ('S001', 'S002', 'S003', 'S005', 'S006', 'S099')) defect_qty,
(select nvl(sum(i.QTY),0) from nyg4.oe_pre_pack_assort@NYG4.world i where a.so_no = i.so_no and a.SO_year = i.so_year and a.sub_no = i.SUB_NO and a.COL_FC = i.COL_FC and a.SIZE_FC = i.SIZE_FC and i.ACTUAL_WEIGHT is not null ) scan_pack,
(select nvl(sum(j.PCS_QTY),0) from nyg4.wip_fg_by_sub@NYG4.world j where a.SO_NO = j.SO_NO and a.so_year = j.SO_YEAR and a.SUB_NO = j.SUB_NO and a.COL_FC = j.COLOR_FC and a.SIZE_FC = j.SIZE_FC and j.DOC_TYPE not in ('A GRADE', 'B GRADE', 'SHIPMENT', 'SCRAP')) fg_qty,
(select nvl(sum(k.PCS_QTY),0) from nyg4.wip_fg_by_sub@NYG4.world k where a.SO_NO = k.SO_NO and a.so_year = k.SO_YEAR and a.SUB_NO = k.SUB_NO and a.COL_FC = k.COLOR_FC and a.SIZE_FC = k.SIZE_FC and k.ISSUE_TYPE not in ('EXPORT', 'SCRAP', 'ADJUST') ) fg_minus,
(select nvl(sum(l.PCS_QTY),0) from nyg4.dfit_fg_issue_detail@NYG4.world l where a.SO_NO = l.SO_NO and a.SO_YEAR = l.SO_YEAR and a.SUB_NO = l.SUB_NO and a.COL_FC = l.COLOR_FC and a.SIZE_FC = l.SIZE_FC and l.ISSUE_TYPE in ('EXPORT')) export_qty
from nyg4.oe_so_sub_detail_v@NYG4.world a where a.so_year>18 AND a.SO_YEAR < 99 """

  cursor.execute(sql)

  _csv = r"C:\QVDatacenter\SCM\GARMENT\NYG\TRACKING_PO_HEADER_G4.csv"

  with open(_csv, "w", newline='', encoding="utf-8") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor)

  conn.close()
  print("COMPLETE TRACKING_PO_HEADER_G4")


#############################################
threads = []

thread1 = CLS_TRACKING_PO_HEADER_G1()
thread1.start()
threads.append(thread1)

thread2 = CLS_TRACKING_PO_HEADER_G2()
thread2.start()
threads.append(thread2)

thread3 = CLS_TRACKING_PO_HEADER_G3()
thread3.start()
threads.append(thread3)


thread4 = CLS_TRACKING_PO_HEADER_G4()
thread4.start()
threads.append(thread4)


thread5 = CLS_TRACKING_PO_SUB()
thread5.start()
threads.append(thread5)

thread6 = CLS_TRACKING_PO_ITEM()
thread6.start()
threads.append(thread6)


for t in threads:
    t.join()
print("COMPLETE")
