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


class CLS_FG_OUTPUT(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)

    def run(self):
      FG_OUTPUT()


def FG_OUTPUT():
  my_dsn = cx_Oracle.makedsn("172.16.6.76", port=1521, sid="NYTG")
  conn = cx_Oracle.connect(user="DEMO", password="DEMO",
                           dsn=my_dsn, encoding="UTF-8", nencoding="UTF-8")
  cursor = conn.cursor()

  sql = """ SELECT Sh.ou_code,ph.pl_no,Sh.so_no,sh.po_no,Sh.batch_no,
                sh.customer_name cust_name,sh.buyyer,Nvl(Ph.SO_LINE_ID,Sh.Line_Id) So_Line,
                sh.item_code,sh.item_desc item_desc,
                sh.color_code,sh.color_desc color_desc,Sh.Color_Shade,
                trunc(ph.shipment_date) shipment_date,Sh.Line_Id,
                ph.pl_date,sh.close_date,Sh.appoint_date,
                Decode(Substr(ph.pl_no,3,1),'S',0,ph.qty_rm) qty_rm,
                ph.roll_fg roll,ph.qty_fg qty_fg,
                Decode(Substr(sh.item_code,1,1),'C',Nvl(Ph.UNIT_FG,0),Nvl(ph.qty_fg,0)) Unit_fg,
                decode(substr(ph.pl_no,1,2),'P2','SCRAP','NORMAL') Type_PL,ph.UPD_BY,
                trunc(sh.dye_sdate) dyedate,trunc(sh.qt_date) qt_date, 
                DECODE(ph.ship_to_wh,'WH','Logistic','Production') ship_to,  Sh.Schedule_Id,
                sh.sale_id,sh.sale_name,e.team_name,PH.GRADE_NO,Sl.Attribute1 TypeOp,Sl.Attribute2 TypePack,
                sh.mps_week_year,sh.MPS_WEEK_NO,sh.dps_week_no,sh.STYLE,
                SH.PRODUCT_TYPE,SH.PROCESS_LINE,SH.WIDTH,SH.WEIGHT_G,sh.item_process,sh.so_no_date,
                Ph.Remark,
                Decode(Substr(Sh.So_No,1,1),'2','NYK',Decode(Substr(Nvl(Sh.Org_Id,0),1,1),'2','NYK','NYF')) So_BU,
                Ph.APPROVED_REMARK,
                Sh.total_qty,SH.CHE_FIN_COST,SH.TOTAL_COST,SL.OE_PRICE_SELL STD_OE_PRICE,
                sl.oe_price_lost std_oe_Lost,
                (nvl(sl.SDC_Cost,0) + nvl(sl.SCC_Cost,0) + nvl(sl.scf_cost,0)) Std_Cc,  
                (nvl(sl.OHC_PRE_COST,0) + nvl(sl.OHC_dye_COST,0) + nvl(sl.OHC_Fin_COST,0) + nvl(sl.OHC_ins_COST,0) + nvl(sl.OHC_ship_COST,0) + nvl(sl.ipc_step_cost,0)) Std_Oh,
                (nvl(sl.SED_Cost,0) + nvl(sl.ipc_mc_cost,0)) Std_Energy, 
                nvl(sl.ipc_depre_cost,0) Std_Ipc_Depre_Cost ,
                Decode(Substr(sh.item_code,1,1),'F','ผ้า','ปก') Type_Fabric,
                Decode(NVL(Sh.job_type,'@'),'N','ปกติ','D','ซ่อมภายใน','R','ซ่อมภายนอก') JobType ,
                ph.Fabric_Type Fg_Type,
                (select v.schedule_id_ref from dfit_mc_schedule v where v.schedule_id=sh.schedule_id) schedule_id_ref,
                SL.METERIAL_GRP_ITEM,
                (select sum(yard) from dfpl_detail dfpld where dfpld.ou_code = ph.ou_code and dfpld.pl_no = ph.pl_no) qty_yard
        FROM DFPL_HEADER ph, 
             dfit_btdata sh, 
             dfora_sale E,
             DFIT_SO_LINE SL
        WHERE ph.ou_code = Sh.ou_code
        AND  ph.batch_no = Sh.batch_no
        AND  sh.sale_id = e.sale_id
        AND  Sh.SO_NO = Sl.SO_NO(+)
        AND  Sh.LINE_ID = Sl.LINE_ID(+)
        and Sh.status <> '9' 
        and ph.status not in ('x','X','9') and ph.shipment_date is not null
        and trunc(ph.shipment_date) >=to_date('01/01/2016','dd/mm/yyyy') """

  df = pd.read_sql_query(sql, conn)

  df.fillna("", inplace=True)

  df = df.applymap(lambda x: x.encode('unicode_escape').
                   decode('utf-8') if isinstance(x, str) else x)


FG_OUTPUT()