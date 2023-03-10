#-*-coding: utf-8 -*-
import pymssql
import numpy as np
import pandas as pd
import sys
import datetime as dt
from datetime import date, datetime, timedelta
import threading
import requests


server = "172.16.0.62"
user = "sa"
password = "P@ssw0rd"


token = 'cDIAgmdmtb86QxWZ4pT5fQREI4IOxRAhbjvcDzhy9jT'

def sendLine(txt):
  url = 'https://notify-api.line.me/api/notify'
  token = 'cDIAgmdmtb86QxWZ4pT5fQREI4IOxRAhbjvcDzhy9jT'
  headers = {'content-type':'application/x-www-form-urlencoded','Authorization':'Bearer ' + token}
  msg = txt
  requests.post(url,headers=headers,data = {'message':msg})

def yarn_stock_aging():
    sql = """ SELECT [SITE]
      ,[WHSE]
      ,[UF_WDATE]
      ,[ITEM_GROUP]
      ,[DATE_DIFF]
      ,[PRODUCT_CODE]
      ,[ITEM]
      ,[ITEM_DESC]
      ,[LOT]
      ,[QTY_ON_HAND]
      ,[STD_UNIT_COST] 
FROM NYT.dbo.V_FG_STOCK_AGING """
    conn = pymssql.connect(server, user, password, "NYT")
    df = pd.read_sql_query(sql, conn)
    # print(df)
    df.to_csv(
      r'C:\QVD_DATA\COM_GARMENT\NYT\YARN_STOCK_AGING.csv',encoding='utf-8-sig', index=False)
    conn.close()
    sendLine("yarn_stock_aging()")


def net_income_product():
    sql = """ SELECT SITE_REF,	INV_NUM	INV_DATE,	INV_YEAR,	INV_MONTH,	INV_QUARTER,	CUST_NUM,
    CUST_NAME,	CUST_ITEM,	ITEM_CODE,	ITEM_DESC,	CO_NUM,	QTY_INVOICED,	
    QTY_INVOICED_LB,	EXTENDED_PRICE,	PRICE_LB,	CURR_CODE,	EXCH_RATE,	EXTENDED_PRIC_THB,	
    PRICE_THB,	EXTENDED_PRICE_FOREIGN,	PRICE_FOREIGN,	STD_UNIT_COST_THB_KG,	
    STD_UNIT_COST_THB_LB,	CUST_PO,	COUNTRY,	CUST_GROUP,	PLANT

                FROM NYT.dbo.V_BI_INVOICE_LISTING_RAW_PRODUCT M """
    conn = pymssql.connect(server, user, password, "NYT")
    df = pd.read_sql_query(sql, conn)
    print(df)
    df.to_csv(
      r'C:\QVD_DATA\COM_GARMENT\NYT\NET_INCOME_PRODUCT.csv',encoding='utf-8-sig', index=False)
    conn.close()
    sendLine("net_income_product()")


def net_income():
    sql = """ SELECT SITE_REF,	INV_NUM,	INV_DATE,	INV_YEAR,	INV_MONTH,	INV_QUARTER,	CUST_NUM,	CUST_NAME,	CUST_ITEM,	ITEM_CODE,	ITEM_DESC,
    	CO_NUM,	QTY_INVOICED,	QTY_INVOICED_LB,	EXTENDED_PRICE,	PRICE_LB,	CURR_CODE,	EXCH_RATE,	EXTENDED_PRIC_THB,	PRICE_THB,	EXTENDED_PRICE_FOREIGN,
        	PRICE_FOREIGN,	STD_UNIT_COST_THB_KG,	STD_UNIT_COST_THB_LB,	CUST_PO,	COUNTRY,	CUST_GROUP,	PLANT
                FROM NYT.dbo.V_BI_INVOICE_LISTING_RAW_YARN M """
    conn = pymssql.connect(server, user, password, "NYT")
    df = pd.read_sql_query(sql, conn)
    print(df)
    df.to_csv(
      r'C:\QVD_DATA\COM_GARMENT\NYT\NET_INCOME.csv',encoding='utf-8-sig', index=False)
    conn.close()
    sendLine("net_income()")



def yarn_to_store_petch():
    sql = """ SELECT 
		MATLTRAN.TRANS_DATE AS RECV_DATE,
		CASE 
			WHEN MATLTRAN.REF_TYPE = 'F' THEN 'F'
			WHEN MATLTRAN.REF_TYPE = 'I' THEN 'Inventory'
			WHEN MATLTRAN.REF_TYPE = 'J' THEN 'JOB'
			WHEN MATLTRAN.REF_TYPE = 'O' THEN 'Customer Order'
			WHEN MATLTRAN.REF_TYPE = 'P' THEN 'Purchase Order'
			WHEN MATLTRAN.REF_TYPE = 'R' THEN 'RMA'
		ELSE
		''
		END AS REFERENCE_TYPE,
		LOT.UF_NYTLOT AS YARN_LOT,
		LOT.UF_PROCESSTYPE AS LOT_TYPE,
		MATLTRAN.ITEM , MATERIALTRANSACTIONSVIEW.ITEMDESC ,
		LOT.UF_YARN AS COMB_ID,
		'PETCH' AS SITE , 
		'PETCH' PLANT,
		MATLTRAN.REF_NUM AS JOB_SL,
		JOB.UF_JOB_JOBTYPE + '.'+ UFV.DESCRIPTION AS JOB_TYPE,
		LOT.UF_PKID AS PK_ID,
		ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.UF_WDATE AS PACK_DATE,
		PPK.UF_BK3 AS SHIFT,
		MATLTRAN.whse AS WAREHOUSE,
		MATLTRAN.loc AS LOCATOR,
		(SELECT count(*) FROM PETCH_App.DBO.LOT AS L
		WHERE L.UF_PKID = LOT.UF_PKID) AS TOTAL_YARN,
		J_ITEM.u_m AS UNIT,
	    LOT.UF_GROSS AS GROSS_WEIGHT, LOT.UF_TARE AS TARE_WEIGHT,
		MATLTRAN.QTY AS NET_WEIGHT
		FROM 
		PETCH_App.DBO.MATLTRAN 
		LEFT JOIN PETCH_App.DBO.LOT
		ON MATLTRAN.ITEM = LOT.ITEM
		AND MATLTRAN.LOT = LOT.LOT
		LEFT JOIN PETCH_App.DBO.MATERIALTRANSACTIONSVIEW
		ON MATLTRAN.TRANS_NUM = MATERIALTRANSACTIONSVIEW.TRANS_NUM
		LEFT JOIN PETCH_App.DBO.ERPP_SL_PPK_BATCHJOBRECEIPT_TMP 
		ON MATLTRAN.ITEM = ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.UF_ITEM
		AND MATLTRAN.REF_NUM = ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.UF_JOB
		AND MATLTRAN.LOT = ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.UF_CON
		LEFT JOIN PETCH_App.DBO.ERPP_SL_PPK_V_TMP PPK
		ON ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.SITE = PPK.SITE
		AND ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.UF_PLANT = PPK.UF_PLANT
		AND ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.UF_JOB = PPK.UF_JOB
		AND ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.TRANS_DATE = PPK.TRANS_DATE
		AND ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.UF_DELIVERY_ID = PPK.UF_DELIVERY_ID
		AND ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.UF_CON = PPK.UF_CON
		AND ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.UF_PKID = PPK.UF_PKID
		AND ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.UF_DELIVERY_STAT = 'Y'
		LEFT JOIN PETCH_App.DBO.JOB 
		ON JOB.JOB = MATLTRAN.REF_NUM
		LEFT JOIN PETCH_App.DBO.USERDEFINEDTYPEVALUES AS UFV
		ON UFV.TYPENAME = 'ERPP_NYT_JOB_JOBTYPE' 
		AND UFV.VALUE = JOB.UF_JOB_JOBTYPE
		LEFT JOIN PETCH_App.DBO.ITEM AS J_ITEM
		ON MATLTRAN.ITEM = J_ITEM.ITEM
		WHERE TRANS_TYPE = 'F'
		AND (SUBSTRING(MATLTRAN.ITEM,1,2) = '01'
		OR MATLTRAN.ITEM IN ('DYE FABRIC','GREY FABRIC'))
		AND MATLTRAN.LOC <> 'RMMAIN'
		AND MATLTRAN.WHSE = '3FG'
		AND MATLTRAN.QTY <> 0  
		AND CONVERT(VARCHAR(10), MATLTRAN.TRANS_DATE, 120) >=  CHAR(39) +CAST('2017-01-01' AS VARCHAR(10)) + CHAR(39) """
    conn = pymssql.connect(server, user, password, "NYT")
    df = pd.read_sql_query(sql, conn)
    print(df)
    df.to_csv(
      r'C:\QVD_DATA\COM_GARMENT\NYT\YARN_TO_STORE_PETCH.csv',encoding='utf-8-sig', index=False)
    conn.close()
    sendLine("yarn_to_store_petch()")



def yarn_to_store_sai5():
    sql = """ SELECT
		MATLTRAN.TRANS_DATE AS RECV_DATE,
		CASE 
			WHEN MATLTRAN.REF_TYPE = 'F' THEN 'F'
			WHEN MATLTRAN.REF_TYPE = 'I' THEN 'Inventory'
			WHEN MATLTRAN.REF_TYPE = 'J' THEN 'JOB'
			WHEN MATLTRAN.REF_TYPE = 'O' THEN 'Customer Order'
			WHEN MATLTRAN.REF_TYPE = 'P' THEN 'Purchase Order'
			WHEN MATLTRAN.REF_TYPE = 'R' THEN 'RMA'
		ELSE
		''
		END AS REFERENCE_TYPE,
		LOT.UF_NYTLOT AS YARN_LOT,
		LOT.UF_PROCESSTYPE AS LOT_TYPE,
		MATLTRAN.ITEM , MATERIALTRANSACTIONSVIEW.ITEMDESC ,
		LOT.UF_YARN AS COMB_ID,
		'SAI5' AS SITE , 
		CASE
			WHEN SUBSTRING(LOT.UF_NYTLOT,5,2) = 'S1' OR SUBSTRING(LOT.UF_NYTLOT,5,2) = 'S5' THEN 'S1'
			WHEN SUBSTRING(LOT.UF_NYTLOT,5,2) = 'S2' THEN 'S2'
			WHEN SUBSTRING(LOT.UF_NYTLOT,5,2) = 'S3' OR SUBSTRING(LOT.UF_NYTLOT,5,2) = 'S4'	THEN 'S3'
			ELSE 'S1'
		END AS PLANT,
		MATLTRAN.REF_NUM AS JOB_SL,
		JOB.UF_JOB_JOBTYPE + '.'+ UFV.DESCRIPTION AS JOB_TYPE,
		LOT.UF_PKID AS PK_ID,
		ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.UF_WDATE AS PACK_DATE,
		PPK.UF_BK3 AS SHIFT,
		MATLTRAN.WHSE AS WAREHOUSE,
		MATLTRAN.LOC AS LOCATOR,
		(SELECT count(*) FROM sai5_App.DBO.LOT AS L
		WHERE L.UF_PKID = LOT.UF_PKID) AS TOTAL_YARN,
		J_ITEM.U_M AS UNIT,
	    LOT.UF_GROSS AS GROSS_WEIGHT, LOT.UF_TARE AS TARE_WEIGHT,
		MATLTRAN.QTY AS NET_WEIGHT
		FROM 
		SAI5_APP.DBO.MATLTRAN 
		LEFT JOIN SAI5_APP.DBO.LOT
		ON MATLTRAN.ITEM = LOT.ITEM
		AND MATLTRAN.LOT = LOT.LOT
		LEFT JOIN sai5_App.DBO.MATERIALTRANSACTIONSVIEW
		ON MATLTRAN.TRANS_NUM = MATERIALTRANSACTIONSVIEW.TRANS_NUM
		LEFT JOIN sai5_App.DBO.ERPP_SL_PPK_BATCHJOBRECEIPT_TMP 
		ON MATLTRAN.ITEM = ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.UF_ITEM
		AND MATLTRAN.REF_NUM = ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.UF_JOB
		AND MATLTRAN.LOT = ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.UF_CON
		LEFT JOIN sai5_App.DBO.ERPP_SL_PPK_V_TMP PPK
		ON ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.SITE = PPK.SITE
		AND ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.UF_PLANT = PPK.UF_PLANT
		AND ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.UF_JOB = PPK.UF_JOB
		AND ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.TRANS_DATE = PPK.TRANS_DATE
		AND ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.UF_DELIVERY_ID = PPK.UF_DELIVERY_ID
		AND ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.UF_CON = PPK.UF_CON
		AND ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.UF_PKID = PPK.UF_PKID
		AND ERPP_SL_PPK_BATCHJOBRECEIPT_TMP.UF_DELIVERY_STAT = 'Y'
		LEFT JOIN sai5_App.DBO.JOB 
		ON JOB.JOB = MATLTRAN.REF_NUM
		LEFT JOIN sai5_App.DBO.USERDEFINEDTYPEVALUES AS UFV
		ON UFV.TYPENAME = 'ERPP_NYT_JOB_JOBTYPE' 
		AND UFV.VALUE = JOB.UF_JOB_JOBTYPE
		LEFT JOIN sai5_App.DBO.ITEM AS J_ITEM
		ON MATLTRAN.ITEM = J_ITEM.ITEM
		WHERE TRANS_TYPE = 'F'
		AND (SUBSTRING(MATLTRAN.ITEM,1,2) = '01'
		OR MATLTRAN.ITEM IN ('DYE FABRIC','GREY FABRIC'))
		AND MATLTRAN.LOC <> 'RMMAIN'
		AND MATLTRAN.WHSE = '1FG' 
		AND MATLTRAN.QTY <> 0  
		AND CONVERT(VARCHAR(10), MATLTRAN.TRANS_DATE, 120) >=  CHAR(39) +CAST('2017-01-01' AS VARCHAR(10)) + CHAR(39) """
    conn = pymssql.connect(server, user, password, "NYT")
    df = pd.read_sql_query(sql, conn)
    print(df)
    df.to_csv(
      r'C:\QVD_DATA\COM_GARMENT\NYT\YARN_TO_STORE_SAI5.csv',encoding='utf-8-sig', index=False)
    conn.close()
    sendLine("yarn_to_store_sai5()")


def artran_all_not_i():
    sql = """ select 
    SITE_REF,	CUST_NUM,	INV_NUM,	INV_SEQ,	CHECK_SEQ,	TYPE,	CO_NUM,	INV_DATE,	DUE_DATE,	AMOUNT,	DISC_AMT,	
    DESCRIPTION,	DISC_DATE,	EXCH_RATE,	MISC_CHARGES,	SALES_TAX,	FREIGHT,	SALES_TAX_2,	ACTIVE,	FIXED_RATE,	NOTEEXISTSFLAG	,
    RECORDDATE,	ROWPOINTER,	ACCT,	ACCT_UNIT1,	ACCT_UNIT2,	ACCT_UNIT3,	ACCT_UNIT4,	DO_NUM,	PAY_TYPE,	REF	CORP_CUST,	
    RMA,	ISSUE_DATE,	CREATEDBY,	UPDATEDBY,	CREATEDATE,	APPLY_TO_INV_NUM,	APPROVAL_STATUS,	APPLY_TO_INV_NUM_CATEGORY,	
    IS_INVOICE,	UF_ERPP_ARBILLNUM,	UF_ERPP_BILLDATE,	UF_ERPP_OLDDUEDATE,	UF_ARTRAN_AMOUNT,	UF_ARTRAN_DESCRIPTION1,	
    UF_ARTRAN_DESCRIPTION2,	UF_ARTRAN_DESCRIPTION3,	UF_ARTRAN_FREIGHT,	UF_ARTRAN_MISCHARGE,	UF_ARTRAN_MISCHARGE1,	
    UF_ARTRAN_QTY1,	UF_ARTRAN_QTY2,	UF_ARTRAN_QTY3,	UF_ARTRAN_SALETAX,	UF_ARTRAN_SALETAX1,	UF_ARTRAN_TOTAL,	UF_ARTRAN_TOTAL1,	
    UF_ARTRAN_UNIT1,	UF_ARTRAN_UNIT2,	UF_ARTRAN_UNIT3,	UF_ARTRAN_UNITPRICE1,	UF_ARTRAN_UNITPRICE2,	UF_ARTRAN_UNITPRICE3,
    UF_ARTRAN_FLAG,	UF_ERPP_BILLDUEPAIDDATE,	UF_ERPP_BILLOLDDUEPAIDDATE
from SAI5_App.dbo.artran_all 
where type <> 'I'
order by inv_num, inv_seq """
    conn = pymssql.connect(server, user, password, "NYT")
    df = pd.read_sql_query(sql, conn)
    print(df)
    df.to_csv(
      r'C:\QVD_DATA\COM_GARMENT\NYT\ARTRAN_ALL_NOT_I.csv',encoding='utf-8-sig', index=False)
    conn.close()
    sendLine("artran_all_not_i()")


def receive_issue_nyt():
    sql = """ SELECT SITE,	TRANS_NUM,	O_TRANS_TYPE,	O_LOCATION,	RECV_DATE,	PROCESS_TYPE,	COMB_ID	PLANT,	CONTAINER,
            GROSS,	TARE,	NET,	JOB_SL,	ITEM,	ITEMDESC,	PK_ID,	COND_PROTYPE,	PACK_DATE,	UF_QTY_TUBE,	UF_JOB_JOBTYPE,
            JOB_DESC,	UF_JOBTYPE_DESC,	LOT	LOCATION,	PACKING_NO,	SO_NO,	SO_LINE,	SO_RELEASE,	REF_TYPE,	TRANS_TYPE,
            CUSTOMER_ID,	CUSTOMER_DESC,	PO_NO,	INVOICE,	INV_DATE,	QTY_INVOICED,	PRICE,	AMOUNT
            FROM V_QVD_FG_RECVWH_ISS_PK
            WHERE YEAR(PACK_DATE) = 2019 """
    conn = pymssql.connect(server, user, password, "NYT")
    df = pd.read_sql_query(sql, conn)
    print(df)
    df.to_csv(
      r'C:\QVD_DATA\COM_GARMENT\NYT\RECEIVE_ISSUE_NYT.csv',encoding='utf-8-sig', index=False)
    conn.close()
    sendLine("receive_issue_nyt()")


def pending_stock_lastyear():
    sql = """ SELECT SITE,	TRANS_NUM,	O_TRANS_TYPE,	O_LOCATION,	RECV_DATE,	PROCESS_TYPE,	COMB_ID	PLANT,	CONTAINER,
            GROSS,	TARE,	NET,	JOB_SL,	ITEM,	ITEMDESC,	PK_ID,	COND_PROTYPE,	PACK_DATE,	UF_QTY_TUBE,	UF_JOB_JOBTYPE,
            JOB_DESC,	UF_JOBTYPE_DESC,	LOT	LOCATION,	PACKING_NO,	SO_NO,	SO_LINE,	SO_RELEASE,	REF_TYPE,	TRANS_TYPE,
            CUSTOMER_ID,	CUSTOMER_DESC,	PO_NO,	INVOICE,	INV_DATE,	QTY_INVOICED,	PRICE,	AMOUNT
            FROM V_QVD_FG_RECVWH_ISS_PK
            WHERE YEAR(PACK_DATE) < (YEAR(GETDATE())-1) AND inv_date IS NULL """
    conn = pymssql.connect(server, user, password, "NYT")
    df = pd.read_sql_query(sql, conn)
    print(df)
    df.to_csv(
      r'C:\QVD_DATA\COM_GARMENT\NYT\PENDING_STOCK_LASTYEAR.csv',encoding='utf-8-sig', index=False)
    conn.close()
    sendLine("pending_stock_lastyear()")

def rm_cost_petch():
    sql = """ SELECT 'PETCH' SITE,
       MTTR.REF_TYPE, 
	   CASE WHEN mttr.ref_type = 'P' THEN 'Purchase'
			 WHEN mttr.ref_type = 'I' AND mttr.trans_type = 'M' THEN 'Move In'
			 WHEN mttr.ref_type = 'J' AND mttr.trans_type = 'W' THEN 'Return from Production'
			 WHEN mttr.ref_type = 'J' AND mttr.trans_type = 'F' THEN 'Repack'
			 WHEN mttr.ref_type = 'I' AND mttr.trans_type = 'P' THEN 'Adjust'
			 WHEN mttr.ref_type = 'R' AND mttr.trans_type = 'W' THEN 'Return from Customer'
		END AS REF_TYPE_DESC,
		CONVERT(VARCHAR(10),MTTR.TRANS_DATE, 103) TRANS_DATE,
       MTTR.REF_NUM,
	   (SELECT [UF_INVOICE]
		FROM PETCH_APP.DBO.LOT
		WHERE LOT.LOT = MTTR.LOT
			  AND LOT.ITEM = ITM.ITEM) As INVOICE,
      CASE WHEN SUBSTRING (itm.item,1,4) = '0211' THEN 'Raw Cotton'
			WHEN SUBSTRING (itm.item,1,4) = '0212' THEN 'Pima Raw Cotton'
			WHEN SUBSTRING (itm.item,1,4) = '0213' THEN 'Cotton ORG'
			WHEN SUBSTRING (itm.item,1,4) = '0221' THEN 'Polyester'
			WHEN SUBSTRING (itm.item,1,4) = '0222' THEN 'Modal'
			WHEN SUBSTRING (itm.item,1,4) = '0223' THEN 'Rayon'
			WHEN SUBSTRING (itm.item,1,4) = '0224' THEN 'Acrylic'
		END AS ITEM_GROUP,
		MTTR.ITEM, ITM.DESCRIPTION,
		MTTR.TRANS_NUM TRANS_NUM,  
		MTTR.QTY AS Quantity_KGS, 
		(MTTR.QTY * 2.2046) AS Quantity_LBS,
		NYT.DBO.STD_COST(ITM.ITEM, MTTR.TRANS_DATE, MTTR.TRANS_DATE,'SAI5') AS STD_Per_Kg,
		(MTTR.QTY * NYT.DBO.STD_COST(ITM.ITEM, MTTR.TRANS_DATE, MTTR.TRANS_DATE,'SAI5')) As STD_Amount,
		(SELECT CASE WHEN POI.U_M = 'LB' THEN (POI.UNIT_MAT_COST_CONV * PO.EXCH_RATE) * 2.2046
				     ELSE(POI.UNIT_MAT_COST_CONV * PO.EXCH_RATE)
			    END AS RATE
		 FROM  PETCH_APP.DBO.PO AS PO
			   LEFT OUTER JOIN PETCH_APP.DBO.POITEM AS POI
			   ON PO.PO_NUM = POI.PO_NUM
	     WHERE POI.PO_NUM = MTTR.REF_NUM
			AND POI.PO_LINE = MTTR.REF_LINE_SUF
			AND POI.PO_RELEASE = MTTR.REF_RELEASE
		) AS ACTUAL_Per_Kg,
		((SELECT CASE WHEN POI.U_M = 'LB' THEN (POI.UNIT_MAT_COST_CONV * PO.EXCH_RATE) * 2.2046
				     ELSE(POI.UNIT_MAT_COST_CONV * PO.EXCH_RATE)
			    END AS RATE
		 FROM  PETCH_APP.DBO.PO AS PO
			   LEFT OUTER JOIN PETCH_APP.DBO.POITEM AS POI
			   ON PO.PO_NUM = POI.PO_NUM
	     WHERE POI.PO_NUM = MTTR.REF_NUM
			AND POI.PO_LINE = MTTR.REF_LINE_SUF
			AND POI.PO_RELEASE = MTTR.REF_RELEASE) * MTTR.QTY) As ACTUAL_Amount
		FROM PETCH_APP.DBO.MATLTRAN AS MTTR 
		LEFT OUTER JOIN PETCH_APP.DBO.TRANSFER AS TRANSFER
		ON TRANSFER.TRN_NUM = MTTR.REF_NUM
		LEFT OUTER JOIN PETCH_APP.DBO.ITEM AS ITM 
		ON ITM.ITEM = MTTR.ITEM
		LEFT OUTER JOIN PETCH_APP.DBO.MATERIALTRANSACTIONSVIEW AS MATLTRANVIEW 
		ON MATLTRANVIEW.TRANS_NUM = MTTR.TRANS_NUM
		LEFT OUTER JOIN PETCH_APP.DBO.FRZCOST AS FRZ
		ON FRZ.ITEM = ITM.ITEM
		WHERE 
		(  (MTTR.REF_TYPE = 'P' AND MTTR.QTY <> 0)
			OR (MTTR.REF_TYPE = 'I' AND MTTR.TRANS_TYPE = 'M' AND MTTR.QTY > 0)
			OR (MTTR.REF_TYPE = 'J' AND MTTR.TRANS_TYPE = 'W')
			OR (MTTR.REF_TYPE = 'J' AND MTTR.TRANS_TYPE = 'F' AND SUBSTRING (MTTR.REF_NUM,5,1) = 'R' AND MTTR.QTY > 0)
			OR (MTTR.REF_TYPE = 'I' AND MTTR.TRANS_TYPE = 'P' AND MTTR.QTY > 0)
			OR (MTTR.REF_TYPE = 'R' AND MTTR.TRANS_TYPE = 'W')
		)
		AND ITM.ITEM LIKE '02%'
		AND CONVERT(date,  MTTR.TRANS_DATE, 103) >= CONVERT(date,'01/01/2015', 103) """
    conn = pymssql.connect(server, user, password, "NYT")
    df = pd.read_sql_query(sql, conn)
    print(df)
    df.to_csv(
      r'C:\QVD_DATA\COM_GARMENT\NYT\RM_COST_PETCH.csv',encoding='utf-8-sig', index=False)
    conn.close()
    sendLine("rm_cost_petch()")


def rm_cost_sai5():
    sql = """ SELECT 'SAI5' SITE,
       MTTR.REF_TYPE, 
	   CASE WHEN mttr.ref_type = 'P' THEN 'Purchase'
			 WHEN mttr.ref_type = 'I' AND mttr.trans_type = 'M' THEN 'Move In'
			 WHEN mttr.ref_type = 'J' AND mttr.trans_type = 'W' THEN 'Return from Production'
			 WHEN mttr.ref_type = 'J' AND mttr.trans_type = 'F' THEN 'Repack'
			 WHEN mttr.ref_type = 'I' AND mttr.trans_type = 'P' THEN 'Adjust'
			 WHEN mttr.ref_type = 'R' AND mttr.trans_type = 'W' THEN 'Return from Customer'
		END AS REF_TYPE_DESC,
		CONVERT(VARCHAR(10),MTTR.TRANS_DATE, 103) TRANS_DATE,
       MTTR.REF_NUM,
	   (SELECT [UF_INVOICE]
		FROM SAI5_APP.DBO.LOT
		WHERE LOT.LOT = MTTR.LOT
			  AND LOT.ITEM = ITM.ITEM) As INVOICE,
      CASE WHEN SUBSTRING (itm.item,1,4) = '0211' THEN 'Raw Cotton'
			WHEN SUBSTRING (itm.item,1,4) = '0212' THEN 'Pima Raw Cotton'
			WHEN SUBSTRING (itm.item,1,4) = '0213' THEN 'Cotton ORG'
			WHEN SUBSTRING (itm.item,1,4) = '0221' THEN 'Polyester'
			WHEN SUBSTRING (itm.item,1,4) = '0222' THEN 'Modal'
			WHEN SUBSTRING (itm.item,1,4) = '0223' THEN 'Rayon'
			WHEN SUBSTRING (itm.item,1,4) = '0224' THEN 'Acrylic'
		END AS ITEM_GROUP,
		MTTR.ITEM, ITM.DESCRIPTION,
		MTTR.TRANS_NUM TRANS_NUM,  
		MTTR.QTY AS Quantity_KGS, 
		(MTTR.QTY * 2.2046) AS Quantity_LBS,
		NYT.DBO.STD_COST(ITM.ITEM, MTTR.TRANS_DATE, MTTR.TRANS_DATE,'SAI5') AS STD_Per_Kg,
		(MTTR.QTY * NYT.DBO.STD_COST(ITM.ITEM, MTTR.TRANS_DATE, MTTR.TRANS_DATE,'SAI5')) As STD_Amount,
		(SELECT CASE WHEN POI.U_M = 'LB' THEN (POI.UNIT_MAT_COST_CONV * PO.EXCH_RATE) * 2.2046
				     ELSE(POI.UNIT_MAT_COST_CONV * PO.EXCH_RATE)
			    END AS RATE
		 FROM  SAI5_APP.DBO.PO AS PO
			   LEFT OUTER JOIN SAI5_APP.DBO.POITEM AS POI
			   ON PO.PO_NUM = POI.PO_NUM
	     WHERE POI.PO_NUM = MTTR.REF_NUM
			AND POI.PO_LINE = MTTR.REF_LINE_SUF
			AND POI.PO_RELEASE = MTTR.REF_RELEASE
		) AS ACTUAL_Per_Kg,
		((SELECT CASE WHEN POI.U_M = 'LB' THEN (POI.UNIT_MAT_COST_CONV * PO.EXCH_RATE) * 2.2046
				     ELSE(POI.UNIT_MAT_COST_CONV * PO.EXCH_RATE)
			    END AS RATE
		 FROM  SAI5_APP.DBO.PO AS PO
			   LEFT OUTER JOIN SAI5_APP.DBO.POITEM AS POI
			   ON PO.PO_NUM = POI.PO_NUM
	     WHERE POI.PO_NUM = MTTR.REF_NUM
			AND POI.PO_LINE = MTTR.REF_LINE_SUF
			AND POI.PO_RELEASE = MTTR.REF_RELEASE) * MTTR.QTY) As ACTUAL_Amount
		FROM SAI5_APP.DBO.MATLTRAN AS MTTR 
		LEFT OUTER JOIN SAI5_APP.DBO.TRANSFER AS TRANSFER
		ON TRANSFER.TRN_NUM = MTTR.REF_NUM
		LEFT OUTER JOIN SAI5_APP.DBO.ITEM AS ITM 
		ON ITM.ITEM = MTTR.ITEM
		LEFT OUTER JOIN SAI5_APP.DBO.MATERIALTRANSACTIONSVIEW AS MATLTRANVIEW 
		ON MATLTRANVIEW.TRANS_NUM = MTTR.TRANS_NUM
		LEFT OUTER JOIN SAI5_APP.DBO.FRZCOST AS FRZ
		ON FRZ.ITEM = ITM.ITEM
		WHERE 
		(  (MTTR.REF_TYPE = 'P' AND MTTR.QTY <> 0)
			OR (MTTR.REF_TYPE = 'I' AND MTTR.TRANS_TYPE = 'M' AND MTTR.QTY > 0)
			OR (MTTR.REF_TYPE = 'J' AND MTTR.TRANS_TYPE = 'W')
			OR (MTTR.REF_TYPE = 'J' AND MTTR.TRANS_TYPE = 'F' AND SUBSTRING (MTTR.REF_NUM,5,1) = 'R' AND MTTR.QTY > 0)
			OR (MTTR.REF_TYPE = 'I' AND MTTR.TRANS_TYPE = 'P' AND MTTR.QTY > 0)
			OR (MTTR.REF_TYPE = 'R' AND MTTR.TRANS_TYPE = 'W')
		)
		AND ITM.ITEM LIKE '02%'
		AND CONVERT(date,  MTTR.TRANS_DATE, 103) >= CONVERT(date,'01/01/2015', 103) """
    conn = pymssql.connect(server, user, password, "NYT")
    df = pd.read_sql_query(sql, conn)
    print(df)
    df.to_csv(
      r'C:\QVD_DATA\COM_GARMENT\NYT\RM_COST_SAI5.csv',encoding='utf-8-sig', index=False)
    conn.close()
    sendLine("rm_cost_sai5()")


def RM_YARN_STOCK_ONHAND_PETCH():
	sql = """ SELECT DISTINCT RM_ONHAND.PR_NO,RM_ONHAND.INV_NO,RM_ONHAND.ITEM,RM_ONHAND.ITEM_DESC,
	RM_ONHAND.LOT,RM_ONHAND.UNIT,RM_ONHAND.ONHAND_UM,SUM(RM_ONHAND.RECEIVED_QTY) RECEIVED_QTY,
	SUM(RM_ONHAND.ONHAND_QTY) ONHAND_QTY
	FROM (SELECT LOT.ITEM ,ITM.DESCRIPTION AS ITEM_DESC,
	ITM.U_M AS ONHAND_UM ,'BLAE' AS UNIT,LOT.LOT ,LOT.RCVD_QTY AS RECEIVED_QTY,
	(ISNULL((SELECT SUM(ISNULL(LTLC.QTY_ON_HAND,0)) 
	FROM PETCH_APP.DBO.LOT_LOC AS LTLC 
	WHERE LTLC.ITEM = LOT.ITEM 
	AND LTLC.LOT = LOT.LOT),0)) AS ONHAND_QTY,
	(SELECT TOP 1 REF_NUM FROM PETCH_APP.DBO.MATLTRACK AS MATL 
	WHERE MATL.ITEM = LOT.ITEM AND MATL.LOT = LOT.LOT 
	AND MATL.TRACK_TYPE = 'R' ORDER BY MATL.TRACK_NUM)AS PR_NO,
	(SELECT TOP 1 ERP_LOG.PO
	FROM PETCH_APP.DBO.ERPP_NYT_INTERFACEPO_LOG AS ERP_LOG 
	WHERE ERP_LOG.ITEM = LOT.ITEM
	AND ERP_LOG.BALE_ID = LOT.LOT) AS INV_NO
	FROM  PETCH_APP.DBO.LOT AS LOT
	INNER JOIN PETCH_APP.DBO.ITEM AS ITM 
	ON LOT.ITEM = ITM.ITEM AND LOT.ITEM LIKE '02%') AS RM_ONHAND
	WHERE RM_ONHAND.ONHAND_QTY <> 0
	GROUP BY RM_ONHAND.PR_NO,RM_ONHAND.INV_NO,RM_ONHAND.ITEM,RM_ONHAND.ITEM_DESC,
	RM_ONHAND.LOT,RM_ONHAND.UNIT,RM_ONHAND.ONHAND_UM
	ORDER BY RM_ONHAND.ITEM """
	conn = pymssql.connect(server, user, password, "NYT")
	df = pd.read_sql_query(sql, conn)
	print(df)
	df.to_csv(
		r'C:\QVD_DATA\COM_GARMENT\NYT\RMSTOCKPETCH.csv',encoding='utf-8-sig', index=False)
	conn.close()
	sendLine("RM_YARN_STOCK_ONHAND_PETCH()")


def RM_YARN_STOCK_ONHAND_SAI5():
	sql = """ SELECT DISTINCT RM_ONHAND.PR_NO,RM_ONHAND.INV_NO,RM_ONHAND.ITEM,RM_ONHAND.ITEM_DESC,
	RM_ONHAND.LOT,RM_ONHAND.UNIT,RM_ONHAND.ONHAND_UM,SUM(RM_ONHAND.RECEIVED_QTY) RECEIVED_QTY,SUM(RM_ONHAND.ONHAND_QTY) ONHAND_QTY
	FROM (SELECT LOT.ITEM ,ITM.DESCRIPTION AS ITEM_DESC,ITM.U_M AS ONHAND_UM ,'BLAE' AS UNIT,LOT.LOT ,LOT.RCVD_QTY AS RECEIVED_QTY,
	(ISNULL((SELECT SUM(ISNULL(LTLC.QTY_ON_HAND,0)) 
	FROM SAI5_APP.DBO.LOT_LOC AS LTLC 
	WHERE LTLC.ITEM = LOT.ITEM 
	AND LTLC.LOT = LOT.LOT),0)) AS ONHAND_QTY,
	(SELECT TOP 1 REF_NUM 
	FROM SAI5_APP.DBO.MATLTRACK AS MATL 
	WHERE MATL.ITEM = LOT.ITEM 
	AND MATL.LOT = LOT.LOT 
	AND MATL.TRACK_TYPE = 'R' 
	ORDER BY MATL.TRACK_NUM)AS PR_NO,
	(SELECT TOP 1 ERP_LOG.PO
	FROM SAI5_APP.DBO.ERPP_NYT_INTERFACEPO_LOG AS ERP_LOG 
	WHERE ERP_LOG.ITEM = LOT.ITEM
	AND ERP_LOG.BALE_ID = LOT.LOT) AS INV_NO
	FROM  SAI5_APP.DBO.LOT AS LOT
	INNER JOIN SAI5_APP.DBO.ITEM AS ITM 
	ON LOT.ITEM = ITM.ITEM 
	AND LOT.ITEM LIKE '02%') AS RM_ONHAND
	WHERE RM_ONHAND.ONHAND_QTY <> 0
	GROUP BY RM_ONHAND.PR_NO,RM_ONHAND.INV_NO,RM_ONHAND.ITEM,RM_ONHAND.ITEM_DESC,
	RM_ONHAND.LOT,RM_ONHAND.UNIT,RM_ONHAND.ONHAND_UM 
	ORDER BY RM_ONHAND.ITEM """
	conn = pymssql.connect(server, user, password, "NYT")
	df = pd.read_sql_query(sql, conn)
	print(df)
	df.to_csv(
		r'C:\QVD_DATA\COM_GARMENT\NYT\RMSTOCKSAI5.csv',encoding='utf-8-sig', index=False)
	conn.close()
	sendLine("RM_YARN_STOCK_ONHAND_SAI5()")

# yarn_stock_aging()
# net_income_product()
# net_income()
# yarn_to_store_petch()
# yarn_to_store_sai5()
# artran_all_not_i()
# receive_issue_nyt()
# pending_stock_lastyear()
# rm_cost_petch()
# rm_cost_sai5()


thread_list = []

# thread1 = threading.Thread(target=yarn_stock_aging)
# thread_list.append(thread1)
# thread1.start()

# thread2 = threading.Thread(target=net_income_product)
# thread_list.append(thread2)
# thread2.start()

# thread3 = threading.Thread(target=net_income)
# thread_list.append(thread3)
# thread3.start()

# thread4 = threading.Thread(target=yarn_to_store_petch)
# thread_list.append(thread4)
# thread4.start()

# Disable request by Sirada.S on 06-01-2022
# thread5 = threading.Thread(target=yarn_to_store_sai5)
# thread_list.append(thread5)
# thread5.start()

# Disable request by Sirada.S on 06-01-2022
# thread6 = threading.Thread(target=artran_all_not_i)
# thread_list.append(thread6)
# thread6.start()

thread7 = threading.Thread(target=RM_YARN_STOCK_ONHAND_SAI5)
thread_list.append(thread7)
thread7.start()

thread8 = threading.Thread(target=RM_YARN_STOCK_ONHAND_PETCH)
thread_list.append(thread8)
thread8.start()

# thread7 = threading.Thread(target=receive_issue_nyt)
# thread_list.append(thread7)
# thread7.start()

# thread8 = threading.Thread(target=pending_stock_lastyear)
# thread_list.append(thread8)
# thread8.start()

# thread9 = threading.Thread(target=rm_cost_petch)
# thread_list.append(thread9)
# thread9.start()

# thread10 = threading.Thread(target=rm_cost_sai5)
# thread_list.append(thread10)
# thread10.start()



