-- Check Data WEB_90100010610_OE
SELECT  EXTRACT(year FROM SO_NO_DATE) ||'/'|| LPAD(EXTRACT(month FROM SO_NO_DATE),2,'0') as YEAR_MONTH ,count(*) as total_row
-- , round(sum(ACT_ORDER_QTY)/1000,0) as ACT_ORDER_QTY_TONS
FROM NYIS.WEB_90100010610_OE
WHERE EXTRACT(year FROM SO_NO_DATE) = 2022  -- and EXTRACT(month FROM SO_NO_DATE) = 1 
GROUP BY  EXTRACT(year FROM SO_NO_DATE) ||'/'|| LPAD(EXTRACT(month FROM SO_NO_DATE),2,'0')
ORDER BY  EXTRACT(year FROM SO_NO_DATE) ||'/'|| LPAD(EXTRACT(month FROM SO_NO_DATE),2,'0')


SYSDATE	YEAR_MONTH	TOTAL_ROW	ACT_ORDER_QTY_TONS
4/2/2565 15:21:45	2022/01	3241	2335
4/2/2565 15:21:45	2022/02	192	118

3575



YEAR_MONTH	TOTAL_ROW
2022/01	3267
2022/02	308


SELECT EXTRACT(year FROM SO_NO_DATE) 
FROM WEB_90100010610_OE
GROUP BY EXTRACT(year FROM SO_NO_DATE) 
ORDER BY EXTRACT(year FROM SO_NO_DATE)



SELECT SUM(ORDER_KGS) as K FROM WEB_90100010610_OE
WHERE EXTRACT(year FROM SO_NO_DATE) = 2019 
--and EXTRACT(month FROM SO_NO_DATE) = 1 
ORDER BY SO_NO, SO_LINE



SELECT * FROM EXPORT_OE_ACTUAL

data_batch_production_cost


  SELECT  EXTRACT(year FROM M.SO_NO_DATE) ||'/'|| LPAD(EXTRACT(month FROM M.SO_NO_DATE),2,'0') as YEAR_MONTH 
  ,count(*) as total_row
  --, round(sum(M.ACT_ORDER_QTY)/1000,0) as ACT_ORDER_QTY_TONS    
  --, sum(M.OE_ORDER) as OE_ORDER 
  -- , DECODE(MAX(M.FINISHING_TYPE), 'ͺ???',  M.T2, M.T1) as T
  FROM (     
              SELECT M.*, I.ITEM_DESC, SF5.GET_ITEM_CATEGORY_FOR_SALES(M.OE_SO_ITEM_GREY) ITEM_CATEGORY_SALES , I.ITEM_CATEGORY, I.ITEM_STRUCTURE,I.MACHINE_GROUP,
              I.O_FN_OPEN, I.O_FN_TUBULAR, I.O_FN_YARD, I.O_FN_GM, I.O_YARN_COUNT, I.O_GAUGE, I.O_MAT_CONS, 
              --CASE WHEN (INSTR(OE_SO_UOM,'KG') > 0) THEN OE_ORDER ELSE OE_ORDER/5 END ACT_ORDER_QTY
              OE_ORDER ACT_ORDER_QTY
              ,(SELECT MAX(SO_RESERVE) FROM DUMMY_SO_HEADERS H WHERE H.ORA_ORDER_NUMBER=M.SO_NO) SO_RESERVE 
              ,(SELECT MAX(SO_BILL_REF) FROM DUMMY_SO_HEADERS H WHERE H.ORA_ORDER_NUMBER=M.SO_NO) SO_BILL_REF 
              ,(SELECT MAX(SO_TYPE) FROM DUMMY_SO_HEADERS H WHERE H.ORA_ORDER_NUMBER=M.SO_NO) SO_TYPE 
              ,(SELECT MAX(DIVISION_CODE) FROM NYIS.PS_SALES_SECURITY@BIS.WORLD S WHERE S.SALES_CODE = M.OE_SALE_ID ) DIVISION_CODE_NEW
              ,(SELECT MAX(TEAM_CODE) FROM NYIS.PS_SALES_SECURITY@BIS.WORLD S WHERE S.SALES_CODE = M.OE_SALE_ID  ) TEAM_NAME_NEW
              ,NULL ORDER_KGS     
             /* ,(
                SELECT  MAX(TO_CHAR(ROUND( 1000/((O_FN_GM*0.023223)*(NVL(O_FN_TUBULAR , O_FN_OPEN )+2))/2  ,2),'990.90'))  T1             
                FROM SF5.FMIT_ITEM F   
                WHERE ITEM_CODE=OE_SO_ITEM_GREY
              ) as T1  
              ,(
                SELECT  MAX(TO_CHAR(ROUND( 1000/((O_FN_GM*0.023223)*(  NVL(O_FN_OPEN , O_FN_TUBULAR) +2))  ,2),'990.90'))   T2             
                FROM SF5.FMIT_ITEM F   
                WHERE ITEM_CODE=OE_SO_ITEM_GREY
              ) as T2 */
              FROM SF5.SF5_GAP_SO_OE_LINE_V M, SF5.FMIT_ITEM I 
              WHERE  M.OE_SO_ITEM_GREY = I.ITEM_CODE(+)  
             -- AND EXTRACT(month FROM SO_NO_DATE) = {month}
              AND EXTRACT(year FROM SO_NO_DATE) = 2022
              AND OE_SO_ITEM LIKE 'F%'              
    ) M 
    GROUP BY  EXTRACT(year FROM M.SO_NO_DATE) ||'/'|| LPAD(EXTRACT(month FROM M.SO_NO_DATE),2,'0') --, M.FINISHING_TYPE
ORDER BY  EXTRACT(year FROM M.SO_NO_DATE) ||'/'|| LPAD(EXTRACT(month FROM M.SO_NO_DATE),2,'0')


YEAR_MONTH	TOTAL_ROW	ACT_ORDER_QTY_TONS
2022/01	3217	4862
2022/02	118	183



  SELECT count(M.SO_NO_DATE) as cnt FROM (
              SELECT M.*, I.ITEM_DESC, SF5.GET_ITEM_CATEGORY_FOR_SALES(M.OE_SO_ITEM_GREY) ITEM_CATEGORY_SALES , I.ITEM_CATEGORY, I.ITEM_STRUCTURE,I.MACHINE_GROUP,
              I.O_FN_OPEN, I.O_FN_TUBULAR, I.O_FN_YARD, I.O_FN_GM, I.O_YARN_COUNT, I.O_GAUGE, I.O_MAT_CONS, 
              --CASE WHEN (INSTR(OE_SO_UOM,'KG') > 0) THEN OE_ORDER ELSE OE_ORDER/5 END ACT_ORDER_QTY
              OE_ORDER ACT_ORDER_QTY
              ,(SELECT MAX(SO_RESERVE) FROM DUMMY_SO_HEADERS H WHERE H.ORA_ORDER_NUMBER=M.SO_NO) SO_RESERVE 
              ,(SELECT MAX(SO_BILL_REF) FROM DUMMY_SO_HEADERS H WHERE H.ORA_ORDER_NUMBER=M.SO_NO) SO_BILL_REF 
              ,(SELECT MAX(SO_TYPE) FROM DUMMY_SO_HEADERS H WHERE H.ORA_ORDER_NUMBER=M.SO_NO) SO_TYPE 
              ,(SELECT MAX(DIVISION_CODE) FROM NYIS.PS_SALES_SECURITY@BIS.WORLD S WHERE S.SALES_CODE = M.OE_SALE_ID ) DIVISION_CODE_NEW
              ,(SELECT MAX(TEAM_CODE) FROM NYIS.PS_SALES_SECURITY@BIS.WORLD S WHERE S.SALES_CODE = M.OE_SALE_ID  ) TEAM_NAME_NEW
              ,NULL ORDER_KGS
              FROM SF5.SF5_GAP_SO_OE_LINE_V M, SF5.FMIT_ITEM I 
              WHERE 1 = 1
              AND EXTRACT(year FROM SO_NO_DATE) = 2022
              --AND EXTRACT(month FROM SO_NO_DATE) = {month}
              AND M.OE_SO_ITEM_GREY = I.ITEM_CODE(+) 
              AND OE_SO_ITEM LIKE 'F%' ) M
              WHERE 1 = 1   
              
              
              
              
SELECT M.*
    ,D.YARN_COST, D.PRINT_COST, D.FINISING_COST, D.TOTAL_COST_KG, D.YARN_COST_AMOUNT, D.PRINT_COST_AMOUNT, D.FINISING_COST_AMOUNT, D.TOTAL_COST_AMOUNT, D.OE_GAP, D.ACT_ORDER_QTY, D.OE_AMOUNT
    ,D.SO_TYPE, D.ORDER_KGS
    ,cal_Mps_dye_hr_new(D.ITEM_CODE,D.COLOR_CODE) STD_DYE_HR
    -- SELECT COUNT(*)  
-- SELECT SUM(D.ACT_ORDER_QTY)/1000 as dd
FROM SF5.SF5_GAP_SO_OE_LINE M, NYIS.WEB_90100010610_OE D
WHERE EXTRACT(YEAR FROM M.SO_NO_DATE)  = 2022
AND M.SO_NO = D.SO_NO(+)
AND M.SO_LINE = D.SO_LINE(+)
--ORDER BY M.SO_NO_DATE , M.SO_NO, M.SO_LINE