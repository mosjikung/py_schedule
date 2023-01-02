import mysql.connector

mydb = mysql.connector.connect(
  host="172.16.9.20",
  user="kkk",
  password="Spe@ker9",
  database="nytg_center"
)

mycursor = mydb.cursor()

mycursor.execute(""" SELECT 
                EM_CODE_NEW*100000 AS ROW_ID,
                EM_CODE_NEW AS EMP,
                EM_CODE_OLD AS CANO,
                CONCAT(FIRSTNAME_ENG, ' ', LASTNAME_ENG) AS NA_TH,
                CONCAT(FIRSTNAME_ENG, ' ', LASTNAME_ENG) AS NA_EN,
                NICKNAME_TH AS NIC,
                EMAIL_ADDRESS AS MAIL,
                BU_HRIS AS BU,
                DEPARTMENT_HRIS AS DEPT,
                POSITION_HRIS AS POS_TH,
                LEVEL_HRIS AS POS_EN,
                'vn' AS F_DB,
                PHONE_EXT AS EXT
            FROM km_employee_new 
            WHERE STATUS_ACTIVE = 'Y' """)

myresult = mycursor.fetchall()

for x in myresult:
  print(x)