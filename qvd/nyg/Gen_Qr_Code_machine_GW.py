import qrcode
import win32api
#import qrcode.image.svg
import cx_Oracle
import os, sys
from PIL import Image

inPath = '//172.16.0.49/Machine_Soft_System/IMGQR/'
outPath = "\\"+"\\172.16.0.49\\Machine_Soft_System\\IMGQR\\IM_JPG\\"

oh="//172.16.0.49/Machine_Soft_System/Instantclient_12_2"
os.environ["ORACLE_HOME"]=oh
os.environ["PATH"]=oh+os.pathsep+os.environ["PATH"]
my_dsn = cx_Oracle.makedsn("172.16.6.83",port=1521,sid="NYTG")

def Gen_QrCode(PP_TAG,PPQR_PATH):
	try:
		qr = qrcode.QRCode(version=1,error_correction=qrcode.constants.ERROR_CORRECT_H,box_size=10,border=4,)
		qr.add_data(PP_TAG)
		qr.make(fit=True)
		img = qr.make_image(fill_color="black", back_color="white")
		img.save(inPath+"IM_PNG/"+PP_TAG+".png")
		im = Image.open(inPath+"IM_PNG/"+PP_TAG+".png")
		im.convert('RGB').save(inPath+"IM_JPG/"+PP_TAG+".jpg","JPEG")
		im = Image.open(inPath+"IM_JPG/"+PP_TAG+".jpg")
		Insert_Link_Img(PP_TAG,PPQR_PATH)
	except Exception as e:
		print("Error")

def Insert_Link_Img(P_TAG,PQR_PATH):
	InCon_Ora = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
	Incursor_Ora = InCon_Ora.cursor()
	try:
		Incursor_Ora.execute("""Update DFIT_MC_CONTROL De Set De.QR_PATH = :VQR_PATH
			Where De.QR_PATH Is Null And De.mc_sys_id = nvl(:PTAG,De.mc_sys_id) And De.QR_PATH Is Null
			And De.mc_STATUS NOT IN('SALE','Sale Out')
			and de.ou_code='G1'
			and nvl(de.MC_ACTIVE,'Y')  ='Y'
			--and de.mc_serial ='1607061'
			""",{'VQR_PATH' :PQR_PATH,'PTAG' :P_TAG})
		InCon_Ora.commit()
	except cx_Oracle.DatabaseError as e:
		print("Error")

	InCon_Ora.close()

Con_Ora = cx_Oracle.connect(user="nygm", password="nygm", dsn=my_dsn)
cursor_Ora = Con_Ora.cursor()
cursor_Ora.execute("""Select Distinct De.QR_PATH,De.mc_sys_id 
	                  From DFIT_MC_CONTROL De
	                  Where De.QR_PATH Is Null
	                  And De.mc_STATUS NOT IN('SALE','Sale Out')
					  and de.ou_code='G1'
					  and nvl(de.MC_ACTIVE,'Y')  ='Y'
					  --and de.mc_serial ='1607061'
					  """)

COUNT_LOOP = 0
for QR_PATH,ITEM_TAG in cursor_Ora:
	if ITEM_TAG is not None:
		COUNT_LOOP =(COUNT_LOOP+1)
		Gen_QrCode(ITEM_TAG,(outPath+ITEM_TAG+".jpg"))		
Con_Ora.close()

win32api.MessageBox(0,'GenQrcode Soft Complete '+str(COUNT_LOOP)+' Barcode', 'Message alert')