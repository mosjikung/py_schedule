@echo on
call C:\GITProject\pyschedule\venv\Scripts\activate.bat
C:\GITProject\pyschedule\venv\Scripts\python.exe "C:\GITProject\pyschedule\qvd\nyg\qvd_CONTROL_ROOM_PO_OTP_QVD.py"
C:\GITProject\pyschedule\venv\Scripts\python.exe "C:\GITProject\pyschedule\nyg\QVD_SALE_ORDER_TRANSFER.py"
C:\GITProject\pyschedule\venv\Scripts\python.exe "C:\GITProject\pyschedule\nyg\QVD_PO_BOM_GRW.py"


