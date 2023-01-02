@echo on
call C:\GITProject\pyschedule\venv\Scripts\activate.bat
C:\GITProject\pyschedule\venv\Scripts\python.exe "C:\GITProject\pyschedule\qvd\nyg\qvd_soft_rm_po_bom_xls.py"
C:\GITProject\pyschedule\venv\Scripts\python.exe "C:\GITProject\pyschedule\qvd\nyg\qvd_soft_rm_NY_ONHAND_AGING_xls.py"
C:\GITProject\pyschedule\venv\Scripts\python.exe "C:\GITProject\pyschedule\nyg\QVD_BILL_OF_MATERIALS.py"

