@echo on
call C:\GITProject\pyschedule\venv\Scripts\activate.bat
C:\GITProject\pyschedule\venv\Scripts\python.exe "C:\GITProject\pyschedule\qvd\nyg\qcPass.py"
C:\"Program Files\QlikView\Distribution Service\QVDistributionService.exe" -r="C:\QlikView\QVW\GARMENT\Garment\QC PASS.qvw"  
