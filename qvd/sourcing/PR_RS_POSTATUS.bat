@echo on
call C:\GITProject\pyschedule\venv\Scripts\activate.bat
C:\GITProject\pyschedule\venv\Scripts\python.exe "C:\GITProject\pyschedule\qvd\sourcing\PR_RS_POSTATUS.py"

QVDistributionService.exe -r="C:\QlikView\Sourcing\PR&RS&POSTATUS.qvw"