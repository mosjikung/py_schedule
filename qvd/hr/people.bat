@echo on
call C:\GITProject\pyschedule\venv\Scripts\activate.bat
C:\GITProject\pyschedule\venv\Scripts\python.exe "C:\GITProject\pyschedule\qvd\hr\people.py"
REM ftp -s:C:\GITProject\pyschedule\qvd\hr\peopleFTP.txt
TIMEOUT 5