# import os

# temp_var=r"C:\Program Files\QlikView\Distribution Service\QVDistributionService.exe"
# os.system(temp_var)

# os.system('"C:/Program Files/QlikView/Distribution Service/QVDistributionService.exe" -r="C:\QlikView\QVW\GARMENT\Garment\QC PASS.qvw"')

import subprocess
subprocess.Popen('"C:\Program Files\QlikView\Distribution Service\QVDistributionService.exe" -r="C:\QlikView\QVW\GARMENT\Garment\QC PASS.qvw"')