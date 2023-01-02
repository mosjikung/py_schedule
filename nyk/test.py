import datetime
from datetime import datetime, timedelta


_date = datetime.now()
_dts = str(_date)[0:13].replace("-", "/")+':00'
print(_dts)