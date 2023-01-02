import requests
import numpy as np
import pandas as pd

# api-endpoint
URL = "http://sfc.nanyangtextile.com/qcgarmentapp/api/wsdata.php/getdatasammary"

# location given here
location = "delhi technological university"

# defining a params dict for the parameters to be sent to the API
PARAMS = {'uid': 'IT', 'location': 'G1', 'jobdate': '2020/08/27'}

# sending get request and saving the response as response object
r = requests.post(url=URL, params=PARAMS)

# extracting data in json format
data = r.json()
# print(data['vdata'])

# df = pd.read_json(data['vdata'])
df = pd.json_normalize(data['vdata'])
df.to_excel('locationOfISS.xlsx')

# print(df)
