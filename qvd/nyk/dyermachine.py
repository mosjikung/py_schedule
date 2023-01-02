import os
import io
import numpy as np
import pandas as pd
import json
import requests
from pandas import json_normalize
import threading
from datetime import datetime, timedelta



def Artos01():
    dte = str(datetime.now())[0:10]
    url = """http://172.16.0.26/nyiot/NYK_dryer_api/dryer_speed_api.php?table_name=NYK_Speed_Prox_Artos01&from_date='2021-01-01 8:00:00'&to_date='{dte} 8:00:00'""".format(dte=dte)
    r = requests.get(url)
    data = r.json()
    df=json_normalize(data)
    # print(df)
    _csv = r"C:\QVD_DATA\dyermachine\Artos01.csv"
    df.to_csv(_csv, index=False)
    
def Artos02():
    dte = str(datetime.now())[0:10]
    url = """http://172.16.0.26/nyiot/NYK_dryer_api/dryer_speed_api.php?table_name=NYK_Speed_Prox_Artos02&from_date='2021-01-01 8:00:00'&to_date='{dte} 8:00:00'""".format(dte=dte)
    r = requests.get(url)
    data = r.json()
    df=json_normalize(data)
    _csv = r"C:\QVD_DATA\dyermachine\Artos02.csv"
    df.to_csv(_csv, index=False)

def Artos03():
    dte = str(datetime.now())[0:10]
    url = """http://172.16.0.26/nyiot/NYK_dryer_api/dryer_speed_api.php?table_name=NYK_Speed_Prox_Artos03&from_date='2021-01-01 8:00:00'&to_date='{dte} 8:00:00'""".format(dte=dte)
    r = requests.get(url)
    data = r.json()
    df=json_normalize(data)
    _csv = r"C:\QVD_DATA\dyermachine\Artos03.csv"
    df.to_csv(_csv, index=False)

def Bruckner():
    dte = str(datetime.now())[0:10]
    url = """http://172.16.0.26/nyiot/NYK_dryer_api/dryer_speed_api.php?table_name=NYK_Speed_Prox_Bruckner&from_date='2021-01-01 8:00:00'&to_date='{dte} 8:00:00'""".format(dte=dte)
    r = requests.get(url)
    data = r.json()
    df=json_normalize(data)
    _csv = r"C:\QVD_DATA\dyermachine\Bruckner.csv"
    df.to_csv(_csv, index=False)

def LK01():
    dte = str(datetime.now())[0:10]
    url = """http://172.16.0.26/nyiot/NYK_dryer_api/dryer_speed_api.php?table_name=NYK_Speed_Prox_LK01&from_date='2021-01-01 8:00:00'&to_date='{dte} 8:00:00'""".format(dte=dte)
    r = requests.get(url)
    data = r.json()
    df=json_normalize(data)
    _csv = r"C:\QVD_DATA\dyermachine\LK01.csv"
    df.to_csv(_csv, index=False)

def LK02():
    dte = str(datetime.now())[0:10]
    url = """http://172.16.0.26/nyiot/NYK_dryer_api/dryer_speed_api.php?table_name=NYK_Speed_Prox_LK02&from_date='2021-01-01 8:00:00'&to_date='{dte} 8:00:00'""".format(dte=dte)
    r = requests.get(url)
    data = r.json()
    df=json_normalize(data)
    _csv = r"C:\QVD_DATA\dyermachine\LK02.csv"
    df.to_csv(_csv, index=False)

def LK03():
    dte = str(datetime.now())[0:10]
    url = """http://172.16.0.26/nyiot/NYK_dryer_api/dryer_speed_api.php?table_name=NYK_Speed_Prox_LK03&from_date='2021-01-01 8:00:00'&to_date='{dte} 8:00:00'""".format(dte=dte)
    r = requests.get(url)
    data = r.json()
    df=json_normalize(data)
    _csv = r"C:\QVD_DATA\dyermachine\LK03.csv"
    df.to_csv(_csv, index=False)

def ShantaShink():
    dte = str(datetime.now())[0:10]
    url = """http://172.16.0.26/nyiot/NYK_dryer_api/dryer_speed_api.php?table_name=NYK_Speed_Prox_ShantaShink&from_date='2021-01-01 8:00:00'&to_date='{dte} 8:00:00'""".format(dte=dte)
    r = requests.get(url)
    data = r.json()
    df=json_normalize(data)
    _csv = r"C:\QVD_DATA\dyermachine\ShantaShink.csv"
    df.to_csv(_csv, index=False)

def Stentex():
    dte = str(datetime.now())[0:10]
    url = """http://172.16.0.26/nyiot/NYK_dryer_api/dryer_speed_api.php?table_name=NYK_Speed_Prox_Stentex&from_date='2021-01-01 8:00:00'&to_date='{dte} 8:00:00'""".format(dte=dte)
    r = requests.get(url)
    data = r.json()
    df=json_normalize(data)
    _csv = r"C:\QVD_DATA\dyermachine\Stentex.csv"
    df.to_csv(_csv, index=False)

Artos01()
Artos02()
Artos03()
Bruckner()
LK01()
LK02()
LK03()
ShantaShink()
Stentex()

