'''call information using api key'''

import urllib.request
import urllib.parse
import json
import pandas as pd
import os
import datetime
import time
import sys
import requests
import xmltodict
import json

api_key = "RsxNAeIynUGQwr0R7jl77ZXfvWjtz5EaJ7dKi%2FgGCAeNdyiR3%2BL23lLmY7cLFUb%2FGM3WzkFH%2BKAtu2oEz9q2Kw%3D%3D"
url = "http://apis.data.go.kr/6410000/buslocationservice/getBusLocationList" # 실시간 버스위치정보 조회
routeID = '233000125'  # BusID

params ={'serviceKey' : api_key, 'routeId' : routeID } # url 뒤에 들어가는 parameter

response = requests.get(url, params=params)
print(response.content)

# from response xml file to json file
json_data = json.loads(json.dumps(xmltodict.parse(response.content)))

# setting dataframe view wider to see long item
pd.set_option('display.max_colwidth', -1)

# from json file to dataframe
df = pd.DataFrame(json_data['response']['msgBody']["busLocationList"])
print(df.head())