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
import pdb

# ============================================================================================================================
def apiKey():
    return "RsxNAeIynUGQwr0R7jl77ZXfvWjtz5EaJ7dKi%2FgGCAeNdyiR3%2BL23lLmY7cLFUb%2FGM3WzkFH%2BKAtu2oEz9q2Kw%3D%3D"
def getParams(routeId) :
    return {'serviceKey' : apiKey(), 'routeId' : routeId} 
def apiDefaultUrl():
    return "http://apis.data.go.kr/6410000/buslocationservice/getBusLocationList"

def NewDirName():
    return str(datetime.date.today())
def getapiUrlByParam(routeId):
    return requests.get(apiDefaultUrl(), params=getParams(routeId))
def getBasePath(account_Id):
    return "/home/" + account_Id + "/gbisOpenApi/realtimePosition"

def getapiCall(account_Id, callCnt, routeNum, routeId, reqTime) :
    basePath = getBasePath(account_Id)
    
    callCnt += 1
    response = getapiUrlByParam(routeId)
    compTime = time.time() - reqTime
    compTimeStr = "compTime:"+str(compTime)+"sec"
    
    json_data = json.loads(json.dumps(xmltodict.parse(response.content)))
    
    print(json_data)
    

    # setting dataframe view wider to see long item
    pd.set_option('display.max_colwidth', None)

    # from json file to dataframe
    df = pd.DataFrame(json_data['response']['msgBody']["busLocationList"])
    print(df.head())

# ========== main

def main():
    account_Id = "hyun_temp"
    busIdDict = {"1002":"233000140", "1008":"233000125"} #추후 matchingTable로 변환 예정
    
    for key,value in busIdDict.items() :
        reqTime = time.time()
        getapiCall(account_Id, 0, key, value, reqTime)
    
    
    #response = getapiUrlByParam()

if __name__ == '__main__':
    main()