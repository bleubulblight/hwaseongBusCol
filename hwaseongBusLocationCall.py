'''call information using api key'''

from email.encoders import encode_noop
from locale import D_FMT
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
def apiKey(): #decoding
    return "RsxNAeIynUGQwr0R7jl77ZXfvWjtz5EaJ7dKi/gGCAeNdyiR3+L23lLmY7cLFUb/GM3WzkFH+KAtu2oEz9q2Kw=="
def getParams(routeId) :
    return {'serviceKey' : apiKey(), 'routeId' : routeId} 
def apiDefaultUrl():
    return "http://apis.data.go.kr/6410000/buslocationservice/getBusLocationList"

def getNewDirName():
    return str(datetime.date.today())
def getapiUrlByParam(routeId):
    return requests.get(apiDefaultUrl(), params=getParams(routeId))
def getBasePath(account_Id):
    return "/home/" + account_Id + "/gbisOpenApi/realtimePosition" ## changing the account ID when you use it on the other PC
def getResultPath(basePath, routeNum, dirName):
    return basePath+'/results_pos/'+routeNum+'/'+dirName

def getapiCall(account_Id, callCnt, routeNum, routeId, reqTime) : #routeNum : 버스번호 , routeId : Api 서버 상 버스ID
    basePath = getBasePath(account_Id)
    
    
    
    try :
        dirName=getNewDirName()
        if not os.path.exists(getResultPath(basePath, routeNum, dirName)):
            os.makedirs(getResultPath(basePath, routeNum, dirName))
            
        callCnt += 1
        response = getapiUrlByParam(routeId)
        compTime = round((time.time() - reqTime), 5)
        compTimeStr = "compTime-"+str(compTime)+"sec"
        
        json_data = json.loads(json.dumps(xmltodict.parse(response.content)))
        
        print(json_data)

        # from json file to dataframe
        df = pd.DataFrame(json_data['response']['msgBody']["busLocationList"])
        df.to_csv(getResultPath(basePath, routeNum, dirName)+"/"+datetime.datetime.now().strftime("%Y%m%d-%H%M%S")+"_"+routeNum+"_rTimeBusPos_"+compTimeStr+'.csv', encoding='utf-8-sig', index=False)
        print(df.head())
        
    
    except Exception as es:
        if callCnt < 5 :
            print("\n\nRetry - callCnt :" , str(callCnt))
            getapiCall(account_Id, callCnt, routeNum, routeId, reqTime)
        else :
            print(str(es) + "\n\n")

# Read All Files in PATH
def getAllFiles(basePath):
    onlyfiles = [f for f in os.listdir(basePath) if os.isfile(os.join(basePath, f)) and not f.startswith(".")]
    return onlyfiles
# Read All CSV Files in PATH
def readAllCsv(basePath,files):
    dataframes = [pd.read_csv(os.join(basePath,f)) for f in files]
    return dataframes
def concatAllDataframes(dataframes):
    return pd.concat(dataframes, axis=0, ignore_index=True)

# ========== main

def main():
    account_Id = "bleubulblight" # changing the account ID when you use it on the other PC
    busIdDict = {"1002":"233000140", "1008":"233000125"} #추후 matchingTable로 변환 예정
    
    for key,value in busIdDict.items() :
        reqTime = time.time()
        getapiCall(account_Id, 0, key, value, reqTime)
    
    #response = getapiUrlByParam()

if __name__ == '__main__':
    main()