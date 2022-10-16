'''call information using api key'''
from multiprocessing.sharedctypes import Value
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
def SavedftoCSVFile(df, basePath, routeNum, dirName, compTimeStr):
    df.to_csv(getResultPath(basePath, routeNum, dirName)+"/"+datetime.datetime.now().strftime("%Y%m%d-%H%M%S")+"_"+routeNum+"_rTimeBusPos_"+compTimeStr+'.csv', encoding='utf-8-sig', index=False)

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

        # setting dataframe view wider to see long item
        pd.set_option('display.max_colwidth', None)

        # from json file to dataframe
        df = pd.DataFrame(json_data['response']['msgBody']["busLocationList"])
        SavedftoCSVFile(df, basePath, routeNum, dirName, compTimeStr)
        print(df.head())
        
    except ValueError as ValueErrorMessage :
        if str(ValueErrorMessage) == 'If using all scalar values, you must pass an index' : # Case : if there is only 1 bus left (less than 2)
            df = pd.DataFrame(json_data['response']['msgBody']["busLocationList"], index=[0]) # type(df) is dict
            SavedftoCSVFile(df, basePath, routeNum, dirName, compTimeStr)
            
    except Exception as es:
        if callCnt < 5 :
            print("\n\nRetry - callCnt :" , str(callCnt))
            getapiCall(account_Id, callCnt, routeNum, routeId, reqTime)
        else :
            errorReport = open(getResultPath(basePath, routeNum, dirName)+"/"+datetime.datetime.now().strftime("%Y%m%d-%H%M%S")+"_"+routeNum+"_rTimeBusPos_"+"errorReport.txt", 'w')
            errorReport.write(str(json_data['response']))
            errorReport.close()
            
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