'''call information using api key'''

from asyncore import read
from email.encoders import encode_noop
from lib2to3.pytree import BasePattern
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
import glob

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
            

# 경로를 받아서 모든 csv 파일의 리스트를 반환하는 함수
def getAllCsvList(resultPath):
    files = glob.glob(resultPath+'/*.csv')
    return files

# getAllCsvList로 받아온 files를 병합
def concatAllDataframes(resultPath):
    files = getAllCsvList(resultPath)
    dataframes = [pd.read_csv(f) for f in files]
    
    # dataframes 에 데이터가 기록된 querytime column 추가, 이때 값은 파일명에서 추출
    for i in range(len(dataframes)):
        dataframes[i]['querytime'] = files[i].split('/')[-1].split('_')[0]
    
    dataframes = pd.concat(dataframes, ignore_index=True)

    if len(dataframes) == 0 :
        print("There is no file to merge")
        return
    
    return dataframes

# 병합할 시각 지정
def judgeConcatOrNot(timeToConcat):
    if datetime.datetime.now().hour == timeToConcat and datetime.datetime.now().minute == 0 : 
        return True
    else :
        return False

# 하루에 한번 병합 시각을 지정하여, 폴더 내의 병합 시행
def concatAllDataframesPerDay(account_Id, routeNum, timeToConcat):
    basePath = getBasePath(account_Id)
    dirName=getNewDirName()
    resultPath = getResultPath(basePath, routeNum, dirName)
    concatCsvFileName = resultPath + "/" + datetime.datetime.now().strftime("%Y%m%d") + "_merged_" + routeNum + "_rTimeBusPos.csv"
    if not os.path.exists(concatCsvFileName):
        # if judgeConcatOrNot(timeToConcat) :    # < 만약 하루에 한번 실행할 경우, 주석처리 된 조건문을 추가.
        df = concatAllDataframes(resultPath)
        df.to_csv(concatCsvFileName, encoding = 'utf-8-sig', index=False)
        print(df.head())
        print(f"All files for {routeNum} are merged at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")


# ========== main

def main():
    account_Id = "hoonyong" # changing the account ID when you use it on the other PC
    busIdDict = {"1002":"233000140", "1008":"233000125"} #추후 matchingTable로 변환 예정
    timeToConcat = 3  # 몇시에 파일을 병합?

    for key,value in busIdDict.items() :
        reqTime = time.time()
        getapiCall(account_Id, 0, key, value, reqTime)

        # 병합
        concatAllDataframesPerDay(account_Id, key, timeToConcat)

if __name__ == '__main__':
    main()