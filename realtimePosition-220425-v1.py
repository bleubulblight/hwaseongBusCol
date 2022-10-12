#-*- coding: utf-8 -*-
#csv format
# realtimePosition { result, doce, developerMessage, link, message, status, total}
# [
#    rowNum, selectedCount, totalCount, subwayId, subwayNm, 
#    statnId, statnNm, trainNo, lastRecptnDt, recptnDt,  
#    updnLine, statnTid, statnTnm, trainSttus, directAt, 
#    lstcarAt
# ]
# api Key : 4943656c6779736f31303174776c5845
# url : http://swopenapi.seoul.go.kr/api/subway/4943656c6779736f31303174776c5845/xml/realtimePosition/0/23/9호선
#http://swopenapi.seoul.go.kr/api/subway/4943656c6779736f31303174776c5845/xml/realtimePosition/0/23/9%ED%98%B8%EC%84%A0
import urllib.request
import urllib.parse
import json
import pandas as pd
import os
import datetime
import logging
import gc
import sys
import time
from sys import argv

script, first, second = argv

def getNewDirName():
    return str(datetime.date.today())
def apiKey():
    return "544270635379736f3630767a784165"
def apiDefaultUri():
    return "http://swopenapi.seoul.go.kr/api/subway/{}/json/realtimePosition/{}/{}/{}"

def getApiUrlByParam(startNum=0,endNum=100,lineName="9호선"):
    return apiDefaultUri().format(apiKey(),startNum,endNum,urllib.parse.quote(lineName))
def resultStrFormatChange(jsonStr):
    return jsonStr.replace("\'","\"").replace("None","\"None\"")
def getBasePath(account_Id):
    return "/home/"+account_Id+"/seoulSubwayJobs/realtimePosition" ## changing the account ID when you use it on the other PC
def getResultPath(basePath, filePrefix, dirName):
    return basePath+'/results_pos/'+filePrefix+'/'+dirName
def apiCall(uri,filePrefix,callCnt,account_Id,reqtime):
    basePath = getBasePath(account_Id)
    account_ID2 = account_Id
    
    
    
    try:
      print(uri)
      print("start" + str(callCnt))
      dirName = getNewDirName()
      if not os.path.exists(getResultPath(basePath, filePrefix, dirName)):
         os.makedirs(getResultPath(basePath, filePrefix, dirName))

      callCnt += 1
     
      req = urllib.request.Request(uri)
      res = urllib.request.urlopen(req, timeout=3)
      data = res.read()
      compTime = round(time.time()-reqtime,0)
      print("\n\ncomptime: ",compTime)
      compTimeStr = "compTime-"+str(compTime)+"sec"
      print("\n\nresttimestr: ",compTimeStr)


      jsonObj = json.loads(data.decode('utf-8'))
      pdataframe = pd.read_json(resultStrFormatChange(str(jsonObj["realtimePositionList"])))

      #print(pdataframe)
      pdataframe.to_csv(getResultPath(basePath, filePrefix, dirName)+"/"+str(datetime.datetime.now())+"_"+filePrefix+"_rTimeTrainPos_"+compTimeStr+'.csv')
      del pdataframe
    
    except Exception as es:
      if callCnt < 5:
        print("\n\nRetry- callCnt: ", callCnt)
        apiCall(uri,filePrefix,callCnt,account_ID2, reqtime)
      else:
         setLoggerInfo(getResultPath(basePath, filePrefix, dirName), filePrefix)
         logger = logging.getLogger("ErrLog")
         logger.info(es)
         sys.exit()

    #f = open("myjson.json", 'w')
    #f.write(str(jsonObj["realtimePositionList"]))
    #f.close
    #uploadToHDFS(open("myjson.json","rb"))
    #for row in jsonObj["realtimePositionList"]:
        #print(row["statnNm"])
def uploadToHDFS(file):
    headers = {
      'Content-Type': 'application/octet-stream'
    }
    req  = urllib.request.Request("http://172.20.10.5:50075/webhdfs/v1/user/myjson.json?op=CREATE&&user.name=jinhanchoi&namenoderpcaddress=localhost:9000&overwrite=false",headers=headers,data=file,method="PUT")

    urllib.request.urlopen(req).read()
def setLoggerInfo(resultPath, filePrefix):
    if not os.path.exists(resultPath+"/errLog"):
       os.makedirs(resultPath+"/errLog")

    mylogger = logging.getLogger("ErrLog")
    mylogger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    stream_hander = logging.StreamHandler()
    stream_hander.setFormatter(formatter)
    mylogger.addHandler(stream_hander)
    file_handler = logging.FileHandler(resultPath+'/errLog/'+"rTimeTrainPos_"+filePrefix+"_"+str(datetime.datetime.now())+'.log')
    mylogger.addHandler(file_handler)

def main():
    accountId = first #"tlsyslab_9_admin" # changing the account ID when you use it on the other PC
    LineNm = second #"Line1" #Chaning the Line number
    LineList = {"Line1":"1호선","Line2":"2호선","Line3":"3호선","Line4":"4호선","Line5":"5호선","Line6":"6호선","Line7":"7호선","Line8":"8호선","Line9":"9호선","LineA":"공항철도","LineBD":"수인분당선","LineKC":"경춘선","LineKJ":"경의중앙선","LineSI":"수인선","LineS":"신분당선","LineWS":"우이신설선"}
    LineNmReq = LineList[LineNm]
    reqtime = time.time()
    print("\n\nreqttime: ", reqtime) #pd.to_datetime(reqtime, format="%Y-%m-%d %H:%M:%S"))
    apiCall(getApiUrlByParam(lineName=LineNmReq),LineNm,0,accountId,reqtime) 

    sys.exit()


# python filename.py
if __name__ == "__main__":
    main()
    #uploadToHDFS()
