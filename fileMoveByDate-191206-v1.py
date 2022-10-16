import glob
import datetime
import shutil
import os,sys
import pandas

def getDirFileList(basePath, targetStr):
    return glob.glob(getFilePattern(basePath, targetStr))
def getFilePattern(basePath,targetStr):
    return basePath+targetStr+' 0[0-2]*'

def getResultPath(basePath, filePrefix, dirName):
    return basePath+'/results_arrival/'+filePrefix+'/'+dirName

def getTodayStr():
    return datetime.datetime.today().strftime("%Y-%m-%d")
def getYesterdayStr():
    yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")

def isPathValid(path):
    return os.path.exists(path)

def getTodayPath(basePath):
    return basePath+getTodayStr()+"/"
def getYesterdayPath(basePath):
    return basePath+getYesterdayStr()+"/"
def getParamPath(basePath, paramPath):
    return basePath+paramPath+"/"

def fileMove(fileList, destPath):
    print("\n\n")
    print("###### Target File Path ######")
    print(destPath)
    for filename in fileList:
        shutil.move(filename,destPath+os.path.basename(filename))
    

def fileMoveBasedTodayDate(basePath):
    fileList = getDirFileList(getTodayPath(basePath), getTodayStr())
    print("\n\n")
    print("###### Target File List #######", getParamPath(basePath, getTodayStr()))
    print("fileList Length: ",len(fileList))
    
    if isPathValid(getYesterdayPath(basePath)):
        fileMove(fileList,getYesterdayPath(basePath))
    else:
        print("\n\n")
        print("dest Path doesn't Exists")
        print("  ***  A: dest Path: ", getYesterdayPath(basePath))
        os.makedirs(getYesterdayPath(basePath))
        fileMove(fileList,getYesterdayPath(basePath))
        print("\n\nfileMove had been completed. destDateStr="+getYesterdayStr())
        
    

def fileMoveBasedParams(basePath, targetDateStr, destDateStr):
    fileList = getDirFileList(getParamPath(basePath, targetDateStr),targetDateStr)
    print("\n\n")
    print("###### Target File List #######", getParamPath(basePath, targetDateStr))
    print("fileList Length: ",len(fileList))
    if isPathValid(getParamPath(basePath, destDateStr)):
        fileMove(fileList,getParamPath(basePath, destDateStr))
        print("\n\nfileMove had been completed. destDateStr="+destDateStr)
    else:
        print("\n\n")
        print("dest Path doesn't Exists.")
        print("  ***  B: dest Path: ", getParamPath(basePath, destDateStr))
        os.makedirs(getParamPath(basePath, destDateStr))
        print("\n\n Folder "+destDateStr+" had been created.")
        fileMove(fileList,getParamPath(basePath, destDateStr))
        print("\n\nfileMove had been completed. destDateStr="+destDateStr)
    

from os import listdir
from os.path import isfile, join

def getAllFiles(basePath):
    onlyfiles = [f for f in listdir(basePath) if isfile(join(basePath, f)) and not f.startswith(".")]
    return onlyfiles
def readAllCsv(basePath,files):
    dataframes = [pandas.read_csv(join(basePath,f)) for f in files]
    return dataframes
def concatAllDataframes(dataframes):
    return pandas.concat(dataframes, axis=0, ignore_index=True)

if __name__ == "__main__":
    
    ##path base logic
    if len(sys.argv) == 4:
        print("\nlen(sys.argv) == 4:\n")
        
        if sys.argv[1] == "rtArrival":
            dtype = "results_arrival"
            fntype = "rtStaArrival"
        elif sys.argv[1] == "rtPosition":
            dtype = "results_pos"
            fntype = "rtTrainPos"
        else:
            dtype = -1


        if dtype != -1:
            destDateStr = getYesterdayStr()
            basePath = "/home/"+sys.argv[2]+"/seoulSubwayJobs/realtimePosition/"+dtype+"/"+sys.argv[3]+"/"
            resultPath = "/home/"+sys.argv[2]+"/seoulSubwayJobs/realtimePosition/results_merged/"+sys.argv[1]+"/"+sys.argv[3]+"/"

            print(basePath)
            fileMoveBasedTodayDate(basePath)

            print("\n\n ### FileMove had been Completed: based on Today \n\n")

            myfiles = getAllFiles(getParamPath(basePath, destDateStr))
            nfiles = str(len(myfiles))
            mergedFrame = concatAllDataframes(readAllCsv(getParamPath(basePath, destDateStr),myfiles))
        
            print("\n\n ### FileMerge had been Completed: based on Today \n\n")
            if not os.path.exists(resultPath): 
                os.makedirs(resultPath) 

            mergedFrame.to_csv(join(resultPath,destDateStr+"_"+sys.argv[3]+"_"+fntype+"_merged_"+nfiles+"-files_"+sys.argv[2]+".csv"),mode="w")

            #remove every file

        else:
            print("\n\n ### dtype Err: please insert rtArrival or rtPosition - current input is "+sys.argv[1])
    
    ##param base logic
    elif len(sys.argv) > 4:
        print("\nlen(sys.argv) > 4:\n")
        print(sys.argv)

        if sys.argv[1] == "rtArrival":
            dtype = "results_arrival"
            fntype = "rtStaArrival"
        elif sys.argv[1] == "rtPosition":
            dtype = "results_pos"
            fntype = "rtTrainPos"
        else:
            dtype = -1

        if dtype != -1:
            basePath = "/home/"+sys.argv[2]+"/seoulSubwayJobs/realtimePosition/"+dtype+"/"+sys.argv[3]+"/"
            resultPath = "/home/"+sys.argv[2]+"/seoulSubwayJobs/realtimePosition/results_merged/"+sys.argv[1]+"/"+sys.argv[3]+"/"
            targetDateStr = sys.argv[4]
            destDateStr = sys.argv[5]
            fileMoveBasedParams(basePath, targetDateStr, destDateStr)

            print("\n\n ### FileMove had been Completed: based on User-dates \n\n")
            if not os.path.exists(resultPath): 
                os.makedirs(resultPath)
            
            myfiles = getAllFiles(getParamPath(basePath, destDateStr))
            nfiles = str(len(myfiles))
            mergedFrame = concatAllDataframes(readAllCsv(getParamPath(basePath, destDateStr),myfiles))
            mergedFrame.to_csv(join(resultPath,destDateStr+"_"+sys.argv[3]+"_"+fntype+"_merged_"+nfiles+"-files_"+sys.argv[2]+".csv"),mode="w")

        else:
            print("\n\n ### dtype Err: please insert rtArrival or rtPosition - current input is "+sys.argv[1])

    # else:
    #     print("Use One or Three Parameters : One -> Based TodayDate, Three -> Based Params Date")
    #     print("One Param Useage : python3 fileMoveByDate.py rtArrival_or_rtPosition userName LineNm(No / at the end)")
    #     print("One Param Useage : python3 fileMoveByDate.py rtArrival utlsyslab_10_admin Line9")
    #     print("Three Param Useage : python3 fileMoveByDate.py rtArrival_or_rtPosition userName LineNm(No / at the end) 2019-08-10 2019-08-09")
    #     print("Three Param Useage : python3 fileMoveByDate.py rtPosition utlsyslab_10_admin LineS 2019-08-10 2019-08-09")
    