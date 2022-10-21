import glob
import datetime
import shutil
import os,sys
from django.urls import is_valid_path
import pandas as pd
import glob
import time


def getBasePath(account_Id):
    return "/home/" + account_Id + "/gbisOpenApi/realtimePosition" ## changing the account ID when you use it on the other PC

def getResultPath(basePath, routeNum, dirName):
    return basePath+'/results_pos/'+routeNum+'/'+dirName

# 오늘 날짜 6자리 형식으로 받아오기
def getTodayStr():
    return datetime.datetime.today().strftime("%Y-%m-%d")

# 어제 날짜 6자리 형식으로 받아오기
def getYesterdayStr():
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")

# 오늘자 폴더 경로
def getTodayPath(basePath):
    return basePath+getTodayStr()+"/"
# 어제자 폴더 경로
def getYesterdayPath(basePath):
    return basePath+getYesterdayStr()+"/"


# 경로에 파일이 있는지 체크
def isPathVaild(path):
    return os.path.exists(path)

# 새벽 시간대에 수집된 csv파일들을 이전 날짜의 폴더로 옮기기 위한 함수
def fileMove(fileList, yesterdayPath):

    print(yesterdayPath)
    for filename in fileList:
        shutil.move(filename, yesterdayPath + os.path.basename(filename))


# 경로를 받아서, 해당 폴더 내의 오전 03시 이전의 모든 csv 파일의 리스트를 반환하는 함수
def getDawnCsvList(path):
    # 파일명의 10번째,11번째 자리 값이 03보다 작은 csv 파일들을 리스트로 반환
    dawnCsvList = [f for f in glob.glob(path + "/*.csv") if int(f.split('/')[-1].split('_')[0][9:11]) <= 3]
    return dawnCsvList

# 새벽 시간대의 파일들은 하루 전 폴더로 옮기기
def fileMovetoYesterdayBasedTodayDate(todayPath):
    dawnCsvList = getDawnCsvList(todayPath)
    if len(dawnCsvList) == 0 :
        print("There is no file to move")
        return 
    yesterdayDate = getYesterdayStr()
    yesterdayPath = '/'.join(todayPath.split('/')[:-2]) + '/' + yesterdayDate + '/'

    if not isPathVaild(yesterdayPath):
        os.mkdir(yesterdayPath)
    fileMove(dawnCsvList, yesterdayPath)

def getAllCsvList(path):
    return [f for f in glob.glob(path + "/*.csv")]

# dataframe 저장
def saveDataframe(dataframe, destPath):
    dataframe.to_csv(destPath, index=False)

# getAllCsvList로 받아온 files를 병합
def concatAllDataframes(path):
    files = getAllCsvList(path)
    dataframes = [pd.read_csv(f) for f in files]
   
    # dataframes 에 데이터가 기록된 querytime column 추가, 이때 값은 파일명에서 추출
    for i in range(len(dataframes)):
        dataframes[i]['querytime'] = files[i].split('/')[-1].split('_')[0]
    
    dataframes = pd.concat(dataframes, ignore_index=True)

    if len(dataframes) == 0 :
        print("There is no file to merge")
        return



# # 병합할 시각 지정
# def judgeConcatOrNot(timeToConcat):
#     if datetime.datetime.now().hour == timeToConcat and datetime.datetime.now().minute == 0 : 
#         return True
#     else :
#         return False


# def concatAllDataframesPerDay(account_Id, routeNum):
#     basePath = getBasePath(account_Id)
#     dirName = getNewDirName()
#     resultPath = getResultPath(basePath, routeNum, dirName)
#     concatCsvFileName = resultPath + "/" + datetime.datetime.now().strftime("%Y%m%d") + "_merged_" + routeNum + "_rTimeBusPos.csv"
#     if not os.path.exists(concatCsvFileName):
#         df = concatAllDataframes(resultPath)
#         df.to_csv(concatCsvFileName, encoding = 'utf-8-sig', index=False)
#         print(df.head())
#         print(f"All files for {routeNum} are merged at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")



# ========== main
# 코드의 경로 : /home/utlsyslab_10_admin
def main():
    account_Id = "utlsyslab_10_admin" # changing the account ID when you use it on the other PC
    # busIdDict = {"1002":"233000140", "1008":"233000125"} #추후 matchingTable로 변환 예정

    basePath = f"/home/{account_Id}/ggBusJobs/realtimePosition/results_pos/" # 추후 반복문
    resultPath = f"/home/{account_Id}/ggBusJobs/realtimePosition/results_pos/results_merged/"
    basePath = f"/mnt/c/Users/hoonyong/OneDrive - 고려대학교/Coding/hwaseongBusCol/ggBusJobs/realtimePosition/results_pos/"
    resultPath = f"/mnt/c/Users/hoonyong/OneDrive - 고려대학교/Coding/hwaseongBusCol/ggBusJobs/realtimePosition/results_merged/"
    
    # 결과 폴더 없으면 생성
    if not isPathVaild(resultPath):
        os.mkdir(resultPath)

    # basePath의 모든 하위 폴더에 대해
    for routeNum in os.listdir(basePath):
        routeNumPath = basePath + routeNum + "/"

        # 결과 폴더 없으면 생성
        if not isPathVaild(resultPath + routeNum):
            os.mkdir(resultPath + routeNum)

        # routeNumPath 아래 모든 하위 날짜 폴더들에 대해
        for date in os.listdir(routeNumPath):
            datePath = routeNumPath + date + "/"

            # 1. 폴더 내부에 대해 dawn 파일들을 이전 날짜 폴더로 옮기기
            fileMovetoYesterdayBasedTodayDate(datePath)
        
        for date in os.listdir(routeNumPath):
            datePath = routeNumPath + date + "/"
            # 2. 폴더 내부의 모든 csv 파일을 병합
            try : 
                dataframe = concatAllDataframes(datePath)
            except ValueError:
                print(f"No files to concat : {datePath.split('/')[-3:]}")
                continue

            # 3. 병합된 파일을 results_merged 폴더에 저장
            mergeResultPath = resultPath + routeNum + '/' + date + '/'
            if not isPathVaild(mergeResultPath):
                os.mkdir(mergeResultPath)

            try : 
                saveDataframe(dataframe, mergeResultPath + '/' + date + 'merged_' + routeNum + '_rTimeBusPos.csv')
            except AttributeError:
                print(f"No files to save : {datePath.split('/')[-3:]}")
                continue

if __name__ == '__main__':
    main()