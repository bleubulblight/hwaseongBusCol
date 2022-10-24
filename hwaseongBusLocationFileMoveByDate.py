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
    for filename in fileList:
        shutil.move(filename, yesterdayPath + os.path.basename(filename))  # shutil.move(A,B) = A를 B로 옮기기

def getDawnCsvList(path):
    # 오늘 날짜이고, 03시 이전인 파일들만 리스트에 담아서 반환
    # 이때 날짜는 path의 날짜와 동일해야 함
    todayDawnCsvList = [f for f in glob.glob(path + "/*.csv") if int(f.split('/')[-1].split('_')[0][9:11]) <= 3 and f.split('/')[-1].split('_')[0][:8] == path.split('/')[-2].replace('-','')]
    return todayDawnCsvList

# 새벽 시간대의 파일들은 하루 전 폴더로 옮기기
def fileMovetoYesterdayBasedTodayDate(todayPath):
    todayDawnCsvList = getDawnCsvList(todayPath)

    if len(todayDawnCsvList) == 0 :
        print(f"해당 날짜에, 이전 날짜로 옮겨야 할 새벽 시간대 리스트가 존재하지 않음 : {'/'.join(todayPath.split('/')[-3:])}")
        return 
    
    #todayPath에서 하루 빼서 어제의 날짜 추출
    yesterdayDate = (datetime.datetime.strptime(todayPath.split('/')[-2], "%Y-%m-%d") - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    yesterdayPath = '/'.join(todayPath.split('/')[:-2]) + '/' + yesterdayDate + '/'
    if not isPathVaild(yesterdayPath):
        os.mkdir(yesterdayPath)
    fileMove(todayDawnCsvList, yesterdayPath)
    print(f"fileMove from {'/'.join(todayPath.split('/')[-3:])} to {'/'.join(yesterdayPath.split('/')[-3:])} is complete!")

def getAllCsvList(path):
    return [f for f in glob.glob(path + "/*.csv")]

# dataframe 저장
def saveDataframe(dataframe, destPath):
    # save dataframe to csv file, with name
    dataframe.to_csv(destPath, index=False, encoding='utf-8-sig')

# getAllCsvList로 받아온 files를 병합
def concatAllDataframes(path):
    files = getAllCsvList(path)
    dataframes = [pd.read_csv(f) for f in files]
   
    # dataframes 에 데이터가 기록된 querytime column 추가, 이때 값은 파일명에서 추출
    for i in range(len(dataframes)):
        dataframes[i]['querytime'] = files[i].split('/')[-1].split('_')[0]
    
    dataframes = pd.concat(dataframes, ignore_index=True)

    return dataframes


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

    basePath = f"/home/{account_Id}/ggBusJobs/realtimePosition/results_pos/" # 참조 경로
    resultPath = f"/home/{account_Id}/ggBusJobs/realtimePosition/results_pos/results_merged/" # 병합된 파일들이 저장될 경로
    # basePath = f"/mnt/c/Users/hoonyong/OneDrive - 고려대학교/Coding/hwaseongBusCol/ggBusJobs/realtimePosition/results_pos/"
    # resultPath = f"/mnt/c/Users/hoonyong/OneDrive - 고려대학교/Coding/hwaseongBusCol/ggBusJobs/realtimePosition/results_merged/"
    
    # 결과 폴더 없으면 생성
    if not isPathVaild(resultPath):
        os.mkdir(resultPath)

    # basePath의 모든 하위 폴더에 대해
    for routeNum in os.listdir(basePath):
        routeNumPath = basePath + routeNum + "/"

        # 결과 폴더 없으면 생성
        if not isPathVaild(resultPath + routeNum):
            os.mkdir(resultPath + routeNum)


        # 1. 날짜 폴더를 참조하여, 해당 날짜의 새벽 (00시~03시) 파일들을 이전 날짜 폴더로 옮기기
        for date in os.listdir(routeNumPath):
            datePath = routeNumPath + date + "/"

            fileMovetoYesterdayBasedTodayDate(datePath)
        
        # 2. 파일을 옮긴 후 날짜 폴더를 참조하여, 폴더 내부의 모든 csv 파일을 병합
        for date in os.listdir(routeNumPath):

            mergeResultPath = resultPath + routeNum + '/' # 예시 : /realtimePosition/results_merged/1002_20000000/
            mergeResultcsvName = mergeResultPath + date + "_merged_" + routeNum + "_rTimeBusPos.csv"
            
            # 이미 파일이 있으면 시간낭비 방지
            # if isPathVaild(mergeResultcsvName):
            #     print(f"이미 병합된 파일이 있습니다.{'/'.join(mergeResultcsvName.split('/')[-3:])}")
            #     continue

            datePath = routeNumPath + date + "/"
            try : 
                dataframe = concatAllDataframes(datePath)
            except ValueError:
                print(f"No files to concat : {'/'.join(datePath.split('/')[-3:])}")
                continue

            # 3. 병합된 파일을 results_merged 폴더에 저장
   
            if not isPathVaild(mergeResultPath):
                os.mkdir(mergeResultPath)

            try : 
                saveDataframe(dataframe, mergeResultcsvName)
                print(f"File saved : {mergeResultcsvName}")
            except AttributeError:
                print(f"No files to save : {'/'.join(datePath.split('/')[-3:])}")
                continue

if __name__ == '__main__':
    main()