import os
import sys
import xmltodict
import json
from urllib import request
import requests
import time
import datetime
import traceback
import folium
from folium.features import DivIcon
import wget
import pdb

# script, areaId = sys.argv
areaId = 31

def getRouteGeoData(routeId, serviceKey) :
    response = requests.get("http://apis.data.go.kr/6410000/busrouteservice/getBusRouteLineList", params={'serviceKey': serviceKey, 'routeId': routeId})
    StoppingGeoDataDict = convertXmltoJson(response)['response']['msgBody']['busRouteLineList']
    return StoppingGeoDataDict

def convertXmltoJson(response) :
    return json.loads(json.dumps(xmltodict.parse(response.content)))

def addMarker(myMap, y, x, routeName) :
    folium.Marker([float(y),float(x)], 
                  icon=DivIcon(icon_size=(250,36), icon_anchor=(0,0), html='<div style="font-size:10pt">'+routeName+'</div>')).add_to(myMap)

def drawRoutebyPolyLine(myMap, routeLocationDataList, busTypeColor, busTypeWeight) :
    folium.PolyLine(routeLocationDataList, color=busTypeColor, weight=busTypeWeight, opacity=1,tooltip='Polyline').add_to(myMap)
    
def extractJson(jsonFile, index, param, serviceKey) :
    routeId = jsonFile[index]['routeId']
    return getRouteGeoData(routeId, serviceKey)[0][param]

def main() :
    myMap_center = [37.541, 126.986]
    myMap = folium.Map(location=myMap_center, zoom_start=10)
    
    serviceKey_Decoded = 'toModCFuZN4ZaggiFbeVftpKdNQl8KYV5i4aP6BOq3hJznX/kSeXZaqyPmUDXV3ZkuYHIM6jtv70quzocdxRGg=='
    response = requests.get("http://apis.data.go.kr/6410000/busrouteservice/getAreaBusRouteList", params={'serviceKey': serviceKey_Decoded, 'areaId': areaId, 'keyword': ''})
    
    routeListinRegion_json = convertXmltoJson(response)['response']['msgBody']['busRouteList']
    routeTypeColorMatcingTable = {'11':'red','12':'blue','13':'green','14':'cyan'}
    routeTypeWeightMatcingTable = {'11':3,'12':2.5,'13':1,'14':3.5}
    
    addMarker(myMap, 
              extractJson(routeListinRegion_json, 0, 'y', serviceKey_Decoded), # json, index, param, serviceKey를 받아 Json으로부터 해당 param 값을 추출
              extractJson(routeListinRegion_json, 0, 'x', serviceKey_Decoded), 
              extractJson(routeListinRegion_json, 0, 'routeName', serviceKey_Decoded))
    addMarker(myMap, 
              extractJson(routeListinRegion_json, -1, 'y', serviceKey_Decoded), 
              extractJson(routeListinRegion_json, -1, 'x', serviceKey_Decoded), 
              extractJson(routeListinRegion_json, -1, 'routeName', serviceKey_Decoded))
    
    for route in routeListinRegion_json :
        print(route)
        StoppingGeoDataDict = getRouteGeoData(route['routeId'], serviceKey_Decoded)
        
        routeLocationDataList = list()
        
        for GeoPoint in StoppingGeoDataDict :
            routeLocationDataList.append([float(GeoPoint['y']), float(GeoPoint['x'])])
            
        drawRoutebyPolyLine(myMap, routeLocationDataList, routeTypeColorMatcingTable[route['routeTypeCd']], routeTypeWeightMatcingTable[route['routeTypeCd']])
        #직행좌석 빨강 / 좌석버스 파랑 / 일반버스 초록 / 마을버스 노랑 
    

    myMap.save('index2.html')
if __name__ == '__main__' :
    main()
    
