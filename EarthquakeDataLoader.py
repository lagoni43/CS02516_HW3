import json
import urllib.request
import os
import datetime
from db_config import get_redis_connection
import redis
from redis.commands.json.path import Path

class EarthquakeDataLoader:
    def __init__(self, startTime, endTime, minMagnitude):
        self.startTime = startTime
        self.endTime = endTime
        self.minMagnitude = minMagnitude

    def downloadEarthquakeData(self, targetEarthquakeJSONFile):
              
        if os.path.exists(targetEarthquakeJSONFile):
            os.remove(targetEarthquakeJSONFile)
        else:
            print(targetEarthquakeJSONFile+" file does not exist")

        requestURLString = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&" + \
            "starttime=" + self.startTime + "&endtime=" + self.endTime + \
            "&minmagnitude=" + str(self.minMagnitude)
        
        print("Requesting earthquake data from USGS: "+requestURLString)
        
        earthquake_data_json = urllib.request.urlretrieve(requestURLString, 
            "earthquakedata.json")
        
    def loadEarthquakeDataIntoRedis(self, targetEarthquakeJSONFile):
        r = get_redis_connection()
        r.flushall()

        with open(targetEarthquakeJSONFile,"rb") as json_file:

            # Reading the data as bytes and encoding due to bug using json.load on the openned file
            data = json_file.read()
            earthquakeDataStr = data.decode(encoding='utf-8', errors='strict') 
            earthquakeData = json.loads(earthquakeDataStr)

            for earthquake in earthquakeData["features"]:
                earthquakeID = earthquake["properties"]["code"]

                earthquakeYear = datetime.datetime.fromtimestamp(earthquake["properties"]["time"]/1000).year

                print("Storing earthquake "+earthquakeID + " from "+ str(earthquakeYear) + " into Redis")

                r.json().set(str(earthquakeYear)+":"+earthquakeID,'$',earthquake["properties"])

