import json
import urllib.request
import os
import datetime
from db_config import get_redis_connection
import redis
from redis.commands.json.path import Path
import redis.commands.search.aggregation as aggregations
import redis.commands.search.reducers as reducers
from redis.commands.search.field import TextField, NumericField, GeoField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import NumericFilter, Query

class EarthquakeDataLoader:
    '''
    Earthquake Data Loader class is a class used for retrieving earthquake data from
    the USGS site and loading it into Redis.  Once data is loaded into Redis it
    is indexed so to can be searchable for data analysis.
    '''

    def __init__(self, startTime, endTime, minMagnitude):
        '''
        Constructor that initializes the data loader for parameters such as the
        start time and end time of earthquake data and minimum magnitude to load. 

        startTime - the year for the earlier earthquake data to retrieve and store
        endTime - the year for the latest data to retrieve and store
        minMagnitude - the minimum magnitude for earthquakes to retrieve and sitre

        '''
        self.startTime = startTime
        self.endTime = endTime
        self.minMagnitude = minMagnitude

    def downloadEarthquakeData(self, targetEarthquakeJSONFile):
        '''
        Function for retrieving the earthquake data from USGS and writing it to a
        local file for future loading into Redis.

        targetEarthquakeJSONFile - The json file to save the retrieved data
        '''
              
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
        '''
        Function for reading the earthquake data from json file.  A config.yaml
        is used to connect to the redis database.

        targetEarthquakeJSONFile - The json file to read the retrieved data from
        '''

        r = get_redis_connection()
        r.flushall()

        with open(targetEarthquakeJSONFile,"rb") as json_file:

            # Reading the data as bytes and encoding due to bug using json.load on the openned file
            data = json_file.read()
            earthquakeDataStr = data.decode(encoding='utf-8', errors='strict') 
            earthquakeData = json.loads(earthquakeDataStr)

            for earthquake in earthquakeData["features"]:
                earthquakeID = earthquake["properties"]["code"]

                print("Storing earthquake "+earthquakeID + " into Redis")
                r.json().set("earthquakes:"+earthquakeID,'$',earthquake)

                # Build and set location array JSON to support GEO indexing
                location = str(earthquake["geometry"]["coordinates"][0])+","+str(earthquake["geometry"]["coordinates"][1])
                r.json().set("earthquakes:"+earthquakeID,'$.location',location)


                # Alternate method for storing data, pre-organized by year
                #earthquakeYear = datetime.datetime.fromtimestamp(earthquake["properties"]["time"]/1000).year
                #print("Storing earthquake "+earthquakeID + " from "+ str(earthquakeYear) + " into Redis")
                #r.json().set(str(earthquakeYear)+":"+earthquakeID,'$',earthquake["properties"])

            # Creating search index
            schema = (
                TextField("$properties.status", as_name="status"), 
                TextField("$properties.alert", as_name="alert"), 
                NumericField("$properties.mag", as_name="mag", sortable=True),                
                NumericField("$properties.time", as_name="time"),
                NumericField("$properties.tsunami", as_name="tsunami"),
                GeoField("$location", as_name="location")
            )

            rs = r.ft("earthquakesIdx")
            rs.create_index(
                schema,
                definition=IndexDefinition(
                    prefix=["earthquakes:"], index_type=IndexType.JSON
                )
            )



