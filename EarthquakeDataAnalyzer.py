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


class EarthquakeDataAnalyzer:
    '''
    Earthquake Data Analyzer class is a class used for performing various
    analysis functions on earthquake data from an existing redis database.
    '''


    def getNumberEarthquakesPerYear(self, startYear, endYear, minMagnitude):
        '''
        Analysis function to search and count the earthquakes in each year within the
        range that exceed a min magnitude.  The function prints a table with the results.

        startYear - the year for the earlier earthquake data to analyze
        endYear - the year for the latest data to analyze
        minMagnitude - the minimum magnitude for earthquakes to analyze

        '''


        r = get_redis_connection()

        yearList = []
        earthQuakeCountList = []

        for year in range(startYear,endYear+1,1):
            yearList.append(year)

            startEpochTime = datetime.datetime(year,1,1).timestamp()
            stopEpochTime = datetime.datetime(year,12,31).timestamp()

            queryString = "@time:["+str(startEpochTime*1000)+" "+str(stopEpochTime*1000)+"] @mag:["+str(minMagnitude)+" 100]"

            #print(queryString)

            res = r.ft("earthquakesIdx").search(Query(queryString).paging(0,10000)).docs

            earthQuakeCountList.append(str(len(res)))
        
        print("Counting number of earthquakes per year from "+str(startYear)+" to "+str(endYear)+" with magnitude > "+str(minMagnitude))

        print("Year,Earthquake_Count")

        for count in range(1,len(yearList)):
            print(str(yearList[count])+","+str(earthQuakeCountList[count]))



    

    def getTop10Earthquakes(self, startYear, endYear):
        '''
        Analysis function to search and sort the top 10 earthquakes within
        the spacified range of years.  The function prints the results of the search.

        startYear - the year for the earlier earthquake data to analyze
        endYear - the year for the latest data to analyze
        '''

        r = get_redis_connection()

        startEpochTime = datetime.datetime(startYear,1,1).timestamp()
        stopEpochTime = datetime.datetime(endYear,12,31).timestamp()

        queryString = "@time:["+str(startEpochTime*1000)+" "+str(stopEpochTime*1000)+"] SORTBY mag DESC"

        print(queryString)

        res = r.ft("earthquakesIdx").search(Query(queryString)).docs

        for earthquake in res:
            #datetime.datetime.fromtimestamp(earthquake["properties"]["time"]/1000)
            print(earthquake)
        

    def getNumberEarthquakesNearLocation(self, startYear, endYear, minMagnitude, latitude, longitude, distance, locationName):
        '''
        Analysis function to perform a geographic search to find teh number of earthquakes within
        a time range, exceeding a minimum magnitude and within a specified distance in kilometers from a location.

        startYear - the year for the earlier earthquake data to analyze
        endYear - the year for the latest data to analyze
        minMagnitude - the minimum magnitude for earthquakes to analyze
        latitude - latitude of teh location
        longitude - longitude of teh location
        distance - distance from teh location
        locationName - name of the location

        '''        
        r = get_redis_connection()

        startEpochTime = datetime.datetime(startYear,1,1).timestamp()
        stopEpochTime = datetime.datetime(endYear,12,31).timestamp()

        queryString = "@location:[ "+str(longitude)+" "+str(latitude)+" "+str(distance)+" km] @time:["+str(startEpochTime*1000)+" "+str(stopEpochTime*1000)+"]"

        print(queryString)

        res = r.ft("earthquakesIdx").search(Query(queryString).paging(0,10000)).docs
        
        print(str(len(res))+" earthquakes within "+str(distance)+" km of "+locationName + " from "+str(startYear)+" to "+str(endYear)+" with magnitude > "+str(minMagnitude))

