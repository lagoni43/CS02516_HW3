from EarthquakeDataLoader import EarthquakeDataLoader
from EarthquakeDataAnalyzer import EarthquakeDataAnalyzer



def main():
    print("Running CS02516 HW 3 - Earthquake Data Processor")

    earthquakeDataLoader = EarthquakeDataLoader("1970-01-01","2023-12-31",6) # change back to 1970

    targetEarthquakeJSONFile = "earthquakedata.json"

    earthquakeDataLoader.downloadEarthquakeData(targetEarthquakeJSONFile)

    earthquakeDataLoader.loadEarthquakeDataIntoRedis(targetEarthquakeJSONFile)

    earthquakeDataAnalyzer = EarthquakeDataAnalyzer()

    print("Running Number of Earthquakes per year search")
    earthquakeDataAnalyzer.getNumberEarthquakesPerYear(1980,2022,6)

    print("\n\n\nRunning Geolocation Earthquakes search")
    earthquakeDataAnalyzer.getNumberEarthquakesNearLocation(1971, 2023, 
        6, 35.5066, 27.2124, 200, "Karpathos, Greece")
    
    earthquakeDataAnalyzer.getNumberEarthquakesNearLocation(1971, 2023, 
        6, 39.9526, -75.1652, 200, "Philadelphia, PA, USA")
    
    earthquakeDataAnalyzer.getNumberEarthquakesNearLocation(1971, 2023, 
        6, 35.6764, 139.65, 200, "Tokyo, Japan")

    print("\n\n\nRunning Top 10 Earthquakes Search")
    earthquakeDataAnalyzer.getTop10Earthquakes(1975,2022)


if __name__ == "__main__":
    main()

