from EarthquakeDataLoader import EarthquakeLoader



def main():
    print("Running CS02516 HW 3 - Earthquake Data Process")

    earthquakeDataLoader = EarthquakeDataLoader("1970-01-01","2023-12-31",6)

    targetEarthquakeJSONFile = "earthquakedata.json"

    earthquakeDataLoader.downloadEarthquakeData(targetEarthquakeJSONFile)

    earthquakeDataLoader.loadEarthquakeDataIntoRedis(targetEarthquakeJSONFile)



if __name__ == "__main__":
    main()

