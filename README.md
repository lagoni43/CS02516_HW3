# CS02516_HW3 - Earthquake Data Processing and Analysis

Earthquake Data Loader and Analyzer

Earthquake Data Loader class is a class used for retrieving earthquake data from
the USGS site and loading it into Redis.  Once data is loaded into Redis it
is indexed so to can be searchable for data analysis.

Earthquake Data Analyzer class is a class used for performing various
analysis functions on earthquake data from an existing redis database.

main.py demonstrates the various loading and analysis functions.