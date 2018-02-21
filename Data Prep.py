import pandas as pd
import datetime
import json

combined = pd.DataFrame()

#The column names sometimes randomly change in the different files, so save the names form the first file so they can be
#applied to the other files that are read in
base = pd.read_csv(r'Data\2013-07 - Citi Bike trip data.csv')
header = base.columns

#Used to combine data that is stored in numerous files, separated by month. Since the file nameing format changes year
#to year, accepts the base file format and year as arguments, then reads in data from the desired months in that year
#Read in a data file, keeping only every nth row, and add it to the combined dataframe, using the headers
#from the first dataset
def everyNth (file, year, startMonth = 1, endMonth = 12, n = 10):
    global combined
    for i in range(endMonth-startMonth+1):
        num = i+startMonth
        if num < 10:
            month = "0" + str(num)
        else:
            month = str(num)

        fName = file % (str(year), month)
        data = pd.read_csv(fName, parse_dates=[1,2], infer_datetime_format=True, na_filter=False)
        data.columns = header
        combined = combined.append(data[data.index.values % n == 0])

#Read in all of the available data
everyNth("Data\%s-%s - Citi Bike trip data.csv", 2013, startMonth=7, endMonth=12)
everyNth("Data\%s-%s - Citi Bike trip data.csv", 2014, startMonth=1, endMonth=12)
everyNth("Data\%s%s-citibike-tripdata.csv", 2015, startMonth=1, endMonth=12)
everyNth("Data\%s%s-citibike-tripdata.csv", 2016, startMonth=1, endMonth=12)
everyNth("Data\%s%s-citibike-tripdata.csv", 2017, startMonth=1, endMonth=9)

#A function to round datetime objects to the nearest hour, to be applied to the combined data
def nearestHour (date):
    if date.minute >= 30:
        date = date + datetime.timedelta(hours=1)
    date = date.replace(second=0, minute=0)
    return date

combined['hourly'] = pd.to_datetime(combined['starttime']).map(lambda x: nearestHour(x))


#Arrange the data in another format so it can be used for maps in Tableau
#Separate each trip into two parts, one for departures and one for arrivals
#Assign rows that refer to departures a direction of -1, and rows that refer to arrivals a direction a +1
bikesOut = combined[['start station id', 'start station name', 'start station longitude', 'start station latitude', 'starttime']]
bikesOut.columns = ['station id', 'station name','station longitude', 'station latitude', 'date']
bikesOut['direction'] = -1
bikesIn = combined[['end station id', 'end station name', 'end station longitude', 'end station latitude', 'stoptime']]
bikesIn.columns = ['station id', 'station name','station longitude', 'station latitude', 'date']
bikesIn['direction'] = 1
flow = bikesOut.append(bikesIn)


#Read in a snapshot of the station status data, and extract the station id and total number of docks for each station
json_data = json.loads(open("Data\stations.json").read())
stations = pd.DataFrame(json_data["stationBeanList"])
docks = stations[["id", "totalDocks"]]

#Join the docks information to the flow dataframe. Since there are some stations present in the flow dataframe that
#aren't in the docks dataframe, fill in the missing values with the mean of the available data
docks.columns = ['station id', 'totalDocks']
averageDock = int(docks['totalDocks'].mean())
flow = flow.merge(docks, on='station id', how='left')
flow['totalDocks'].fillna(averageDock, inplace = True)


#Save the two dataframes
flow.to_csv('Data\Flow.csv', index=False)
combined.to_csv('Data\Citi Bike Data Aggregated Reduced.csv', index=False)
