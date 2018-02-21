import pandas as pd
import requests
import datetime

#Put in your DarkSky API key here
dsKey = "###############"

#Refers to 7/1/2013 12 a.m., which is when the data starts
time = 1372696845

#Refers to 10/1/2017 12 a.m., which is the cutoff time for the data
endDate = 1506789645
dsData = pd.DataFrame()

#Make api requests for the weather in New York for every hour from the start of when ride data was collected until the
#when the last trip data is available
while time < endDate:

    url = 'https://api.darksky.net/forecast/' + dsKey + '/40.729311,-73.990620,' + str(time)

    response = requests.get(url)
    json_Data = response.json()
    temp = pd.DataFrame(json_Data['hourly']['data'])
    dsData = dsData.append(temp)
    time += 86400



#Save the raw data that was collected from the api
dsData.to_csv('Data\Dark Sky Data.csv', index=False)

#Columns of interest to be used in analysis
columns = ['apparentTemperature','humidity','precipIntensity','precipProbability','precipType','summary','temperature','time','windSpeed']
dsData = dsData[columns]

#Convert the UNIX times to datetime objects, and save a cleaned version of the DarkSky data
toDate = lambda x: datetime.datetime.fromtimestamp(x)
dsData['hourly'] = dsData['time'].map(toDate)
dsData.to_csv('Data\Dark Sky Data Cleaned.csv', index=False)

