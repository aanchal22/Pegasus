import pandas as pd
import numpy as np
from pydbgen import pydbgen
import matplotlib.pyplot as plt
import uuid
import csv

class DataGenerator:
    def __init__(self, datasetSize = 1):
        self.pyDb = pydbgen.pydb()
        self.datasetSize = datasetSize

    def getBookingAmount(self, size):
        return (np.random.randint(100,4000, size=size))

    def getRoomNights(self, size):
        return (np.random.randint(1,30, size=size))

    def getBookingID(self, size):
        return [str(uuid.uuid4()) for _ in range(size)]

    def getBookingDate(self, size, low = 0, high = 365):
        randomInt = np.random.uniform(low, high, size).astype(int)
        checkInDateArr = np.datetime64('today') + randomInt
        return (checkInDateArr)

    def getTimeStamp(self, size, low = 0, high = 365):
        randomInt = np.random.uniform(low, high, size).astype(int)
        randomHr = np.random.randint(0,24,size)
        randomMin = np.random.randint(0,60,size)
        randomSec = np.random.randint(0,60,size)

        randomHr = pd.DataFrame(randomHr)
        randomHr.rename( columns={0 :'I'}, inplace=True )
        randomHr['I'] = randomHr['I'].astype(str)
        randomHr.loc[(randomHr['I'].str.len() == 1), 'I'] = '0' + randomHr['I'].astype(str)

        randomMin = pd.DataFrame(randomMin)
        randomMin.rename( columns={0 :'I'}, inplace=True )
        randomMin['I'] = randomMin['I'].astype(str)
        randomMin.loc[(randomMin['I'].str.len() == 1), 'I'] = '0' + randomMin['I'].astype(str)

        randomSec = pd.DataFrame(randomSec)
        randomSec.rename( columns={0 :'I'}, inplace=True )
        randomSec['I'] = randomSec['I'].astype(str)
        randomSec.loc[(randomSec['I'].str.len() == 1), 'I'] = '0' + randomSec['I'].astype(str)
        randomTime = randomHr['I'] + ':' + randomMin['I'] + ':' + randomSec['I']
        randomTime = randomTime.to_numpy()

        timeArr = np.datetime64('today') - randomInt
        timeArr = np.datetime_as_string(timeArr)

        arr_list = [timeArr, randomTime]
        final_time_arr = np.apply_along_axis(' '.join, 0, arr_list)
        return (final_time_arr)

    # Generate complete dataset with properties containing array of described values
    def genDataset(self):
        size = self.datasetSize
        data = {
            'BookingAmount' : self.getBookingAmount(size),
            'RoomNights' : self.getRoomNights(size),
            'TimeStamp' : self.getTimeStamp(size),
            'BookingID' : self.getBookingID(size),
            'BookingDate' : self.getBookingDate(size)
        }
        return (pd.DataFrame(data))

myDataGen = DataGenerator(10000)
myDataFrame = myDataGen.genDataset()

#add Username column to the file
username = pd.read_csv('username.csv')['UserName'].to_list()

myDataFrame.insert(loc = 0, column = 'UserName', value = 0)

for i in range(100):
    myDataFrame.loc[i, 'UserName'] = np.random.choice(username)

#add hotelname to the file
hotelname = pd.read_csv('city_hotel.csv')['HotelName'].to_list()
hotelname = np.unique(hotelname)

#add city to the file
cityname = pd.read_csv('city_hotel.csv')['City'].to_list()
cityname = np.unique(cityname)

myDataFrame.insert(loc = 1, column = 'CityName', value = 0)
myDataFrame.insert(loc = 2, column = 'HotelName', value = 0)

for i in range(100):
    myDataFrame.loc[i, 'CityName'] = np.random.choice(cityname)
    myDataFrame.loc[i, 'HotelName'] = np.random.choice(hotelname)

myDataFrame.to_csv('etl_hotel.csv', index = False)