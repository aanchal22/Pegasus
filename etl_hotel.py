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

    # Generate complete dataset with properties containing array of described values
    def genDataset(self):
        size = self.datasetSize
        data = {
            'BookingAmount' : self.getBookingAmount(size),
            'RoomNights' : self.getRoomNights(size),
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

#add city to the file
cityname = pd.read_csv('city_hotel.csv')['City'].to_list()

myDataFrame.insert(loc = 1, column = 'CityName', value = cityname)
myDataFrame.insert(loc = 2, column = 'HotelName', value = hotelname)

myDataFrame.to_csv('etl_hotel.csv', index = False)