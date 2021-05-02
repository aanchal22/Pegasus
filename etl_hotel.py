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

    def getBookingID(self, size):
        return [str(uuid.uuid4()) for _ in range(size)]


    # Generate complete dataset with properties containing array of described values
    def genDataset(self):
        size = self.datasetSize
        data = {
            'BookingAmount' : self.getBookingAmount(size),
            'BookingID' : self.getBookingID(size)
        }
        return (pd.DataFrame(data))


BouncedDataDF = pd.read_csv("bounced_dataset.csv", usecols = ["UserName", "BouncedAt", "TimeStamp", "CheckIn", "CheckOut", "CityName"])

BouncedDataDF_filtered = BouncedDataDF[BouncedDataDF['BouncedAt'] == 0]
BouncedDataDF_filtered = BouncedDataDF_filtered.reset_index()
DFsize = len(BouncedDataDF_filtered)

myDataGen = DataGenerator(DFsize)
myDataFrame = myDataGen.genDataset()

BouncedDataDF_filtered.loc[:, 'RoomNights'] = (pd.to_datetime(BouncedDataDF_filtered['CheckOut'], format="%Y-%m-%d") - pd.to_datetime(BouncedDataDF_filtered['CheckIn'], format="%Y-%m-%d")).dt.days
BouncedDataDF_filtered.loc[:, 'BookingAmount'] = myDataFrame['BookingAmount']
BouncedDataDF_filtered.loc[:, 'BookingID'] = myDataFrame['BookingID']
df = BouncedDataDF_filtered.drop(columns=['index', 'BouncedAt'])

#add hotelname to the file
hotelname = pd.read_csv('city_hotel.csv')['HotelName'].to_numpy()
df.insert(loc = 4, column = 'HotelName', value = np.random.choice(hotelname, size = DFsize, replace=True))

df.to_csv('etl_hotel.csv', index = False)