import pandas as pd
import numpy as np
from pydbgen import pydbgen
import matplotlib.pyplot as plt
import uuid

class DataGenerator:
    def __init__(self, datasetSize = 1):
        self.pyDb = pydbgen.pydb()
        self.datasetSize = datasetSize

    
    def getVendorAmount(self, size):
        return (np.random.randint(10000,10000000, size=size))

    
    def getRoomNights(self, size):
        return (np.random.randint(1,30, size=size))

    
    def getVendorBookingID(self, size):
        return (np.random.randint(10000000,100000000, size=size))

    
    def getTransactionID(self, size):
        return [str(uuid.uuid4()) for _ in range(size)]

    
    def getBookingDate(self, size, low = 0, high = 365):
        randomInt = np.random.uniform(low, high, size).astype(int)
        checkInDateArr = np.datetime64('today') + randomInt
        return (checkInDateArr)

    
    # Generate complete dataset with properties containing array of described values
    def genDataset(self):
        size = self.datasetSize
        data = {
            'VendorAmount' : self.getVendorAmount(size),
            'RoomNights' : self.getRoomNights(size),
            'VendorBookingID' : self.getVendorBookingID(size),
            'TransactionID' : self.getTransactionID(size),
            'BookingDate' : self.getBookingDate(size)
        }
        return (pd.DataFrame(data))

myDataGen = DataGenerator(10000)
myDataFrame = myDataGen.genDataset()
CityDF = pd.read_csv("city_dataset.csv", usecols = ["CityName"])
myDataFrame.insert(loc = 0, column = 'CityName', value = CityDF)

myDataFrame.to_csv('etl_hotel.csv', index = False)
