import pandas as pd
import numpy as np
from pydbgen import pydbgen
import matplotlib.pyplot as plt

class DataGenerator:
    def __init__(self, datasetSize = 1):
        self.pyDb = pydbgen.pydb()
        self.datasetSize = datasetSize

    
    def getCityName(self, size):
        return (np.array([self.pyDb.city_real() for i in range(size)]))

    
    def getVendorAmount(self, size):
        return (np.random.randint(10000,10000000, size=size))

    
    def getRoomNights(self, size):
        return (np.random.randint(1,30, size=size))

    
    def getVendorBookingID(self, size):
        return (np.random.randint(10000000,100000000, size=size))

    
    def getTransactionID(self, size):
        return 0

    
    def getBookingDate(self, size, low = 0, high = 365):
        randomInt = np.random.uniform(low, high, size).astype(int)
        checkInDateArr = np.datetime64('today') + randomInt
        return (checkInDateArr)

    
    # Generate complete dataset with properties containing array of described values
    def genDataset(self):
        size = self.datasetSize
        data = {
            'CityName' : self.getCityName(size),
            'VendorAmount' : self.getVendorAmount(size),
            'RoomNights' : self.getRoomNights(size),
            'VendorBookingID' : self.getVendorBookingID(size),
            'TransactionID' : self.getTransactionID(size),
            'BookingDate' : self.getBookingDate(size)
        }
        return (pd.DataFrame(data))

myDataGen = DataGenerator(10000)
myDataFrame = myDataGen.genDataset()
#myDataFrame.insert(loc = 7, column = 'NumofRooms', value = 0)

#pr = [0.63,0.37]

#for i in range(10000):
#	x = myDataFrame.loc[i, 'NumOfPeople']
#	if(x%2==0):
#		y=x/2
#		levels = [y,x]
#	else:
#		y=(x/2)+1
#		levels = [y,x]

#	myDataFrame.loc[i, 'NumofRooms'] = np.random.choice(levels, size = 1, p = pr).astype(int)



myDataFrame.to_csv('etl_hotel.csv', index = False)
myDataFrame.to_excel('etl_hotel.xlsx', index = False)
