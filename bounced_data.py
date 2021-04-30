import pandas as pd
import numpy as np
from pydbgen import pydbgen
import matplotlib.pyplot as plt

class DataGenerator:
    def __init__(self, datasetSize = 1):
        self.pyDb = pydbgen.pydb()
        self.datasetSize = datasetSize

    # Generate array of fake user email addresses
    def getUserName(self, size):
        return (np.array([self.pyDb.fake.email() for i in range(size)]))

    # Generate array of bounced-at levels, select b/w 0 & 1
    def getBouncedAt(self, size, levels = [0, 1], prob = [0.5,0.5]):
        return (np.random.choice(levels, size = size, p = prob))

    # Generate array of random timestamps
    def getTimeStamp(self, size, low = 0, high = 365):
        randomInt = np.random.uniform(low, high, size).astype(int)
        timeArr = np.datetime64('today') - randomInt
        return (timeArr)

    # Generate array of random device ID's
    def getDeviceID(self, size):
        return (np.random.randint(1000000,10000000, size=size))

    # Generate array of device OS
    def getFlavor(self, size, levels = ['Android','IOS'], prob = [0.6,0.4]):
        return (np.random.choice(levels, size = size, p = prob))

    # Generate array with int values specifying number of people
    def getNumOfPeople(self, size):
        return (np.random.randint(1,10,size=size))

    # Generate array with int values specifying number of rooms
    def getNumOfRooms(self, size):
        return (np.random.randint(1,10,size = size))

    def getCheckIn(self, size, low = 0, high = 120):
    	randomInt = np.random.uniform(low, high, size).astype(int)
    	checkInDateArr = np.datetime64('today') + randomInt
    	return (checkInDateArr)

    def getCheckOut(self, size, low = 0, high = 30):
    	randomInt = np.random.uniform(low, high, size).astype(int)
    	checkOutDateArr = np.datetime64('today') + randomInt
    	return (checkOutDateArr)

    # Generate complete dataset with properties containing array of described values
    def genDataset(self):
        size = self.datasetSize
        data = {
            'UserName' : self.getUserName(size),
            'BouncedAt' : self.getBouncedAt(size),
            'TimeStamp' : self.getTimeStamp(size),
            'DeviceID' : self.getDeviceID(size),
            'Flavor' : self.getFlavor(size),
            'CheckIn' : self.getCheckIn(size),
            'CheckOut' : self.getCheckOut(size),
            'NumOfPeople' : self.getNumOfPeople(size),
            'NumOfRooms' : self.getNumOfRooms(size)
        }
        return (pd.DataFrame(data))

myDataGen = DataGenerator(10000)
myDataFrame = myDataGen.genDataset()
myDataFrame.to_csv('bouced_dataset.csv', index = False)
myDataFrame.to_excel('bounced_dataset.xlsx', index = False)
