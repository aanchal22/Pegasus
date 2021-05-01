import pandas as pd
import numpy as np
from pydbgen import pydbgen
import matplotlib.pyplot as plt

class DataGenerator:
    def __init__(self, datasetSize = 1):
        self.pyDb = pydbgen.pydb()
        self.datasetSize = datasetSize


    # Generate array of bounced-at levels, select b/w 0 & 1
    def getFirstTxnDate(self, size, low = 0, high = 365):
        randomInt = np.random.uniform(low, high, size).astype(int)
        timeArr = np.datetime64('today') - randomInt
        return (timeArr)

    # Generate array of random timestamps
    def getInactivityRatio(self, size):
        return (np.random.uniform(low = 0, high = 1, size = size))

    # Generate array of random device ID's
    def getTotalSearch(self, size):
        return (np.random.randint(1,10000, size=size))

    # Generate array of device OS
    def getTxnTag(self, size):
        return (np.random.uniform(low = 0, high = 1000, size = size))

    # Generate array with int values specifying number of people
    def getDhTag(self, size):
        return (np.random.uniform(low = 0, high = 1000, size = size))

    # Generate array with int values specifying number of rooms
    def getADGBT(self, size):
        return (np.random.randint(1,10,size = size))

    # Generate complete dataset with properties containing array of described values
    def genDataset(self):
        size = self.datasetSize
        data = {
            'FirsstTxnDate' : self.getFirstTxnDate(size),
            'InactivityRatio' : self.getInactivityRatio(size),
            'TotalSearch' : self.getTotalSearch(size),
            'TxnTag' : self.getTxnTag(size),
            'DhTag' : self.getDhTag(size),
            'ADGBT' : self.getADGBT(size)
        }
        return (pd.DataFrame(data))

myDataGen = DataGenerator(10000)
myDataFrame = myDataGen.genDataset()
UserNameDF = pd.read_csv("bouced_dataset.csv", usecols = ["UserName"])
myDataFrame.insert(loc = 0, column = 'UserName', value = UserNameDF)
myDataFrame.to_csv('marketing_profiling.csv', index = False)
myDataFrame.to_excel('marketing_profiling.xlsx', index = False)