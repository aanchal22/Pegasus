import pandas as pd
import numpy as np
from pydbgen import pydbgen
import matplotlib.pyplot as plt

pr = [0.63,0.37]

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
            'NumOfPeople' : self.getNumOfPeople(size)
        }
        return (pd.DataFrame(data))

myDataGen = DataGenerator(10000)
myDataFrame = myDataGen.genDataset()

# insert NumOfRooms in the excel
myDataFrame.insert(loc = 7, column = 'NumofRooms', value = 0)

for i in range(10000):
	x = myDataFrame.loc[i, 'NumOfPeople']
	if(x%2==0):
		y=x/2
		levels = [y,x]
	else:
		y=(x/2)+1
		levels = [y,x]

	myDataFrame.loc[i, 'NumofRooms'] = np.random.choice(levels, size = 1, p = pr).astype(int)

#insert Checkout in the excel
myDataFrame.insert(loc = 6, column = 'CheckOut', value = 0)

for i in range(10000):
	x = myDataFrame.loc[i, 'CheckIn']
	randomInt = np.random.randint(1,30)

	myDataFrame.loc[i, 'CheckOut'] = (x + np.timedelta64(randomInt, 'D')).date()

myDataFrame.to_csv('bouced_dataset.csv', index = False)
# myDataFrame.to_excel('bounced_dataset.xlsx', index = False)x
