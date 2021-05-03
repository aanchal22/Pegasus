import pandas as pd
import numpy as np
from pydbgen import pydbgen
import math
import csv

pr = [0.63, 0.37]
size = 10000


class DataGenerator:
    def __init__(self, datasetSize=1):
        self.pyDb = pydbgen.pydb()
        self.datasetSize = datasetSize
        self.cityname = pd.read_csv('other_data/city_master.csv')['CityName'].to_numpy()
        self.username = pd.read_csv('other_data/user_master.csv')['UserName'].to_numpy()

    # Generate array of bounced-at levels, select b/w 0 & 1
    def getBouncedAt(self, size, levels=[0, 1], prob=[0.5, 0.5]):
        return (np.random.choice(levels, size=size, p=prob))


    # Generate array of usernames from username dataset
    def getUserName(self, size):
        return (np.random.choice(self.username, size=size, replace=True))

    # Generate array of citynames from city dataset table
    def getCityName(self, size):
        return (np.random.choice(self.cityname, size=size, replace=True))

    # Generate array of random timestamps
    def getTimeStamp(self, size, low=0, high=365):
        randomInt = np.random.uniform(low, high, size).astype(int)
        randomHr = np.random.randint(0, 24, size)
        randomMin = np.random.randint(0, 60, size)
        randomSec = np.random.randint(0, 60, size)

        randomHr = pd.DataFrame(randomHr)
        randomHr.rename(columns={0: 'I'}, inplace=True)
        randomHr['I'] = randomHr['I'].astype(str)
        randomHr.loc[(randomHr['I'].str.len() == 1),
                     'I'] = '0' + randomHr['I'].astype(str)

        randomMin = pd.DataFrame(randomMin)
        randomMin.rename(columns={0: 'I'}, inplace=True)
        randomMin['I'] = randomMin['I'].astype(str)
        randomMin.loc[(randomMin['I'].str.len() == 1),
                      'I'] = '0' + randomMin['I'].astype(str)

        randomSec = pd.DataFrame(randomSec)
        randomSec.rename(columns={0: 'I'}, inplace=True)
        randomSec['I'] = randomSec['I'].astype(str)
        randomSec.loc[(randomSec['I'].str.len() == 1),
                      'I'] = '0' + randomSec['I'].astype(str)
        randomTime = randomHr['I'] + ':' + \
            randomMin['I'] + ':' + randomSec['I']
        randomTime = randomTime.to_numpy()

        timeArr = np.datetime64('today') - randomInt
        timeArr = np.datetime_as_string(timeArr)

        arr_list = [timeArr, randomTime]
        final_time_arr = np.apply_along_axis(' '.join, 0, arr_list)
        return (final_time_arr)

    # Generate array of random device ID's
    def getDeviceID(self, size):
        return (np.random.randint(1000000, 10000000, size=size))

    # Generate array of device OS
    def getFlavor(self, size, levels=['Android', 'IOS'], prob=[0.6, 0.4]):
        return (np.random.choice(levels, size=size, p=prob))

    # Generate array with int values specifying number of people
    def getNumOfPeople(self, size):
        return (np.random.randint(1, 10, size=size))

    # Generate array of Checkin Dates
    def getCheckIn(self, size, low=0, high=120):
        randomInt = np.random.uniform(low, high, size).astype(int)
        checkInDateArr = np.datetime64('today') + randomInt
        return (checkInDateArr)

    # Generate complete dataset with properties containing array of described values
    def genDataset(self):
        size = self.datasetSize
        data = {
            'BouncedAt': self.getBouncedAt(size),
            'UserName': self.getUserName(size),
            'CityName': self.getCityName(size),
            'TimeStamp': self.getTimeStamp(size),
            'DeviceID': self.getDeviceID(size),
            'Flavor': self.getFlavor(size),
            'CheckIn': self.getCheckIn(size),
            'NumOfPeople': self.getNumOfPeople(size)
        }
        return (pd.DataFrame(data))


myDataGen = DataGenerator(size)
myDataFrame = myDataGen.genDataset()


# insert NumOfRooms to the dataframe
room_calculator = pd.DataFrame(data={'People': myDataFrame['NumOfPeople']})
room_calculator['People_2'] = (
    room_calculator['People']/2).apply(np.ceil).astype(int)
room_calculator['range'] = [
    list(range(i, j+1)) for i, j in room_calculator[['People_2', 'People']].values]
room_calculator.drop(['People_2', 'People'], axis=1)
room_calculator['Rooms'] = room_calculator['range'].apply(np.random.choice)
myDataFrame['NumofRooms'] = room_calculator['Rooms']
del room_calculator


# insert Checkout to the dataframe
randomInt = np.random.randint(1,30)
myDataFrame['CheckOut'] = (myDataFrame['CheckIn'] + np.timedelta64(randomInt, 'D'))

myDataFrame.to_csv('bounced_data/bounced_data.csv', index=False)
