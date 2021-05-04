import pandas as pd
import numpy as np
from pydbgen import pydbgen
import math
import csv
import datetime
import sys

pr = [0.63, 0.37]
size = sys.argv[1]
# size = 10000
size = int(size)

class DataGenerator:
    def __init__(self, datasetSize=1):
        self.pyDb = pydbgen.pydb()
        self.datasetSize = datasetSize
        self.cityname = pd.read_csv('other_data/city_master.csv')['CityName'].to_numpy()
        self.username = pd.read_csv('other_data/user_master.csv')['UserName'].to_numpy()
        self.username = self.username[:50]
    # Generate array of bounced-at levels, select b/w 0 & 1
    def getBouncedAt(self, size, levels=[0, 1], prob=[0.3, 0.7]):
        return (np.random.choice(levels, size=size, p=prob))


    # Generate array of usernames from username dataset
    def getUserName(self, size):
        return (np.random.choice(self.username, size=size, replace=True))


    # Generate array of random timestamps
    def getTimeStamp(self, size, low=0, high=365):
        final_time_arr= np.full((size), np.datetime64('today'))
        # randomInt = np.random.uniform(low, high, size).astype(int)
        # randomHr = np.random.randint(0, 24, size)
        # randomMin = np.random.randint(0, 60, size)
        # randomSec = np.random.randint(0, 60, size)

        # randomHr = pd.DataFrame(randomHr)
        # randomHr.rename(columns={0: 'I'}, inplace=True)
        # randomHr['I'] = randomHr['I'].astype(str)
        # randomHr.loc[(randomHr['I'].str.len() == 1),
        #              'I'] = '0' + randomHr['I'].astype(str)

        # randomMin = pd.DataFrame(randomMin)
        # randomMin.rename(columns={0: 'I'}, inplace=True)
        # randomMin['I'] = randomMin['I'].astype(str)
        # randomMin.loc[(randomMin['I'].str.len() == 1),
        #               'I'] = '0' + randomMin['I'].astype(str)

        # randomSec = pd.DataFrame(randomSec)
        # randomSec.rename(columns={0: 'I'}, inplace=True)
        # randomSec['I'] = randomSec['I'].astype(str)
        # randomSec.loc[(randomSec['I'].str.len() == 1),
        #               'I'] = '0' + randomSec['I'].astype(str)
        # randomTime = randomHr['I'] + ':' + \
        #     randomMin['I'] + ':' + randomSec['I']
        # randomTime = randomTime.to_numpy()

        # timeArr = np.datetime64('today') - randomInt
        # timeArr = np.datetime_as_string(timeArr)

        # arr_list = [timeArr, randomTime]
        # final_time_arr = np.apply_along_axis(' '.join, 0, arr_list)
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
        checkInDateArr = np.datetime64('today') 
        return (checkInDateArr)

    # Generate complete dataset with properties containing array of described values
    def genDataset(self):
        size = self.datasetSize
        data = {
            'BouncedAt': self.getBouncedAt(size),
            'UserName': self.getUserName(size),
            'TimeStamp': self.getTimeStamp(size),
            'DeviceID': self.getDeviceID(size),
            'Flavor': self.getFlavor(size),
            'CheckIn': self.getCheckIn(size),
            'NumOfPeople': self.getNumOfPeople(size)
        }
        return (pd.DataFrame(data))


# myDataGen = DataGenerator(size)
# myDataFrame = myDataGen.genDataset()
# # myDataFrame.insert(loc = 3, column = 'CityID', value = 0)

# CityD = pd.read_csv("other_data/city_master.csv", usecols = ["CityName","CityId"]).to_numpy()
# citydetails = CityD[np.random.choice(CityD.shape[0], size, replace = True)]
# d = pd.DataFrame(citydetails, columns=['CityName', 'CityId'])
# myDataFrame = pd.concat([myDataFrame, d], axis=1)

# # insert NumOfRooms to the dataframe
# room_calculator = pd.DataFrame(data={'People': myDataFrame['NumOfPeople']})
# room_calculator['People_2'] = (
#     room_calculator['People']/2).apply(np.ceil).astype(int)
# room_calculator['range'] = [
#     list(range(i, j+1)) for i, j in room_calculator[['People_2', 'People']].values]
# room_calculator.drop(['People_2', 'People'], axis=1)
# room_calculator['Rooms'] = room_calculator['range'].apply(np.random.choice)
# myDataFrame['NumofRooms'] = room_calculator['Rooms']
# del room_calculator


# # insert Checkout to the dataframe
# randomInt = np.random.randint(1, 30, size=size)
# randomtime = [np.timedelta64(z,'D') for z in randomInt]

# myDataFrame['CheckOut'] = pd.to_datetime(myDataFrame['CheckIn']) + pd.to_timedelta(randomtime)

# filename = 'bounced_data/bounced_data_%s.csv' % datetime.datetime.now().strftime('%s')
# myDataFrame.to_csv(filename, index=False)

usernames = pd.read_csv('other_data/user_master.csv')['UserName'].sample(frac=0.01).values.tolist()
# usernames = pd.read_csv('other_data/user_master.csv')['UserName'].head(3000).values.tolist()
CityD = pd.read_csv("other_data/city_master.csv", usecols = ["CityName","CityId"]).to_numpy()

rows_to_append = []
for username in usernames:
    randomInt = np.random.randint(1, 7)
    device_id = np.random.randint(1000000, 10000000)
    flavour = np.random.choice(['Android', 'IOS'],p=[0.6, 0.4])
    checkin = (datetime.datetime.now() + datetime.timedelta(np.random.randint(0, 120))).date()
    checkout = checkin + datetime.timedelta(np.random.randint(0, 3))
    timestamp = datetime.datetime.now() - datetime.timedelta(np.random.randint(0, 60))
    numpeople = np.random.randint(1, 6)
    numrooms = np.random.randint(np.ceil(numpeople/2), numpeople) if numpeople > 1 else 1
    city = CityD[np.random.choice(CityD.shape[0],replace = True)]
    cityname = city[0]
    cityid = city[1]
    for n in range(randomInt+1):
        if n > randomInt/2:
            if np.random.choice([0,1], p=[0.4, 0.6]) == 1:
                city = CityD[np.random.choice(CityD.shape[0],replace = True)]
                cityname = city[0]
                cityid = city[1]
                checkin = (datetime.datetime.now() + datetime.timedelta(np.random.randint(0, 90))).date()
                checkout = checkin + datetime.timedelta(np.random.randint(0, 7))
                timestamp = datetime.datetime.now() - datetime.timedelta(np.random.randint(0, 60))
                numpeople = np.random.randint(1, 6)
                numrooms = np.random.randint(np.ceil(numpeople/2), numpeople) if numpeople > 1 else 1
        if n < randomInt:
            ba = 1
        else:
            ba = np.random.choice([0,1], p=[0.2, 0.8])
        
        timestamp = timestamp + datetime.timedelta(minutes=3)
        # BouncedAt,UserName,TimeStamp,DeviceID,Flavor,CheckIn,NumOfPeople,CityName,CityId,NumofRooms,CheckOut
        column = {"UserName": username, "BouncedAt": ba, "TimeStamp": timestamp, "DeviceID": device_id,
                    "Flavor": flavour, "CheckIn": checkin, "NumOfPeople": numpeople, 
                    "CityName": cityname, "CityId": cityid, "NumofRooms": numrooms, "CheckOut": checkout}
        rows_to_append.append(column)
        
myDataFrame = pd.DataFrame(rows_to_append)
myDataFrame = myDataFrame[['BouncedAt', 'UserName', 'TimeStamp', 'DeviceID', 'Flavor', 'CheckIn', 'NumOfPeople', 'CityName', 'CityId', 'NumofRooms', 'CheckOut']]
filename = 'bounced_data/bounced_data_%s.csv' % datetime.datetime.now().strftime('%s')
myDataFrame.to_csv(filename, index=False)
