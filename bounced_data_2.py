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
