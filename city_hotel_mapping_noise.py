import pandas as pd
import numpy as np
from pydbgen import pydbgen
import uuid
import sys

size = sys.argv[1]
size = int(size)
noise = sys.argv[2]
noise = int(noise)
prob = [(100-noise)/100, noise/100]

HotelName = ['Palm Woods', 'The Robe Hotel', 'Hotel City Lake', 'The Pink Sheet', 'Carpe Diem']

CityDF = pd.read_csv("other_data_n/city_master_n.csv", usecols = ["CityName","CityId"])

# insert NumOfRooms in the excel
CityDF.insert(loc = 1, column = 'HotelName', value = 0)
CityDF.insert(loc = 2, column = 'City', value = 0)
CityDF.insert(loc = 3, column = 'CityID', value = 0)
CityDF.insert(loc = 4, column = 'HotelID', value = 0)

hotelid = []
for i in range(size):
    hotelid.append(np.random.choice([str(uuid.uuid4()), None], p = prob))

CityDF['HotelID'] = hotelid


c = 0

for i in range(size):
    x = CityDF.loc[i, 'CityName']
    y = CityDF.loc[i, 'CityId']
    for j in range (5):
        CityDF.loc[c + (i+j),'HotelName'] = np.random.choice([HotelName[j], None], p = prob)
        CityDF.loc[c + (i+j),'City'] = np.random.choice([x, None], p = prob)
        CityDF.loc[c + (i+j),'CityID'] = np.random.choice([y, None], p =prob)
    c += 4

CityDF = CityDF.drop(columns = ['CityName', 'CityId'])

CityDF.to_csv('other_data_n/city_hotel_mapping_n.csv', index = False)
