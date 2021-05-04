import pandas as pd
import numpy as np
from pydbgen import pydbgen
import uuid

size = 2000

HotelName = ['Palm Woods', 'The Robe Hotel', 'Hotel City Lake', 'The Pink Sheet', 'Carpe Diem']

CityDF = pd.read_csv("other_data/city_master.csv", usecols = ["CityName","CityId"])

# insert NumOfRooms in the excel
CityDF.insert(loc = 1, column = 'HotelName', value = 0)
CityDF.insert(loc = 2, column = 'City', value = 0)
CityDF.insert(loc = 3, column = 'CityID', value = 0)
CityDF.insert(loc = 4, column = 'HotelID', value = 0)

CityDF['HotelID'] = [str(uuid.uuid4()) for _ in range(size)]

c = 0

for i in range(size):
    x = CityDF.loc[i, 'CityName']
    y = CityDF.loc[i, 'CityId']
    for j in range (5):
        CityDF.loc[c + (i+j),'HotelName'] = HotelName[j]
        CityDF.loc[c + (i+j),'City'] = x
        CityDF.loc[c + (i+j),'CityID'] = y
    c += 4

CityDF = CityDF.drop(columns = ['CityName', 'CityId'])

CityDF.to_csv('other_data/city_hotel_mapping.csv', index = False)
