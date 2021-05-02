import pandas as pd
import numpy as np
from pydbgen import pydbgen
import matplotlib.pyplot as plt

HotelName = ['Palm Woods', 'The Robe Hotel', 'Hotel City Lake', 'The Pink Sheet', 'Carpe Diem']

CityDF = pd.read_csv("city_dataset.csv", usecols = ["CityName"])

# insert NumOfRooms in the excel
CityDF.insert(loc = 1, column = 'HotelName', value = 0)
CityDF.insert(loc = 2, column = 'City', value = 0)
c = 0

for i in range(1000):
    x = CityDF.loc[i, 'CityName']
    for j in range (5):
        CityDF.loc[c + (i+j),'HotelName'] = HotelName[j]
        CityDF.loc[c + (i+j),'City'] = x
    c += 4

CityDF = CityDF.drop(columns = ['CityName'])

CityDF.to_csv('city_hotel.csv', index = False)
