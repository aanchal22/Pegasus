import pandas as pd
import numpy as np
from pydbgen import pydbgen
import uuid
import csv
import os
import glob
import datetime
import sys

noise = sys.argv[1]
noise = int(noise)

class DataGenerator:
    def __init__(self, datasetSize = 1):
        self.pyDb = pydbgen.pydb()
        self.datasetSize = datasetSize

    def getBookingAmount(self, size, prob = [(100-noise)/100, noise/100]):
        BookAmt = []
        for i in range(size):
            r = np.random.randint(100,4000)
            BookAmt.append(np.random.choice([r,None],p =prob))
        return (BookAmt)

    def getBookingID(self, size):
        return [str(uuid.uuid4()) for _ in range(size)]


    # Generate complete dataset with properties containing array of described values
    def genDataset(self):
        size = self.datasetSize
        data = {
            'BookingAmount' : self.getBookingAmount(size),
            'BookingID' : self.getBookingID(size)
        }
        return (pd.DataFrame(data))


mycsvdir = 'bounced_data_noise/'
csvfiles = glob.glob(os.path.join(mycsvdir, '*.csv'))
dataframes = []
for csvfile in csvfiles:
    df = pd.read_csv(csvfile, usecols = ["UserName", "BouncedAt", "TimeStamp", "CheckIn", "CheckOut", "CityName", "CityId"])
    # df[‘header’] = os.path.basename(csvfile).split(‘.’)[0]
    dataframes.append(df)
BouncedDataDF = pd.concat(dataframes, ignore_index=True)

BouncedDataDF_filtered = BouncedDataDF[BouncedDataDF['BouncedAt'] == 0]
BouncedDataDF_filtered = BouncedDataDF_filtered.reset_index()
DFsize = len(BouncedDataDF_filtered)

myDataGen = DataGenerator(DFsize)
myDataFrame = myDataGen.genDataset()

BouncedDataDF_filtered.loc[:, 'RoomNights'] = (pd.to_datetime(BouncedDataDF_filtered['CheckOut'], format="%Y-%m-%d") - pd.to_datetime(BouncedDataDF_filtered['CheckIn'], format="%Y-%m-%d")).dt.days
BouncedDataDF_filtered.loc[:, 'BookingAmount'] = myDataFrame['BookingAmount']
BouncedDataDF_filtered.loc[:, 'BookingID'] = myDataFrame['BookingID']
df = BouncedDataDF_filtered.drop(columns=['index', 'BouncedAt'])

#add hotelname to the file
hotelname = pd.read_csv('other_data/city_hotel_mapping.csv')['HotelName'].to_numpy()
df.insert(loc = 4, column = 'HotelName', value = np.random.choice(hotelname, size = DFsize, replace=True))

filename = 'transacted_data_noise/transaction_data_%s.csv' % datetime.datetime.now().strftime('%s')
df.to_csv(filename, index = False)
