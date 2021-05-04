import pandas as pd
import numpy as np
from pydbgen import pydbgen
import uuid
import sys

size = sys.argv[1]
size = int(size)
noise = sys.argv[2]
noise = int(noise)

class DataGenerator:
    def __init__(self, datasetSize = 1):
        self.pyDb = pydbgen.pydb()
        self.datasetSize = datasetSize

    def getCityName(self, size, prob = [(100-noise)/100, noise/100]):
        cityarr = []
        for i in range(size):
            cityarr.append(np.random.choice([self.pyDb.city_real(), None], p = prob))
        return cityarr

    def getCityType(self, size, levels = ['Business', 'Leisure', None], prob = [(100-noise)/200, (100-noise)/200, noise/100]):
        citytypearr = np.random.choice(levels, size = size, p = prob)
        return citytypearr

    def getCityID(self, size, prob = [(100-noise)/100, noise/100]):
        cityid = []
        for i in range(size):
            cityid.append(np.random.choice([str(uuid.uuid4()), None], p = prob))
        return cityid

    # Generate complete dataset with properties containing array of described values
    def genDataset(self):
        size = self.datasetSize
        data = {
            'CityName' : self.getCityName(size),
            'CityType' : self.getCityType(size),
            'CityId'   : self.getCityID(size)
        }
        return (pd.DataFrame(data))

myDataGen = DataGenerator(size)
myDataFrame = myDataGen.genDataset()
myDataFrame.to_csv('other_data_n/city_master_n.csv', index = False)
