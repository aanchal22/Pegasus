import pandas as pd
import numpy as np
from pydbgen import pydbgen
import matplotlib.pyplot as plt

class DataGenerator:
    def __init__(self, datasetSize = 1):
        self.pyDb = pydbgen.pydb()
        self.datasetSize = datasetSize

    def getCityName(self, size):
        cityarr = np.array([self.pyDb.city_real() for i in range(size)])
        return cityarr

    def getCityType(self, size, levels = ['Business', 'Leisure'], prob = [0.28,0.72]):
        citytypearr = np.random.choice(levels, size = size, p = prob)
        return citytypearr

    # Generate complete dataset with properties containing array of described values
    def genDataset(self):
        size = self.datasetSize
        data = {
            'CityName' : self.getCityName(size),
            'CityType' : self.getCityType(size)
        }
        return (pd.DataFrame(data))

myDataGen = DataGenerator(1000)
myDataFrame = myDataGen.genDataset()
myDataFrame.to_csv('city_dataset.csv', index = False)