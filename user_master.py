import pandas as pd
import numpy as np
from pydbgen import pydbgen
import sys

size = sys.argv[1]
size=int(size)
class UserNameGenerator:
    def __init__(self, datasetSize = 1):
        self.pyDb = pydbgen.pydb()
        self.datasetSize = datasetSize

    def getUserName(self, size):
        return (np.array([self.pyDb.fake.email() for i in range(size)]))

    def genDataset(self):
        size = self.datasetSize
        data = {
            'UserName' : self.getUserName(size)
        }
        return (pd.DataFrame(data))

usernameGen = UserNameGenerator(size)
usernameDF = usernameGen.genDataset()
usernameDF.to_csv('other_data/user_master.csv', index = False)
