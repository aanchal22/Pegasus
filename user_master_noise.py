import pandas as pd
import numpy as np
from pydbgen import pydbgen
import sys

size = sys.argv[1]
size=int(size)
noise = sys.argv[2]
noise = int(noise)

class UserNameGenerator:
    def __init__(self, datasetSize = 1):
        self.pyDb = pydbgen.pydb()
        self.datasetSize = datasetSize

    def getUserName(self, size, prob = [(100-noise)/100, noise/100]):
        UserArr = []
        for i in range(size):
            UserArr.append(np.random.choice([self.pyDb.fake.email(),None], p = prob))
        return (UserArr)

    def genDataset(self):
        size = self.datasetSize
        data = {
            'UserName' : self.getUserName(size)
        }
        return (pd.DataFrame(data))

usernameGen = UserNameGenerator(size)
usernameDF = usernameGen.genDataset()
usernameDF.to_csv('other_data_n/user_master_n.csv', index = False)
