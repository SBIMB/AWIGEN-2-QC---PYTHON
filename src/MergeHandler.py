from functools import reduce
import pandas as pd


class MergeHandler:
    # pass functions in Instrument class here
    # for every instrument passed in, add to a list and merge them using study_id
    def __init__(self, *argv):
        self.dfs = []
        for arg in argv:
            self.dfs.append(arg)

    # merge all the instruments using the study id
    # returns a merged dataset which can be passed to DataAnalyzer class
    def join_data_frames(self):
        dataset = reduce(lambda left, right: pd.merge(left, right, on='study_id'), self.dfs)
        return dataset