"""
This script contains class and function used to calculate AADT from RTC data.
"""
import json
import pandas as pd
import numpy as np
import os


class AADT(object):
    def __init__(self, dataframe, date_col, hour_col, minute_col, veh_col_prefix='NUM_VEH'):
        self.df = dataframe
        self.date_col = date_col
        self.col_prefix = veh_col_prefix
        self.df_multiplied = self._traffic_multiplier()

    def _traffic_multiplier(self):
        df = self.df.copy(deep=True)
        survey_date = self.date_col
        col_prefix = self.col_prefix
        columns = df.columns.tolist()
        veh_columns = np.char.startswith(columns, col_prefix)
        directory = os.path.dirname(__file__)

        with open(directory + "/traffic_multiplier.json") as f:  # Load the multiplier JSON file
            _multiplier = json.load(f)
            multiplier = pd.DataFrame.from_dict(_multiplier, orient='index')  # Load as pandas DataFrame
            multiplier_col = '_multiplier'
            multiplier.columns = [multiplier_col]  # Rename the column

        df['_day'] = df[survey_date].dt.dayofweek.astype(
            str)  # Create column for storing day (0 is Monday - 6 Sunday)
        df = df.join(multiplier, on='_day')
        df.loc[:, veh_columns] = df.apply(lambda x: x[veh_columns] * x[multiplier_col],
                                          axis=1)  # Multiply every VEH column

        return df
