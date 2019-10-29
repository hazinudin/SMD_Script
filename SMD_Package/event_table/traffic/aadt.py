"""
This script contains class and function used to calculate AADT from RTC data.
"""
import json
import pandas as pd
import numpy as np
import os
import datetime


class AADT(object):
    def __init__(self, dataframe, date_col='SURVEY_DATE', hour_col='SURVEY_HOURS', minute_col='SURVEY_MINUTE',
                 routeid_col='LINKID', survey_direction='SURVEY_DIREC', veh_col_prefix='NUM_VEH'):
        self.df = dataframe
        self.date_col = date_col
        self.hour_col = hour_col
        self.minute_col = minute_col
        self.routeid_col = routeid_col
        self.survey_direction = survey_direction
        self.col_prefix = veh_col_prefix

        self.df_multiplied = self._traffic_multiplier()

    def daily_aadt(self, lane_aadt=True):
        veh_columns = self.veh_columns
        df = self._add_survey_time()
        df.set_index('_survey_time', inplace=True)  # Set the survey time as index
        grouped = df.groupby(by=[self.routeid_col, self.survey_direction])
        resample_result = grouped[veh_columns].apply(lambda x: x.resample('1440T', label='right',
                                                     base=(x.index.min().hour*60)+x.index.min().minute).
                                                     sum().reset_index(drop=True).mean())

        if lane_aadt:
            veh_summary = resample_result.reset_index()  # Lane AADT
        else:
            veh_summary = resample_result.reset_index().groupby(by='LINKID').sum().reset_index()  # Route AADT

        veh_summary['AADT'] = veh_summary.sum(axis=1)  # Create AADT column which sum all veh columns
        return veh_summary

    def _traffic_multiplier(self):
        df = self.df.copy(deep=True)
        survey_date = self.date_col
        veh_columns = self.veh_columns
        directory = os.path.dirname(__file__)

        with open(directory + "/traffic_multiplier.json") as f:  # Load the multiplier JSON file
            _multiplier = json.load(f)
            multiplier = pd.DataFrame.from_dict(_multiplier, orient='index')  # Load as pandas DataFrame
            multiplier_col = '_multiplier'
            multiplier.columns = [multiplier_col]  # Rename the column

        df['_day'] = df[survey_date].dt.dayofweek.astype(str)  # Create column for storing day (0 is Monday - 6 Sunday)
        df = df.join(multiplier, on='_day')
        df.loc[:, veh_columns] = df.apply(lambda x: x[veh_columns] * x[multiplier_col],
                                          axis=1)  # Multiply every VEH column

        return df

    def _add_survey_time(self, column='_survey_time'):
        df = self.df_multiplied.copy(deep=True)
        survey_date = self.date_col
        hour = self.hour_col
        minute = self.minute_col

        # Add new column
        df[column] = df.apply(lambda x: x[survey_date] + datetime.timedelta(hours=x[hour], minutes=x[minute]), axis=1)

        return df

    @property
    def veh_columns(self):
        all_columns = np.array(self.df.columns.tolist())
        veh_masking = np.char.startswith(all_columns, self.col_prefix)
        veh_columns = all_columns[veh_masking]

        return veh_columns
