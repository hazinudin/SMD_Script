"""
This script contains class and function used to calculate AADT from RTC data.
"""
import json
import pandas as pd
import numpy as np
import os
import datetime


class TrafficSummary(object):
    def __init__(self, dataframe, date_col='SURVEY_DATE', hour_col='SURVEY_HOURS', minute_col='SURVEY_MINUTE',
                 routeid_col='LINKID', survey_direction='SURVEY_DIREC', veh_col_prefix='NUM_VEH'):
        self.df = dataframe
        self.date_col = date_col
        self.hour_col = hour_col
        self.minute_col = minute_col
        self.routeid_col = routeid_col
        self.survey_direction = survey_direction
        self.col_prefix = veh_col_prefix
        self.exclude = ['NUM_VEH1', 'NUM_VEH8']  # Exclude these veh column from AADT sum
        self.R_value = 50.54

        self.df_multiplied = self._traffic_multiplier()

    def daily_aadt(self, lane_aadt=True):
        """
        This class method calculate the AADT and daily average value of all VEH column.
        :param lane_aadt: If True then the output result is lane based, if False then the result is route based.
        :return:
        """
        veh_columns = self.veh_columns  # Get all the veh columns
        df = self._add_survey_time()  # Add the survey time column
        df.set_index('_survey_time', inplace=True)  # Set the survey time as index
        vdf_df = self.vdf_df()
        grouped = df.groupby(by=[self.routeid_col])
        resample_result = grouped[veh_columns].apply(lambda x: x.resample('1440T', label='right',
                                                     base=(x.index.min().hour*60)+x.index.min().minute).
                                                     sum().reset_index(drop=True).mean()).astype(int)

        # Calculate the CESA based on the resampled AADT.
        vdf_calc = resample_result.transpose().join(vdf_df).fillna(1)
        vdf_calc = vdf_calc.apply(lambda x: x*x['VDF'], axis=1)  # Multiply the VDF.
        vdf_calc = vdf_calc.drop('VDF', axis=1).transpose()  # Transpose to original format.
        vdf_calc['CESA'] = vdf_calc.apply(lambda x: x.sum(), axis=1)
        vdf_calc['CESA'] = vdf_calc['CESA']*365*float(self.R_value/1000000)  # Multiply with R value.

        # Create AADT column which sum all veh columns
        resample_result['AADT'] = resample_result[self.excluded_veh_cols].sum(axis=1)
        resample_result = resample_result.join(vdf_calc[['CESA']])  # Join the CESA column.

        if lane_aadt:
            veh_summary = resample_result.reset_index()  # Lane AADT
        else:
            veh_summary = resample_result.reset_index().groupby(by='LINKID').sum().reset_index()  # Route AADT

        return veh_summary

    def _traffic_multiplier(self):
        """
        Load the traffic multiplier JSON file as DataFrame.
        :return:
        """
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

    @property
    def excluded_veh_cols(self):
        all_columns = np.array(self.df.columns.tolist())
        veh_masking = np.char.startswith(all_columns, self.col_prefix)
        veh_cols = all_columns[veh_masking]

        excluded = np.setdiff1d(veh_cols, self.exclude)
        return excluded

    @staticmethod
    def vdf_df():
        """
        Load the VDF JSON file as DataFrame.
        :return:
        """
        module_dir = os.path.dirname(__file__)
        json_file = 'vdf.json'

        with open(module_dir + "/" + json_file) as f:  # Load the VDF JSON.
            vdf_dict = json.load(f)

        df = pd.DataFrame.from_dict(vdf_dict, orient='index').rename(columns={0: "VDF"})
        return df
