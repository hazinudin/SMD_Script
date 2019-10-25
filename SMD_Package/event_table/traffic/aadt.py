"""
This script contains class and function used to calculate AADT from RTC data.
"""
import json
import pandas as pd
import numpy as np
import os


def _daily_average(group, survey_time_col, col_prefix='NUM_VEH'):
    result = dict()
    columns = group.columns.tolist()
    veh_masking = np.char.startswith(columns, col_prefix)
    veh_cols = np.array(columns)[veh_masking]
    group.set_index(survey_time_col, inplace=True)
    sum_result = group[veh_cols].resample('1440T', label='right', base=375).sum().reset_index(drop=True)
    daily_average = sum_result.mean()

    return daily_average


def _traffic_multiplier(dataframe, survey_date, col_prefix='NUM_VEH'):
    df = dataframe
    columns = df.columns.tolist()
    veh_columns = np.char.startswith(columns, col_prefix)
    directory = os.path.dirname(__file__)

    with open(directory + "/traffic_multiplier.json") as f:  # Load the multiplier JSON file
        _multiplier = json.load(f)
        multiplier = pd.DataFrame.from_dict(_multiplier, orient='index')  # Load as pandas DataFrame
        multiplier_col = '_multiplier'
        multiplier.columns = [multiplier_col]  # Rename the column

    df['_day'] = df[survey_date].dt.dayofweek.astype(str)  # Create column for storing day (0 is Monday - 6 Sunday)
    df = df.join(multiplier, on='_day')
    df.loc[:, veh_columns] = df.apply(lambda x: x[veh_columns]*x[multiplier_col], axis=1)  # Multiply every VEH column

    return df
