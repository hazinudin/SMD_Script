"""
This script contains function used to calculate FWD/LWD d0-d200 and its normalized value. The calculation also includes
sorting process and table lookup from a database connection.
"""
import json
import pandas as pd
import os
import numpy as np


class Deflection(object):
    def __init__(self, df, force_col, data_type, d0_col, d200_col, asp_temp, routeid_col='LINKID', from_m_col='FROM_STA',
                 to_m_col='TO_STA', survey_direc='SURVEY_DIREC', surf_thickness_col='SURF_THICKNESS', force_ref=40,
                 routes='ALL', **kwargs):
        """
        This class is used to calculate D0-D200 value for FWD/LWD
        :param df: The input Pandas DataFrame
        :param d0_col: The D0 column.
        :param d200_col: The d200 column.
        :param asp_temp: The asphalt temperature column.
        :param routeid_col: The route id column
        :param from_m_col: The From Measure column
        :param to_m_col: The To Measure column
        :param survey_direc: The Survey Direction column
        :param surf_thickness_col: The survey thickness column.
        :param force_col: The Force/Load column
        :param data_type: FWD or LWD data set.
        :param force_ref: The value of reference force in kN.
        """
        if data_type == 'FWD' and (survey_direc is None):
            raise ValueError("Type is FWD but survey_direc is None")

        if routes == 'ALL':
            self.df = df.copy(deep=True)
        elif type(routes) == list:
            self.df = df.loc[df[routeid_col].isin(routes)].copy(deep=True)
        else:
            self.df = df.loc[df[routeid_col] == routes].copy(deep=True)

        self.force_col = force_col
        self.route_col = routeid_col
        self.from_m = from_m_col
        self.to_m = to_m_col
        self.force_ref = force_ref
        self.survey_direc = survey_direc
        self.d0_col = d0_col
        self.d200_col = d200_col
        self.surf_thickness_col = surf_thickness_col
        self.norm_d0 = 'NORM_'+self.d0_col
        self.norm_d200 = 'NORM_'+self.d200_col
        self.corr_d0 = 'CORR_'+self.d0_col
        self.corr_d200 = 'CORR_'+self.d200_col
        self.curvature = 'D0_D200'
        self.corr_curvature = 'CORR_D0_D200'

        if data_type == 'FWD':
            self.sorted = self._sorting()
        elif data_type == 'LWD':
            self.sorted = self.df

        if self.sorted is not None:
            self.sorted[[self.norm_d0, self.norm_d200]] = self._normalized_d0_d200()  # Create and fill the normalized columns
            self.sorted[self.curvature] = self.sorted[self.norm_d0]-self.sorted[self.norm_d200]  # The d0-d200 columns
            self.ampt_tlap = 41/self.sorted[asp_temp]  # The AMPT/TLAP series.
            self._temp_correction('d200_temp_correction.json', self.norm_d200, self.corr_d200)
            self._temp_correction('d0_temp_correction.json', self.norm_d0, self.corr_d0)
            self.sorted[self.corr_curvature] = self.sorted[self.corr_d0]-self.sorted[self.corr_d200]
            self.sorted.drop(['OBJECTID', 'SURVEY_DATE', 'UPDATE_DATE'], axis=1, inplace=True)

    def _sorting(self):
        """
        This class method sort the input DataFrame to get the closest row to referenced force value.
        :return:
        """
        if np.all(self.df[self.force_col].isnull()): # If all the row in Force column is Null.
            return None

        self.df['_ref_diff'] = self.df[self.force_col]-self.force_ref  # Create the diff column
        self.df['_ref_diff'] = self.df['_ref_diff'].abs()  # Get only the absolute value
        grouped = self.df.groupby([self.route_col, self.survey_direc, self.from_m, self.to_m])  # Do a group by
        closest_index = grouped['_ref_diff'].idxmin()  # Get the closest index
        closest_row = self.df.loc[closest_index].drop('_ref_diff', axis=1)  # Get the closest rows

        return closest_row  # Return closest row

    def _normalized_d0_d200(self):
        """
        This class method calculate the normalized value of D0 and D200 column.
        :return:
        """
        result = self.sorted[[self.d0_col, self.d200_col, self.force_col]].\
            apply(lambda x: (self.force_ref/x[self.force_col])*(x/1000), axis=1)  # The normalized calculation

        return result[[self.d0_col, self.d200_col]]  # Only return the D0 and D200 column

    def _temp_correction(self, lookup_table_path, deflection_col, corrected_col):
        """
        This class method calculate the temperature corrected value of D0 and D200.
        """
        module_folder = os.path.dirname(__file__)
        table_path = os.path.join(module_folder, lookup_table_path)

        with open(table_path) as j_file:
            lookup_dict = json.load(j_file)

        lookup_df = pd.DataFrame.from_dict(lookup_dict, orient='index')
        lookup_df.index = lookup_df.index.map(float)
        lookup_df.columns = lookup_df.columns.map(int)  # Change the column from string to integer.
        lookup_thickness = list(lookup_df)  # Available thickness from the lookup table columns.

        input_thickness = self.sorted[self.surf_thickness_col].apply(lambda x: lookup_thickness[
            np.argmin([abs(_ - x) for _ in lookup_thickness])
        ])

        temp_factor = lookup_df.lookup(self.ampt_tlap.apply(lambda x: np.round(x, 1) if np.round(x, 1) < 1.8 else 1.8),
                                       input_thickness)
        self.sorted[corrected_col] = self.sorted[deflection_col]*temp_factor

        return self
