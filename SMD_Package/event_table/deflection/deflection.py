"""
This script contains function used to calculate FWD/LWD d0-d200 and its normalized value. The calculation also includes
sorting process and table lookup from a database connection.
"""


class Deflection(object):
    def __init__(self, df, force_col, type, route_col='LINKID', from_m_col='STA_FROM', to_m_col='STA_TO',
                 survey_direc='SURVEY_DIREC', force_ref=40):
        """
        This class is used to calculate D0-D200 value for FWD/LWD
        :param df: The input Pandas DataFrame
        :param route_col: The route id column
        :param from_m_col: The From Measure column
        :param to_m_col: The To Measure column
        :param survey_direc: The Survey Direction column
        :param force_col: The Force/Load column
        :param type: FWD or LWD data set.
        :param force_ref: The value of reference force in kN.
        """
        if type == 'FWD' and (survey_direc is None):
            raise ValueError("Type is FWD but survey_direc is None")

        self.df = df.copy(deep=True)
        self.force_col = force_col
        self.route_col = route_col
        self.from_m = from_m_col
        self.to_m = to_m_col
        self.force_ref = force_ref
        self.survey_direc = survey_direc

        if type == 'FWD':
            self.sorted = self._sorting()
        elif type == 'LWD':
            self.sorted = self.df

    def _sorting(self):
        """
        This class method sort the input DataFrame to get the closest row to referenced force value.
        :return:
        """
        self.df['_ref_diff'] = self.df[self.force_col]-self.force_ref  # Create the diff column
        self.df['_ref_diff'] = self.df['_ref_diff'].abs()  # Get only the absolute value
        grouped = self.df.groupby([self.route_col, self.survey_direc, self.from_m, self.to_m])  # Do a group by
        closest_index = grouped['_ref_diff'].idxmin()  # Get the closest index
        closest_row = self.df.loc[closest_index]  # Get the closest rows

        return closest_row  # Return closest row
