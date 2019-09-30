"""
This script contains function used to calculate FWD d0-d200 and its normalized value. The calculation also includes
sorting process and table lookup from a database connection.
"""
import pandas as pd


def _sorting(dataframe, from_m_col, to_m_col, direction, force, closest_value=40):
    """
    This function will sort the FWD based on the force value, the row with force value with closest to 40kN will be
    picked.
    :param dataframe: The FWD input DataFrame
    :param from_m_col: The From Measure column.
    :param to_m_col: The To Measure column.
    :param force: The Force column value.
    :param closest_value: The closest value used
    :return:
    """

    df = dataframe  # The input dataframe