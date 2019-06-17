from arcpy import da, env
from pandas import DataFrame
import numpy as np


def verify_balai(input_code, balai_table, balai_col, environment):
    env.workspace = environment  # Setting up the GDB environment

    if type(input_code) != list:  # Check if they input value is in list type
        input = np.array([input_code])
    else:
        input = np.array(input_code)  # If the input is already in list type

    ar = da.FeatureClassToNumPyArray(balai_table, balai_col)  # The balai col in numpy format
    balai_series = DataFrame(ar)[balai_col]  # Create a list from

    # Check if all the request is in the balai code domain
    result = np.any(np.in1d(input, balai_series))

    return result
