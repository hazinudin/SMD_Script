from arcpy import da, env
from pandas import DataFrame
import numpy as np
from SMD_Package import SMDConfigs


def verify_balai(input_code, environment, return_false=True):
    env.workspace = environment  # Setting up the GDB environment
    smd_config = SMDConfigs()

    balai_table = smd_config.table_names['balai_table']
    balai_col = smd_config.table_fields['balai_table']['balai_code']

    if type(input_code) != list:  # Check if they input value is in list type
        input = np.array([input_code])
    else:
        input = np.array(input_code)  # If the input is already in list type

    ar = da.FeatureClassToNumPyArray(balai_table, balai_col)  # The balai col in numpy format
    balai_series = DataFrame(ar)[balai_col]  # Create a list from

    # Check if all the request is in the balai code domain
    mask = np.in1d(input, balai_series)

    if return_false:
        result = input[np.invert(mask)].tolist()
    else:
        result = input[mask].tolist()

    return result
