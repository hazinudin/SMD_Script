import os
import sys
import json
from arcpy import GetParameterAsText, SetParameterAsText, AddMessage
sys.path.append('E:\SMD_Script')  # Import the EventTable package
from EventTable import TableCheck

os.chdir('E:\SMD_Script')  # Change the directory to the SMD root directory

# Load the roughness script config JSON file
with open('RoughnessCheck/roughness_config.json') as config_f:
    config = json.load(config_f)

# Get GeoProcessing input parameter
input_JSON = GetParameterAsText(0)

# Load the input JSON
InputDetails = json.loads(input_JSON)
ColumnDetails = config['column_details']  # Load the roughness column details dictionary
TablePath = InputDetails["file_name"]

# Create a EventTableCheck class object
event_check = TableCheck.EventTableCheck(TablePath, ColumnDetails)  # The __init__ already include header check
AddMessage(event_check.header_check_result)
AddMessage(event_check.dtype_check_result)

# If the header check and data type returns None, the process can continue
if event_check.header_check_result is None and event_check.dtype_check_result is None:
    pass
else:
    if event_check.header_check_result is None:
        SetParameterAsText(1, event_check.dtype_check_result)
    else:
        SetParameterAsText(1, event_check.header_check_result)
