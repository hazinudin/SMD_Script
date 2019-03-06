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
AllRoute_Result = GetParameterAsText(1)

# Load the input JSON
InputDetails = json.loads(input_JSON)
ColumnDetails = config['column_details']  # Load the roughness column details dictionary
TablePath = InputDetails["file_name"]

# Create a EventTableCheck class object
EventCheck = TableCheck.EventTableCheck(TablePath, ColumnDetails)  # The __init__ already include header check
AddMessage(EventCheck.header_check_result)
AddMessage(EventCheck.dtype_check_result)

# If the header check and data type check returns None, the process can continue
if EventCheck.header_check_result is None and EventCheck.dtype_check_result is None:
    # Check the year and semester value
    EventCheck.year_and_semester_check(InputDetails['year'], InputDetails['semester'])
    SetParameterAsText(2, EventCheck.error_list)
else:
    if EventCheck.header_check_result is None:
        SetParameterAsText(2, EventCheck.dtype_check_result)
    else:
        SetParameterAsText(2, EventCheck.header_check_result)
