import os
import sys
import json
from arcpy import GetParameterAsText, SetParameterAsText, AddMessage
sys.path.append('E:\SMD_Script')  # Import the EventTable package
from EventTable import TableCheck

os.chdir('E:\SMD_Script')  # Change the directory to the SMD root directory

# Load the roughness script config JSON file
with open('RoughnessCheck/roughness_config.json') as config_f:
    roughness_config = json.load(config_f)

# Get GeoProcessing input parameter
input_JSON = GetParameterAsText(0)
AllRoute_Result = GetParameterAsText(1)

# Load the input JSON
InputDetails = json.loads(input_JSON)
TablePath = InputDetails["file_name"]
DataYear = InputDetails["year"]
Semester = InputDetails['semester']
KodeBalai = InputDetails["balai"]

# All the column details in the roughness_config.json
ColumnDetails = roughness_config['column_details']  # Load the roughness column details dictionary
UpperBound = roughness_config['upper_bound']
LowerBound = roughness_config['lower_bound']
IRIColumn = "IRI"

# GetAllRoute result containing all route from a Balai
Route_and_balai = json.loads(AllRoute_Result)
BalaiRoutes = Route_and_balai['results'][0]['routes']

# Create a EventTableCheck class object
EventCheck = TableCheck.EventTableCheck(TablePath, ColumnDetails)  # The __init__ already include header check
AddMessage(EventCheck.header_check_result)
AddMessage(EventCheck.dtype_check_result)

# If the header check and data type check returns None, the process can continue
if EventCheck.header_check_result is None and EventCheck.dtype_check_result is None:

    EventCheck.year_and_semester_check(DataYear, Semester)  # Check the year/semester value
    EventCheck.route_domain(KodeBalai, BalaiRoutes)  # Check the input route domain
    EventCheck.value_range_check(LowerBound, UpperBound, IRIColumn)  # Check the IRI value range
    EventCheck.segment_len_check()  # Check the segment length validity

    ErrorMessageList = EventCheck.error_list  # Get all the error list from the TableCheck object
    if len(ErrorMessageList) != 0:  # if there is an  error in any validation process after header and dType check
        SetParameterAsText(2, TableCheck.reject_message(ErrorMessageList))
    else:  # If there is no error
        SetParameterAsText(2, "Finish")

else:
    if EventCheck.header_check_result is None:  # If there is an error with header check
        SetParameterAsText(2, EventCheck.dtype_check_result)
    else:
        # There must be an error with the dType check
        SetParameterAsText(2, EventCheck.header_check_result)
