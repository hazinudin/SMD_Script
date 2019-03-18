import os
import sys
import json
from arcpy import GetParameterAsText, SetParameterAsText, AddMessage
sys.path.append('E:\SMD_Script')  # Import the SMD_Package package
from SMD_Package import EventValidation, reject_message

os.chdir('E:\SMD_Script')  # Change the directory to the SMD root directory

# Load the roughness script config JSON file
with open('RoughnessCheck/roughness_config.json') as config_f:
    roughness_config = json.load(config_f)

# Load the SMD config JSON file
with open('smd_config.json') as smd_config_f:
    smd_config = json.load(smd_config_f)

# The smd config JSON details
LrsNetwork = smd_config['table_names']['lrs_network']
LrsNetworkRID = smd_config['table_fields']['lrs_network']['route_id']

RNIEventTable = smd_config['table_names']['rni']
RNIRouteID = smd_config['table_fields']['rni']['route_id']
RNIFromMeasure = smd_config['table_fields']['rni']['from_measure']
RNIToMeasure = smd_config['table_fields']['rni']['to_measure']
RNILaneCode = smd_config['table_fields']['rni']['lane_code']

dbConnection = smd_config['smd_database']['instance']

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
SearchRadius = roughness_config['search_radius']
IRIColumn = "IRI"

# GetAllRoute result containing all route from a Balai
Route_and_balai = json.loads(AllRoute_Result)
BalaiRoutes = Route_and_balai['results'][0]['routes']

# Create a EventTableCheck class object
# The __init__ already include header check
EventCheck = EventValidation(TablePath, ColumnDetails, LrsNetwork, dbConnection)

# If the header check and data type check returns None, the process can continue
if EventCheck.header_check_result is None:

    EventCheck.year_and_semester_check(DataYear, Semester)  # Check the year/semester value
    EventCheck.route_domain(KodeBalai, BalaiRoutes)  # Check the input route domain
    EventCheck.value_range_check(LowerBound, UpperBound, IRIColumn)  # Check the IRI value range
    EventCheck.segment_len_check(LrsNetworkRID, routes=EventCheck.valid_route)  # Check the segment length validity
    EventCheck.measurement_check(routes=EventCheck.valid_route)  # Check the from-to measurement
    EventCheck.coordinate_check(LrsNetworkRID,  # Check the segment starting coordinate
                                routes=EventCheck.valid_route, threshold=SearchRadius, at_start=False)
    EventCheck.lane_code_check(RNIEventTable, routes=EventCheck.valid_route,
                               rni_route_col=RNIRouteID)  # Check the event layer lane code combination

    ErrorMessageList = EventCheck.error_list  # Get all the error list from the TableCheck object

    if len(ErrorMessageList) != 0:  # if there is an  error in any validation process after header and dType check
        msg_count = 1
        for error_message in ErrorMessageList:
            AddMessage(str(msg_count)+'. '+error_message)
            msg_count += 1
        SetParameterAsText(2, reject_message(ErrorMessageList))
    else:  # If there is no error
        SetParameterAsText(2, "Finish")  # Should return a success JSON String

else:
    # There must be an error with the dType check
    SetParameterAsText(2, EventCheck.header_check_result)
