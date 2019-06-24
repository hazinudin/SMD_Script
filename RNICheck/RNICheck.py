import os
import sys
import json
from arcpy import GetParameterAsText, SetParameterAsText, AddMessage, env
sys.path.append('E:\SMD_Script')  # Import the SMD_Package package
from SMD_Package import EventValidation, output_message, GetRoutes, gdb_table_writer, input_json_check, read_input_excel, verify_balai, convert_and_trim

os.chdir('E:\SMD_Script')  # Change the directory to the SMD root directory

# Load the roughness script config JSON file
with open('RNICheck/rni_config.json') as config_f:
    rni_config = json.load(config_f)

# Load the SMD config JSON file
with open('smd_config.json') as smd_config_f:
    smd_config = json.load(smd_config_f)

# The smd config JSON details
LrsNetwork = smd_config['table_names']['lrs_network']
LrsNetworkRID = smd_config['table_fields']['lrs_network']['route_id']
BalaiTable = smd_config['table_names']['balai_table']
BalaiKodeBalai = smd_config['table_fields']['balai_table']['balai_code']
dbConnection = smd_config['smd_database']['instance']
RNIEventTable = smd_config['table_names']['rni']
RNIRouteID = smd_config['table_fields']['rni']['route_id']
RNIFromMeasure = smd_config['table_fields']['rni']['from_measure']
RNIToMeasure = smd_config['table_fields']['rni']['to_measure']
RNILaneCode = smd_config['table_fields']['rni']['lane_code']
RNISurfaceType = smd_config['table_fields']['rni']['surface_type']
RNILaneWidth = smd_config['table_fields']['rni']['lane_width']

# Get GeoProcessing input parameter
inputJSON = GetParameterAsText(0)

# Load the input JSON
InputDetails = input_json_check(inputJSON, 1, req_keys=['file_name', 'balai', 'year', 'semester'])
TablePath = InputDetails["file_name"]
DataYear = InputDetails["year"]
DataSemester = InputDetails['semester']
KodeBalai = InputDetails["balai"]

# All the column details in the roughness_config.json
ColumnDetails = rni_config['column_details']  # Load the roughness column details dictionary
SearchRadius = rni_config['search_radius']
OutputGDBTable = rni_config['output_table']  # The GDB table which store all the valid table row
RoadTypeDetails = rni_config['roadtype_details']
ComparisonTable = rni_config['compare_table']['table_name']
CompRouteID = rni_config['compare_table']['route_id']
CompFromM = rni_config['compare_table']['from_measure']
CompToM = rni_config['compare_table']['to_measure']
CompSurfaceType = rni_config['compare_table']['surface_type']

# The input Table Column
RouteIDCol = 'LINKID'
FromMCol = "STA_FR"
ToMCol = "STA_TO"
CodeLane = "CODE_LANE"

# Set the environment workspace
env.workspace = dbConnection

# Check if balai from the request is valid
code_check_result = verify_balai(KodeBalai, BalaiTable, BalaiKodeBalai, env.workspace, return_false=True)
if len(code_check_result) != 0:  # If there is an error
    message = "Kode {0} {1} tidak valid.".format("balai", code_check_result)
    SetParameterAsText(1, output_message("Failed", message))
    sys.exit(0)

# GetAllRoute result containing all route from a Balai
routeList = GetRoutes("balai", KodeBalai, LrsNetwork, BalaiTable).route_list()

# Create a EventTableCheck class object
# The __init__ already include header check
try:
    InputDF = read_input_excel(TablePath)  # Read the excel file
except IOError:  # If the file path is invalid
    SetParameterAsText(1, output_message("Failed", "Invalid file directory"))  # Throw an error message
    sys.exit(0)  # Stop the script
if InputDF is None:  # If the file format is not .xlsx
    SetParameterAsText(1, output_message("Failed", "File is not in .xlsx format"))
    sys.exit(0)  # Stop the script


EventCheck = EventValidation(InputDF, ColumnDetails, LrsNetwork, LrsNetworkRID, dbConnection)
header_check_result = EventCheck.header_check_result
dtype_check_result = EventCheck.dtype_check_result
year_sem_check_result = EventCheck.year_and_semester_check(DataYear, DataSemester, year_check_only=True, lane_code='LANE_CODE')

# If the header check, data type check and year semester check returns None, the process can continue
if (header_check_result is None) & (dtype_check_result is None) & (year_sem_check_result is None):

    EventCheck.route_domain(KodeBalai, routeList)  # Check the input route domain
    valid_routes = EventCheck.valid_route

    EventCheck.range_domain_check(lane_code='LANE_CODE')
    EventCheck.segment_len_check(routes=valid_routes, lane_code='LANE_CODE')  # Check the segment length validity
    EventCheck.measurement_check(RNIEventTable, RNIRouteID, RNIToMeasure, routes=valid_routes,
                                 lane_code='LANE_CODE', compare_to='LRS')  # Check the from-to measurement
    EventCheck.coordinate_check(routes=valid_routes, threshold=SearchRadius, at_start=False, lane_code='LANE_CODE')
    EventCheck.rni_roadtype_check(RoadTypeDetails, routes=valid_routes)

    failed_routes = EventCheck.route_results.keys()  # Only contain the Error Message with Error status, without Review

    if len(failed_routes) == 0:  # If there is an route with no error, then write to GDB
        EventCheck.rni_compare_surftype_len(RNIEventTable, RNIRouteID, RNIFromMeasure, RNIToMeasure, RNISurfaceType,
                                            2018, RNILaneCode, routes=valid_routes)
        EventCheck.rni_compare_surfwidth(RNIEventTable, RNIRouteID, RNIFromMeasure, RNIToMeasure, RNILaneWidth, 2018,
                                         routes=valid_routes)

    failed_routes = EventCheck.route_results.keys()  # Only contain the Error Message with Error status, without Review
    valid_df = EventCheck.copy_valid_df()
    passed_routes_row = valid_df.loc[~valid_df[RouteIDCol].isin(failed_routes)]

    if len(passed_routes_row) != 0:
        convert_and_trim(passed_routes_row, RouteIDCol, FromMCol, ToMCol, CodeLane, LrsNetwork, LrsNetworkRID,
                         dbConnection)
        gdb_table_writer(dbConnection, passed_routes_row, OutputGDBTable, ColumnDetails, new_table=False)

    # Write the JSON Output string.
    SetParameterAsText(1, output_message("Checked", EventCheck.altered_route_result()))

    # FOR ARCMAP USAGE ONLY#
    msg_count = 1
    for error_message in EventCheck.altered_route_result(message_type='error', dict_output=False):
        AddMessage(str(msg_count)+'. '+error_message)
        msg_count += 1
    for error_message in EventCheck.altered_route_result(message_type='ToBeReviewed', dict_output=False):
        AddMessage(str(msg_count)+'. '+error_message)
        msg_count += 1

elif dtype_check_result is None:
    # There must be an error with semester and year check
    SetParameterAsText(1, output_message("Rejected", year_sem_check_result))

elif year_sem_check_result is None:
    # There must be an error with dtype check or header check
    SetParameterAsText(1, output_message("Rejected", dtype_check_result))

else:
    # There is an error with dtype check and year sem check
    dtype_check_result.append(year_sem_check_result)
    SetParameterAsText(1, output_message("Rejected", dtype_check_result))
