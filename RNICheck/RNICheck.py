import os
import sys
import json
from arcpy import GetParameterAsText, SetParameterAsText, AddMessage, env
sys.path.append('E:\SMD_Script')  # Import the SMD_Package package
from SMD_Package import EventValidation, output_message, GetRoutes, gdb_table_writer, input_json_check, read_input_excel, verify_balai, convert_and_trim
from SMD_Package.event_table.measurement.adjustment import Adjust

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
BalaiRouteTable = smd_config['table_names']['balai_route_table']
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
forceWrite = GetParameterAsText(1)

# Load the input JSON
InputDetails = input_json_check(inputJSON, 1, req_keys=['file_name', 'balai', 'year', 'routes'])
TablePath = InputDetails["file_name"]
DataYear = InputDetails["year"]
KodeBalai = InputDetails["balai"]
RouteReq = InputDetails["routes"]

# All the column details in the roughness_config.json
ColumnDetails = rni_config['column_details']  # Load the roughness column details dictionary
SearchRadius = rni_config['search_radius']
OutputGDBTable = 'SMD.RNI_{0}'.format(DataYear)  # The GDB table which store all the valid table row
RoadTypeDetails = rni_config['roadtype_details']
ComparisonTable = rni_config['compare_table']['table_name']
CompRouteID = rni_config['compare_table']['route_id']
CompFromM = rni_config['compare_table']['from_measure']
CompToM = rni_config['compare_table']['to_measure']
CompSurfaceType = rni_config['compare_table']['surface_type']
CompLaneCode = rni_config['compare_table']['lane_code']
CompLaneWidth = rni_config['compare_table']['lane_width']

# The input Table Column
RouteIDCol = 'LINKID'
FromMCol = "STA_FROM"
ToMCol = "STA_TO"
CodeLane = "LANE_CODE"

# Set the environment workspace
env.workspace = dbConnection

# Check if balai from the request is valid
code_check_result = verify_balai(KodeBalai, BalaiTable, BalaiKodeBalai, env.workspace, return_false=True)
if len(code_check_result) != 0:  # If there is an error
    message = "Kode {0} {1} tidak valid.".format("balai", code_check_result)
    SetParameterAsText(1, output_message("Failed", message))
    sys.exit(0)

# GetAllRoute result containing all route from a Balai
routeList = GetRoutes("balai", KodeBalai, LrsNetwork, BalaiTable, BalaiRouteTable).route_list()

# Create a EventTableCheck class object
# The __init__ already include header check
try:
    InputDF = read_input_excel(TablePath)  # Read the excel file
except IOError:  # If the file path is invalid
    SetParameterAsText(2, output_message("Failed", "Invalid file directory"))  # Throw an error message
    sys.exit(0)  # Stop the script
if InputDF is None:  # If the file format is not .xlsx
    SetParameterAsText(2, output_message("Failed", "File is not in .xlsx format"))
    sys.exit(0)  # Stop the script


EventCheck = EventValidation(InputDF, ColumnDetails, LrsNetwork, LrsNetworkRID, dbConnection)
header_check_result = EventCheck.header_check_result
dtype_check_result = EventCheck.dtype_check_result
year_sem_check_result = EventCheck.year_and_semester_check(DataYear, None, year_check_only=True)

# If the header check, data type check and year semester check returns None, the process can continue
if (header_check_result is None) & (dtype_check_result is None) & (year_sem_check_result is None):

    EventCheck.route_domain(KodeBalai, routeList)  # Check the input route domain
    EventCheck.route_selection(selection=RouteReq)
    EventCheck.segment_duplicate_check()
    valid_routes = EventCheck.valid_route

    EventCheck.range_domain_check(routes=valid_routes)
    EventCheck.survey_year_check(DataYear)
    EventCheck.segment_len_check(routes=valid_routes)
    EventCheck.measurement_check(routes=valid_routes, compare_to='LRS', ignore_end_gap=False)
    if str(forceWrite) != 'true':
        EventCheck.coordinate_check(routes=valid_routes, threshold=SearchRadius, at_start=False)
    EventCheck.rni_roadtype_check(RoadTypeDetails, routes=valid_routes)

    valid_df = EventCheck.copy_valid_df()  # Create the valid DataFrame copy
    passed_routes = EventCheck.no_error_route

    if len(passed_routes) != 0:  # Only process the route which passed the Error check.
        EventCheck.rni_compare_surftype_len(ComparisonTable, CompRouteID, CompFromM, CompToM, CompSurfaceType,
                                            2018, CompLaneCode, routes=passed_routes)
        EventCheck.rni_compare_surfwidth(ComparisonTable, CompRouteID, CompFromM, CompToM, CompLaneWidth, 2018,
                                         routes=passed_routes)

        passed_routes = EventCheck.passed_routes  # Refresh the all passed routes list

        if len(passed_routes) != 0:
            passed_routes_row = valid_df.loc[valid_df[RouteIDCol].isin(passed_routes)]
            adjust = Adjust(passed_routes_row, RouteIDCol, FromMCol, ToMCol, CodeLane)
            if str(forceWrite) == 'true':
                adjust.survey_direction()
            adjust.convert()  # Only convert the measurement value
            gdb_table_writer(dbConnection, adjust.df, OutputGDBTable, ColumnDetails, new_table=False)

        # Write the JSON Output string for all error.
        errors = EventCheck.altered_route_result(include_valid_routes=True, message_type='error')
        reviews = EventCheck.altered_route_result(include_valid_routes=False, message_type='ToBeReviewed')
        all_msg = errors + reviews
        SetParameterAsText(2, output_message("Succeeded", all_msg))
    else:
        # Write the JSON Output string.
        errors = EventCheck.altered_route_result(include_valid_routes=True, message_type='error')
        SetParameterAsText(2, output_message("Succeeded", errors))

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
    SetParameterAsText(2, output_message("Rejected", year_sem_check_result))

elif year_sem_check_result is None:
    # There must be an error with dtype check or header check
    SetParameterAsText(2, output_message("Rejected", dtype_check_result))

else:
    # There is an error with dtype check and year sem check
    dtype_check_result.append(year_sem_check_result)
    SetParameterAsText(2, output_message("Rejected", dtype_check_result))
