import os
import sys
import json
from arcpy import GetParameterAsText, SetParameterAsText, AddMessage, env
sys.path.append('E:\SMD_Script')  # Import the SMD_Package package
from SMD_Package import EventValidation, output_message, GetRoutes, gdb_table_writer, input_json_check, verify_balai, \
    read_input_excel, convert_and_trim

os.chdir('E:\SMD_Script')  # Change the directory to the SMD root directory

# Load the roughness script config JSON file
with open('RTCCheck/rtc_config.json') as config_f:
    rtc_config = json.load(config_f)

# Load the SMD config JSON file
with open('smd_config.json') as smd_config_f:
    smd_config = json.load(smd_config_f)

# The smd config JSON details
LrsNetwork = smd_config['table_names']['lrs_network']
LrsNetworkRID = smd_config['table_fields']['lrs_network']['route_id']

BalaiTable = smd_config['table_names']['balai_table']
BalaiRouteTable = smd_config['table_names']['balai_route_table']
BalaiKodeBalai = smd_config['table_fields']['balai_table']['balai_code']
dbConnection = smd_config['smd_database']['instance']

# Get GeoProcessing input parameter
inputJSON = GetParameterAsText(0)

# Load the input JSON
InputDetails = input_json_check(inputJSON, 1, req_keys=['file_name', 'balai', 'year', 'routes'])
TablePath = InputDetails["file_name"]
DataYear = InputDetails["year"]
KodeBalai = InputDetails["balai"]
RouteReq = InputDetails["routes"]

# All the column details in the roughness_config.json
ColumnDetails = rtc_config['column_details']  # Load the roughness column details dictionary
SearchRadius = rtc_config['search_radius']
OutputTable = rtc_config['output_table']
RouteIDCol = 'LINKID'

# GetAllRoute result containing all route from a Balai
env.workspace = dbConnection
routeList = GetRoutes("balai", KodeBalai, LrsNetwork, BalaiTable, BalaiRouteTable).route_list()

# Check if balai from the request is valid
code_check_result = verify_balai(KodeBalai, BalaiTable, BalaiKodeBalai, env.workspace, return_false=True)
if len(code_check_result) != 0:  # If there is an error
    message = "Kode {0} {1} tidak valid.".format("balai", code_check_result)
    SetParameterAsText(1, output_message("Failed", message))
    sys.exit(0)

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

# If the header check, data type check and year semester check returns None, the process can continue
if (header_check_result is None) & (dtype_check_result is None):

    EventCheck.route_domain(KodeBalai, routeList)  # Check the input route domain
    EventCheck.route_selection(selection=RouteReq)
    valid_routes = EventCheck.valid_route

    EventCheck.range_domain_check()
    EventCheck.coordinate_check(routes=valid_routes, segm_dist=False, lat_col='RTC_LAT', long_col='RTC_LONG',
                                monotonic_check=False)
    EventCheck.rtc_duration_check(routes=valid_routes)  # The RTC duration check.
    EventCheck.rtc_time_interval_check(routes=valid_routes)  # The RTC survey time interval check.

    valid_df = EventCheck.copy_valid_df()
    passed_routes = EventCheck.passed_routes

    SetParameterAsText(1, output_message("Checked", EventCheck.altered_route_result()))

    if len(passed_routes) != 0:  # If there is an route with no error, then write to GDB
        passed_routes_row = valid_df.loc[valid_df[RouteIDCol].isin(passed_routes)]
        gdb_table_writer(dbConnection, passed_routes_row, OutputTable, ColumnDetails)

    # FOR ARCMAP USAGE ONLY #
    msg_count = 1
    for error_message in EventCheck.altered_route_result(message_type='error', dict_output=False):
        AddMessage(str(msg_count) + '. ' + error_message + ' ERROR')
        msg_count += 1

    for error_message in EventCheck.altered_route_result(message_type='ToBeReviewed', dict_output=False):
        AddMessage(str(msg_count) + '. ' + error_message + ' WARNING')
        msg_count += 1

else:
    # There must be an error with dtype check or header check
    SetParameterAsText(1, output_message("Rejected", dtype_check_result))