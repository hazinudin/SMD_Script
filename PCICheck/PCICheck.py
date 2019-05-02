import os
import sys
import json
from arcpy import GetParameterAsText, SetParameterAsText, AddMessage, env
sys.path.append('E:\SMD_Script')  # Import the SMD_Package package
from SMD_Package import EventValidation, output_message, GetRoutes, gdb_table_writer, input_json_check

os.chdir('E:\SMD_Script')  # Change the directory to the SMD root directory

# Load the roughness script config JSON file
with open('PCICheck/pci_config.json') as config_f:
    pci_config = json.load(config_f)

# Load the SMD config JSON file
with open('smd_config.json') as smd_config_f:
    smd_config = json.load(smd_config_f)

# The smd config JSON details
LrsNetwork = smd_config['table_names']['lrs_network']
LrsNetworkRID = smd_config['table_fields']['lrs_network']['route_id']

BalaiTable = smd_config['table_names']['balai_table']
dbConnection = smd_config['smd_database']['instance']

# Get GeoProcessing input parameter
inputJSON = GetParameterAsText(0)

# Load the input JSON
InputDetails = input_json_check(inputJSON, 1, req_keys=['file_name', 'balai', 'year', 'semester'])
TablePath = InputDetails["file_name"]
DataYear = InputDetails["year"]
DataSemester = InputDetails['semester']
KodeBalai = InputDetails["balai"]

# All the column details in the roughness_config.json
ColumnDetails = pci_config['column_details']  # Load the roughness column details dictionary
OutputTable = pci_config['output_table']
RouteIDCol = 'LINKID'

# GetAllRoute result containing all route from a Balai
env.workspace = dbConnection
routeList = GetRoutes("balai", KodeBalai, LrsNetwork, BalaiTable).route_list()

# Create a EventTableCheck class object
# The __init__ already include header check
EventCheck = EventValidation(TablePath, ColumnDetails, LrsNetwork, LrsNetworkRID, dbConnection)
header_check_result = EventCheck.header_check_result
dtype_check_result = EventCheck.dtype_check_result
year_sem_check_result = EventCheck.year_and_semester_check(DataYear, DataSemester, year_check_only=True, lane_code='LANE_CODE')

# If the header check, data type check and year semester check returns None, the process can continue
if (header_check_result is None) & (dtype_check_result is None) & (year_sem_check_result is None):

    EventCheck.route_domain(KodeBalai, routeList)  # Check the input route domain
    valid_routes = EventCheck.valid_route

    EventCheck.range_domain_check(lane_code='LANE_CODE')
    EventCheck.segment_len_check(routes=valid_routes, lane_code='LANE_CODE')  # Check the segment length validity
    EventCheck.measurement_check(routes=valid_routes, lane_code='LANE_CODE')  # Check the from-to measurement
    EventCheck.coordinate_check(routes=valid_routes, at_start=False, lane_code='LANE_CODE')  # Check the input coordinate
    ErrorMessageList = EventCheck.error_list  # Get all the error list from the TableCheck object

    failed_routes = EventCheck.route_results.keys()
    valid_df = EventCheck.copy_valid_df()
    passed_routes_row = valid_df.loc[~valid_df[RouteIDCol].isin(failed_routes)]

    if len(passed_routes_row) != 0:  # If there is an route with no error, then write to GDB
        gdb_table_writer(dbConnection, passed_routes_row, OutputTable, ColumnDetails)

    msg_count = 1
    for error_message in EventCheck.altered_route_result('error', dict_output=False):
        AddMessage(str(msg_count)+'. '+error_message)
        msg_count += 1

    SetParameterAsText(1, output_message("Checked", EventCheck.altered_route_result()))

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
