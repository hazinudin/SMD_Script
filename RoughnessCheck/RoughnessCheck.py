import os
import sys
import json
from arcpy import GetParameterAsText, SetParameterAsText, AddMessage, env
sys.path.append('E:\SMD_Script')  # Import the SMD_Package package
from SMD_Package import EventValidation, output_message, GetRoutes, gdb_table_writer, input_json_check, read_input_excel, verify_balai, convert_and_trim, create_patch
from SMD_Package.event_table.measurement.adjustment import Adjust

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
RNISurfaceType = smd_config['table_fields']['rni']['surface_type']

BalaiTable = smd_config['table_names']['balai_table']
BalaiKodeBalai = smd_config['table_fields']['balai_table']['balai_code']
dbConnection = smd_config['smd_database']['instance']
BalaiRouteTable = smd_config['table_names']['balai_route_table']

# Get GeoProcessing input parameter
inputJSON = GetParameterAsText(0)
forceWrite = GetParameterAsText(1)

# Load the input JSON
InputDetails = input_json_check(inputJSON, 2, req_keys=['file_name', 'balai', 'year', 'semester', 'routes'])
TablePath = InputDetails["file_name"]
DataYear = InputDetails["year"]
Semester = InputDetails['semester']
KodeBalai = InputDetails["balai"]
RouteReq = InputDetails["routes"]

# All the column details in the roughness_config.json
ColumnDetails = roughness_config['column_details']  # Load the roughness column details dictionary
SearchRadius = roughness_config['search_radius']
IRIColumn = "IRI"
RouteIDCol = 'LINKID'
FromMCol = "STA_FROM"
ToMCol = "STA_TO"
CodeLane = "LANE_CODE"
LongitudeCol = 'STATO_LONG'
LatitudeCol = 'STATO_LAT'
AltitudeCol = 'STATO_ALT'
SegmentLength = 'SEGMENT_LENGTH'

# The GDB table which store all the valid table row
OutputGDBTable = 'SMD.ROUGHNESS_{0}_{1}_RERUN_2'.format(Semester, DataYear)

# All the details about comparison table
CompTable = roughness_config['compare_table']['table_name']
CompRouteID = roughness_config['compare_table']['route_id']
CompFromMeasure = roughness_config['compare_table']['from_measure']
CompToMeasure = roughness_config['compare_table']['to_measure']
CompLaneCode = roughness_config['compare_table']['lane_code']
CompIRI = roughness_config['compare_table']['iri']

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
year_sem_check_result = EventCheck.year_and_semester_check(DataYear, Semester)

# If the header check, data type check and year semester check returns None, the process can continue
if (header_check_result is None) & (dtype_check_result is None) & (year_sem_check_result is None):

    EventCheck.route_domain(KodeBalai, routeList)  # Check the input route domain
    EventCheck.route_selection(selection=RouteReq)
    valid_routes = EventCheck.valid_route

    EventCheck.range_domain_check()
    EventCheck.segment_duplicate_check()
    EventCheck.lane_direction_check(routes=valid_routes)
    EventCheck.segment_len_check(routes=valid_routes)
    EventCheck.measurement_check(routes=valid_routes, tolerance=0)
    if str(forceWrite) != 'true':
        EventCheck.coordinate_check(routes=valid_routes, threshold=SearchRadius, at_start=False, comparison='RNIline-LRS')
    EventCheck.lane_code_check(routes=valid_routes)

    valid_df = EventCheck.copy_valid_df()  # Create the valid DataFrame copy
    passed_routes = EventCheck.passed_routes

    if len(passed_routes) != 0:  # If there is an route with no error, then write to GDB
        passed_routes_row = valid_df.loc[valid_df[RouteIDCol].isin(passed_routes)]  # Only select the route which pass
        passed_routes_row = create_patch(passed_routes_row, LrsNetwork, LrsNetworkRID)
        adjust = Adjust(passed_routes_row, RouteIDCol, FromMCol, ToMCol, CodeLane)
        # if str(forceWrite) == 'true':
        #     adjust.survey_direction()
        adjust.trim_to_reference(fit_to='RNI')  # Trim and convert to reference
        gdb_table_writer(dbConnection, adjust.df, OutputGDBTable, ColumnDetails, new_table=False)

        # Write the JSON Output string for all error.
        errors = EventCheck.altered_route_result(include_valid_routes=True, message_type='error')
        reviews = EventCheck.altered_route_result(include_valid_routes=False, message_type='ToBeReviewed')
        all_msg = errors + reviews
        SetParameterAsText(2, output_message("Succeeded", all_msg))
    else:
        # Write the JSON Output string.
        SetParameterAsText(2, output_message("Succeeded", EventCheck.altered_route_result(include_valid_routes=True)))

    # FOR ARCMAP USAGE ONLY #
    msg_count = 1
    for error_message in EventCheck.altered_route_result(message_type='error', dict_output=False):
        AddMessage(str(msg_count)+'. '+error_message+' ERROR')
        msg_count += 1

    for error_message in EventCheck.altered_route_result(message_type='ToBeReviewed', dict_output=False):
        AddMessage(str(msg_count)+'. '+error_message+' WARNING')
        msg_count += 1

elif dtype_check_result is None:
    # There must be an error with semester and year check
    SetParameterAsText(2, output_message("Rejected", year_sem_check_result))
else:
    # There must be an error with dtype check or header check
    SetParameterAsText(2, output_message("Rejected", dtype_check_result))