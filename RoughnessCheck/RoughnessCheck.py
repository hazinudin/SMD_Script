import os
import sys
import json
from arcpy import GetParameterAsText, SetParameterAsText, AddMessage, env
sys.path.append('E:\SMD_Script')  # Import the SMD_Package package
from SMD_Package import EventValidation, output_message, GetRoutes

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
dbConnection = smd_config['smd_database']['instance']

# Get GeoProcessing input parameter
input_JSON = GetParameterAsText(0)

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

# All the details about comparison table
CompTable = roughness_config['compare_table']['table_name']
CompRouteID = roughness_config['compare_table']['route_id']
CompFromMeasure = roughness_config['compare_table']['from_measure']
CompToMeasure = roughness_config['compare_table']['to_measure']
CompIRI = roughness_config['compare_table']['iri']

# GetAllRoute result containing all route from a Balai
env.workspace = dbConnection
routeList = GetRoutes("balai", KodeBalai, LrsNetwork, BalaiTable).route_list()

# Create a EventTableCheck class object
# The __init__ already include header check
EventCheck = EventValidation(TablePath, ColumnDetails, LrsNetwork, LrsNetworkRID, dbConnection)

# If the header check and data type check returns None, the process can continue
if EventCheck.header_check_result is None:

    EventCheck.year_and_semester_check(DataYear, Semester)  # Check the year/semester value
    EventCheck.route_domain(KodeBalai, routeList)  # Check the input route domain
    valid_routes = EventCheck.valid_route
    EventCheck.value_range_check(LowerBound, UpperBound, IRIColumn)  # Check the IRI value range
    EventCheck.segment_len_check(routes=valid_routes)  # Check the segment length validity
    EventCheck.measurement_check(routes=valid_routes)  # Check the from-to measurement
    EventCheck.coordinate_check(routes=valid_routes, threshold=SearchRadius, at_start=False)
    EventCheck.lane_code_check(RNIEventTable, routes=valid_routes,
                               rni_route_col=RNIRouteID)  # Check the event layer lane code combination
    EventCheck.compare_kemantapan(RNIEventTable, RNISurfaceType, IRIColumn, CompTable, CompFromMeasure, CompToMeasure,
                                  CompRouteID, CompIRI,routes=valid_routes)

    ErrorMessageList = EventCheck.error_list  # Get all the error list from the TableCheck object

    if len(ErrorMessageList) != 0:  # if there is an  error in any validation process after header and dType check
        msg_count = 1
        for error_message in ErrorMessageList:
            AddMessage(str(msg_count)+'. '+error_message)
            msg_count += 1
        SetParameterAsText(1, output_message("Rejected", ErrorMessageList))
    else:  # If there is no error
        SetParameterAsText(1, output_message("Success", "Tidak ditemui error."))  # Should return a success JSON String

else:
    # There must be an error with the dType check
    SetParameterAsText(1, output_message("Rejected", EventCheck.header_check_result))
