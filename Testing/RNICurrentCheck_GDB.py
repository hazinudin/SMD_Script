import os
import sys
import json
from arcpy import GetParameterAsText, SetParameterAsText, AddMessage, env, SetParameter, GetParameter
from pandas import DataFrame, Series
sys.path.append('E:\SMD_Script')  # Import the SMD_Package package
from SMD_Package import EventValidation, output_message, GetRoutes, event_fc_to_df, gdb_table_writer, verify_balai
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
KodeBalai = GetParameterAsText(0)
RNITable = GetParameterAsText(1)
DataYear = GetParameter(2)
RouteReq = 'ALL'

# All the column details in the roughness_config.json
ColumnDetails = rni_config['column_details']  # Load the roughness column details dictionary
SearchRadius = rni_config['search_radius']
RoadTypeDetails = rni_config['roadtype_details']
ComparisonTable = rni_config['compare_table']['table_name']
CompRouteID = rni_config['compare_table']['route_id']
CompFromM = rni_config['compare_table']['from_measure']
CompToM = rni_config['compare_table']['to_measure']
CompSurfaceType = rni_config['compare_table']['surface_type']

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
routeList = GetRoutes("balai", KodeBalai, LrsNetwork, BalaiTable, BalaiRouteTable, lrs_lintas='ID_LINTAS').route_list()

# Create a EventTableCheck class object
# The __init__ already include header check
try:
    InputDF = event_fc_to_df(RNITable, ColumnDetails.keys(), routeList, RouteIDCol, dbConnection, True)  # Read the excel file
except IOError:  # If the file path is invalid
    SetParameterAsText(1, output_message("Failed", "Invalid file directory"))  # Throw an error message
    sys.exit(0)  # Stop the script
if InputDF is None:  # If the file format is not .xlsx
    SetParameterAsText(1, output_message("Failed", "File is not in .xlsx format"))
    sys.exit(0)  # Stop the script

InputDF.loc[:, ['ID_KONSULTAN', 'ID_SURVEYOR', 'ID_ALATSURVEY', 'STATO_ALT']] = 0
InputDF.loc[InputDF['SURF_YEAR'].isnull(), ['SURF_YEAR']] = 0
InputDF.loc[:, ['SURVEY_DATE']] = '23/03/2019'
InputDF.dropna(inplace=True)

EventCheck = EventValidation(InputDF, ColumnDetails, LrsNetwork, LrsNetworkRID, dbConnection)
header_check_result = EventCheck.header_check_result
dtype_check_result = EventCheck.dtype_check_result
year_sem_check_result = None

# If the header check, data type check and year semester check returns None, the process can continue
if (header_check_result is None) & (dtype_check_result is None) & (year_sem_check_result is None):

    EventCheck.route_domain('LRS', routeList)  # Check the input route domain
    EventCheck.route_selection(selection=RouteReq)
    EventCheck.segment_duplicate_check()
    valid_routes = EventCheck.valid_route

    EventCheck.range_domain_check()
    EventCheck.survey_year_check(DataYear)
    EventCheck.segment_len_check(routes=valid_routes)
    EventCheck.measurement_check(routes=valid_routes, compare_to='LRS', ignore_end_gap=False)
    EventCheck.coordinate_check(routes=valid_routes, threshold=SearchRadius, at_start=False)
    EventCheck.rni_roadtype_check(RoadTypeDetails, routes=valid_routes)

    valid_df = EventCheck.copy_valid_df()  # Create the valid DataFrame copy

    df_result = DataFrame(EventCheck.altered_route_result())
    if not df_result.empty:
        failed_routes = df_result['linkid'].unique().tolist()
        passed_row = valid_df.loc[~valid_df[RouteIDCol].isin(failed_routes)]
        df_verif = df_result.loc[df_result['status'] == 'verified']
        df_result.to_csv("{1}/error_check_{0}.csv".format(KodeBalai, env.scratchFolder))
        SetParameter(3, "{1}/error_check_{0}.csv".format(KodeBalai, env.scratchFolder))

        adjusted = Adjust(passed_row, RouteIDCol, FromMCol, ToMCol, CodeLane)
        adjusted.trim_to_reference(fit_to='LRS')
        gdb_table_writer(dbConnection, adjusted.df, 'SMD.RNI_2019_1', ColumnDetails, new_table=False)

    else:
        SetParameter(3, "No error")
        passed_row = valid_df
        adjusted = Adjust(passed_row, RouteIDCol, FromMCol, ToMCol, CodeLane)
        adjusted.trim_to_reference(fit_to='LRS')
        gdb_table_writer(dbConnection, adjusted.df, 'SMD.RNI_2019_1', ColumnDetails, new_table=False)

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
    SetParameterAsText(3, output_message("Rejected", year_sem_check_result))

elif year_sem_check_result is None:
    # There must be an error with dtype check or header check
    SetParameterAsText(3, output_message("Rejected", dtype_check_result))

else:
    # There is an error with dtype check and year sem check
    dtype_check_result.append(year_sem_check_result)
    SetParameterAsText(3, output_message("Rejected", dtype_check_result))