import sys
sys.path.append('E:/SMD_Script')
from SMD_Package import gdb_table_writer, read_input_excel, input_json_check, output_message, convert_and_trim
from SMD_Package.event_table.measurement.adjustment import Adjust
from arcpy import GetParameterAsText, env, SetParameterAsText, AddMessage
import numpy as np
import json
import os

os.chdir('E:/SMD_Script')


def load_config_data(config_file_path):
    """
    This function will load the specified JSON configuration file.
    :param config_file_path: The JSON configuration file path.
    :return: Dictionary object
    """

    with open(config_file_path) as config_f:
        config_dict = json.load(config_f)

    return config_dict


# Load the SMD config JSON file
smd_config = load_config_data('smd_config.json')

# The smd config JSON details
LrsNetwork = smd_config['table_names']['lrs_network']
LrsNetworkRID = smd_config['table_fields']['lrs_network']['route_id']
dbConnection = smd_config['smd_database']['instance']

# Get GeoProcessing input parameter
inputJSON = GetParameterAsText(0)

# Load the input JSON
InputDetails = input_json_check(inputJSON, 1, req_keys=['file_name', 'routes', 'data', 'year'])
TablePath = InputDetails["file_name"]
Routes = InputDetails["routes"]
DataType = InputDetails["data"]
DataYear = InputDetails["year"]
DataSemester = InputDetails.get("semester")

# The input Event Table Columns
inputRouteID = 'LINKID'
inputFromM = 'STA_FROM'
inputToM = 'STA_TO'
inputLaneCode = 'LANE_CODE'

# Load the excel file as an DataFrame
try:
    InputDF = read_input_excel(TablePath)  # Read the excel file
except IOError:  # If the file path is invalid
    SetParameterAsText(1, output_message("Failed", "Invalid file directory or invalid file name"))  # Throw an error message
    sys.exit(0)  # Stop the script
if InputDF is None:  # If the file format is not .xlsx
    SetParameterAsText(1, output_message("Failed", "File is not in .xlsx format"))
    sys.exit(0)  # Stop the script

# Check if the specified route is in the input table
if type(Routes) is not list:  # Check if the inputted routes is a list
    message = "Inputted Routes should be in array"
    SetParameterAsText(1, message)
    sys.exit(0)
if type(Routes) is list:  # If the inputted routes is a list
    df_routes = InputDF[inputRouteID]  # Available routes in the input table
    mask = np.in1d(Routes, df_routes)  # Masking for input route array
    invalid_route = np.array(Routes)[~mask]  # All the route which does not exist in input table
    if len(invalid_route) != 0:  # There is an invalid route
        message = "Rute {0} tidak terdapat pada table excel".format(invalid_route)
        SetParameterAsText(1, output_message("Failed", message))
        sys.exit(0)  # Stop the script

# Determine the output table based on the specified data type
adjust = Adjust(InputDF, inputRouteID, inputFromM, inputToM, inputLaneCode)
if str(DataType) == "IRI":  # If the data is IRI/Roughness
    data_config = load_config_data('RoughnessCheck/roughness_config_2020.json')
    OutputGDBTable = "SMD.RNI_TEST_"+str(DataYear)
    ColumnDetails = data_config['column_details']
    adjust.trim_to_reference(fit_to='RNI')

elif str(DataType) == "RNI":  # If the data is RNI
    data_config = load_config_data('RNICheck/rni_config_2020.json')
    OutputGDBTable = "SMD.ROUGHNESS_TEST_"+str(DataSemester)+"_"+str(DataYear)
    ColumnDetails = data_config['column_details']

else:  # If other than that, the process will be terminated with an error message.
    message = 'Data type {0} is not supported'.format(DataType)
    SetParameterAsText(1, output_message("Failed", message))
    sys.exit(0)

# Start writing the input table to GDB
gdb_table_writer(dbConnection, adjust.df, OutputGDBTable, ColumnDetails)
SetParameterAsText(1, output_message("Succeeded", ""))
