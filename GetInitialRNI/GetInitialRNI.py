"""
Ths script accept return RNI details from a specified route in requested by the user.
"""
from arcpy import GetParameterAsText
import json
import sys
import os
sys.path.append('E:\SMD_Script')
from SMD_Package import input_json_check, event_fc_to_df

os.chdir('E:\SMD_Script')

# Load the SMD config JSON file
with open('smd_config.json') as smd_config_f:
    smd_config = json.load(smd_config_f)

# Get the input JSON from user
inputJSON = GetParameterAsText(0)

# Load the input JSON
InputDetails = input_json_check(inputJSON, 1, req_keys=['routes'])
RouteReq = InputDetails["routes"]

# The SMD config JSON detail
RNITable = smd_config["table_names"]["rni"]
RNIRouteID = smd_config["table_fields"]["rni"]["route_id"]
RNILaneCode = smd_config["table_fields"]["rni"]["lane_code"]
dbConnection = smd_config["smd_database"]["instance"]

# Process the request
RNI_df = event_fc_to_df(RNITable, [RNIRouteID, RNILaneCode], RouteReq, RNIRouteID, dbConnection, is_table=True)