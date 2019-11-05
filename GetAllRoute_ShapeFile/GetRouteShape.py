"""
This script get all route shape and create a new Shapefile, from the specified route/s contained in the JSON output
from GetAllRoute script.
"""

from arcpy import env, GetParameterAsText, SetParameter, SetParameterAsText, Exists
import sys
from datetime import datetime
import os
sys.path.append('E:/SMD_Script')
from SMD_Package import GetRoutes, input_json_check, output_message, verify_balai, download, Configs
os.chdir('E:/SMD_Script')


class SDE_TableConnection(object):
    """
    Object for checking the requested table existence in a SDE connection
    Could be also used to check the SDE connection itself
    """
    def __init__(self, sde_connection, tables):
        """
        If all_connected is false then there is a table which does not exist in the SDE connection
        Further details about every table connection can be accessed in table_status dictionary
        :param sde_connection:
        :param tables:
        """
        env.workspace = sde_connection
        missing_table = []
        all_connected = True

        if type(tables) == list:
            for table in tables:
                if Exists(table):
                    pass
                else:
                    missing_table.append(table)
                    all_connected = False

        self.all_connected = all_connected
        self.missing_table = missing_table


# Get the script parameter
inputJSON = GetParameterAsText(0)

# Load the input JSON, result from GetAllRoute and config JSON
input_details = input_json_check(inputJSON, 1, req_keys=['type', 'codes'])

config = Configs()

# The LRS Network Table Details
lrsNetwork = config.table_names['lrs_network']
lrsNetwork_RouteID = config.table_fields['lrs_network']['route_id']
lrsNetwork_RouteName = config.table_fields['lrs_network']['route_name']

# The Balai Table
balaiTable = config.table_names['balai_table']
balaiProvCol = config.table_fields['balai_table']['prov_code']
balaiBalaiCol = config.table_fields['balai_table']['balai_code']

# The Balai Route Table
balaiRouteTable = config.table_names['balai_route_table']

# The RNI Table Details
rniTable = config.table_names['rni']

# The SDE Database Connection
dbConnection = config.smd_database['instance']

# Set the environment workspace
env.workspace = dbConnection
env.overwriteOutput = True

# Check the input type
if input_details['type'] != 'routes':  # If the input type is not 'routes', then request the route

    # Check the input request type
    if input_details['type'] == 'no_prov':
        code_col = balaiProvCol
    elif input_details['type'] == 'balai':
        code_col = balaiBalaiCol
    else:  # If the input request type is nor 'balai' or 'no_prov'
        SetParameterAsText(1, output_message("Failed", "Request type {0} is invalid.".format(input_details['type'])))
        sys.exit(0)

    # Check the input code validity
    code_check = verify_balai(input_details['codes'], balaiTable, code_col, dbConnection)
    if len(code_check) != 0:  # If there is an invalid code
        message = "Kode {0} {1} tidak valid.".format(input_details['type'], code_check)
        SetParameterAsText(1, output_message("Failed", message))
        sys.exit(0)  # Stop the script

    getAllRouteResult = GetRoutes(input_details['type'], input_details["codes"], lrsNetwork, balaiTable, balaiRouteTable)
    routeList = getAllRouteResult.route_list()  # The list containing the query result

elif input_details['type'] == 'routes':  # If the input type is 'routes' then use the value from 'codes'
    if type(input_details['codes']) != list:  # If the input is not a list type
        routeList = list(input_details['codes'])
    elif type(input_details['codes']) == list:  # If the input is already a list type
        routeList = input_details['codes']

    # Check the input code validity
    code_check = verify_balai(routeList, lrsNetwork, lrsNetwork_RouteID, dbConnection)
    if len(code_check) != 0:  # If there is an invalid code
        message = "Kode {0} {1} tidak valid.".format(input_details['type'], code_check)
        SetParameterAsText(1, output_message("Failed", message))
        sys.exit(0)  # Stop the script

# Checking the existence of all required table
ConnectionCheck = SDE_TableConnection(env.workspace, [rniTable, lrsNetwork])
if ConnectionCheck.all_connected:

    # Create the shapefile from the segment created by the dissolve segment function
    RouteGeometries = download.LRSShapeFile(routeList)

    if input_details["type"] == "balai":
        req_type = 'Balai'
    elif input_details["type"] == "no_prov":
        req_type = "Prov"
    elif input_details["type"] == "routes":
        req_type = "Rute"

    if type(input_details["codes"]) == list:
        req_codes = str(input_details["codes"]).strip("[]").replace("'", "").replace(', ','_').replace('u', '')
    else:
        req_codes = str(input_details["codes"])

    current_year = datetime.now().year
    RouteGeometries.centerline_shp("SegmenRuas_"+str(current_year))  # Create the polyline shapefile
    RouteGeometries.lrs_endpoint_shp("AwalAkhirRuas_"+str(current_year))  # Create the point shapefile
    download.rni_to_csv(routeList, 'RNITable.csv')
    zip_file = RouteGeometries.create_zipfile("Data_{0}_{1}_{2}".format(req_type, req_codes, current_year),
                                              added_file=['RNITable.csv']).zip_output

    SetParameterAsText(1, RouteGeometries.output_message())
    SetParameter(2, zip_file)

else:
    SetParameterAsText(1, output_message("Failed", "Required table are missing.{0}".format(ConnectionCheck.missing_table)))
