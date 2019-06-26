"""
This script get all route from the specified code(Kode Balai or Kode Provinsi) in the input JSON and return the query
result in a form of string JSON.
"""

from arcpy import da, GetParameterAsText, SetParameterAsText, Exists, env, AddMessage
import json
import os
import sys
sys.path.append('E:\SMD_Script')
from SMD_Package import GetRoutes, output_message, input_json_check, verify_balai


# Change the directory to the SMD Script root folder
os.chdir('E:\SMD_Script')

# Get the parameter for script
inputJSON = GetParameterAsText(0)

# Load the SMD config JSON file
with open('smd_config.json') as config_f:
    config = json.load(config_f)

# Load the input JSON string as dictionary
# input JSON loaded as input_details
input_details = input_json_check(inputJSON, 1, escape_str=True, req_keys=['type', 'codes'])

# Define variable
queryType = input_details['type']
queryValue = input_details['codes']
lrsNetwork = config['table_names']['lrs_network']
balaiTable = config['table_names']['balai_table']
balaiRouteTable = config['table_names']['balai_route_table']

lrs_RID = 'ROUTEID'
lrs_provcode = 'NOPROP'
balaiTableBalaiCode = 'NOMOR_BALAI'
balaiTableProvCode = 'NO_PROV'

env.workspace = config['smd_database']['instance']

# Check for input value validity
if queryType in ['balai', 'no_prov']:
    pass
else:
    message = "{0} is not a valid input".format(queryType)
    SetParameterAsText(1, output_message("Failed", message))
    sys.exit(0)

# Check the SDE geodatabase connection and feature class availability
for table in [balaiTable, lrsNetwork]:
    try:
        if Exists(table):
            pass
    except:
        message = "{0} does not exist".format(table)
        SetParameterAsText(1, output_message("Failed", message))
        sys.exit(0)

# Check if all request code are valid
if queryType == 'no_prov':
    code_field = balaiTableProvCode
if queryType == 'balai':
    code_field = balaiTableBalaiCode

code_check_result = verify_balai(queryValue, balaiTable, code_field, env.workspace, return_false=True)
if len(code_check_result) != 0:  # If there is an error
    message = "Kode {0} {1} tidak valid.".format(queryType, code_check_result)
    SetParameterAsText(1, output_message("Failed", message))
    sys.exit(0)

# Creating the route query request object
route_query = GetRoutes(queryType, queryValue, lrsNetwork, balaiTable, balaiRouteTable, lrs_routeid=lrs_RID,
                        lrs_prov_code=lrs_provcode, balai_code=balaiTableBalaiCode, balai_prov=balaiTableProvCode)
SetParameterAsText(1, route_query.create_json_output(detailed=True))
