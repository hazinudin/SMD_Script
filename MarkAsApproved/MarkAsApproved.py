"""
This script will update the value of 'APPROVED' column for a selected route specified in the input parameter.
"""
import sys
import os
import json
from arcpy import GetParameterAsText, SetParameterAsText, Exists, env, ListFields, management, da
from pandas import DataFrame, Series
sys.path.append('E:\SMD_Script')
from SMD_Package import input_json_check, output_message

os.chdir('E:\SMD_Script')  # Change the directory to the SMD root directory

# Load the SMD config JSON file
with open('smd_config.json') as smd_config_f:
    smd_config = json.load(smd_config_f)


def strlist_to_querylist(input_list):
    """
    This function replace the list square bracket with regular bracket for arcpy query usage.
    :param input_list:
    :return:
    """
    str_list = str(input_list)
    replaced = str_list.replace('[', '(').replace(']', ')')

    return replaced


inputJSON = GetParameterAsText(0)

# The Input Details
InputDetails = input_json_check(inputJSON, 1, req_keys=['routes', 'data', 'semester', 'year'])
RouteReq = InputDetails['routes']
DataType = InputDetails['data']
DataSemester = InputDetails['semester']
DataYear = InputDetails['year']
ApprovedField = "APPROVED"
RouteIDColumn = "LINKID"

# Assemble the table name from Data Type, Data Semester, and Data Year variable.
if DataSemester is None:
    TableName = "SMD.{0}_{1}".format(DataType, DataYear)
else:
    TableName = "SMD.{0}_{1}_{2}".format(DataType, DataYear, DataSemester)

# SMD Config
dbConnection = smd_config['smd_database']['instance']
env.workspace = dbConnection  # Setting up the environment for ArcPy

# Check for RouteReq type
if type(RouteReq) == str or type(RouteReq) == unicode:
    RouteReq = [str(RouteReq)]
if type(RouteReq) == list:
    RouteReq = [str(x) for x in RouteReq]

# Check if the Table Exist
TableExists = Exists(TableName)

if TableExists:
    table_fields = [x.name for x in ListFields(TableName)]  # Get all the fields from the table
    list_query = strlist_to_querylist(RouteReq)
    existed_routes = list()  # List for storing processed routes
    da_clause = "{0} IN {1}".format(RouteIDColumn, list_query)

    if ApprovedField not in table_fields:  # If the field does not exist in the table
        management.AddField(TableName, ApprovedField, 'INTEGER')

    with da.UpdateCursor(TableName, [RouteIDColumn, ApprovedField], where_clause=da_clause) as cursor:
        for row in cursor:
            row[1] = 1  # Update the approved value row
            route = row[0]
            cursor.updateRow(row)

            if route not in existed_routes:
                existed_routes.append(route)  # Append the route

    # Check for a route which was not processed
    result_df = DataFrame(RouteReq, columns=['linkid'])
    result_df['status'] = Series('Route does not exist', index=result_df.index)
    result_df.loc[result_df['linkid'].isin(existed_routes), ['status']] = 'Route exist'

    result_msg = result_df.to_dict('records')
    SetParameterAsText(1, output_message('Succeeded', result_msg))

else:  # If the requested table does not exist
    error_message = "Tabel {0} tidak dapat ditemukan.".format(TableName)
    SetParameterAsText(1, output_message('Failed', error_message))
    sys.exit(0)
