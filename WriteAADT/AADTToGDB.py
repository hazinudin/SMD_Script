import sys
sys.path.append('E:/SMD_Script')
from SMD_Package import gdb_table_writer, event_fc_to_df, GetRoutes
from SMD_Package.event_table.traffic.aadt import TrafficSummary
from arcpy import GetParameterAsText, env
import json
import os
os.chdir('E:/SMD_Script')

request = GetParameterAsText(0)
request_j = json.loads(request)
with open('smd_config.json') as config_f:
    smd_config = json.load(config_f)

# Setting up the GDB environment
dbConnection = smd_config['smd_database']['instance']
env.workspace = dbConnection

# Get all the request detail
routeSelection = request_j['routes']
if routeSelection == "ALL":
    getRoute = GetRoutes("balai", 'ALL')
    routeSelection = getRoute.route_list()
    pass
elif type(routeSelection) == unicode:
    routeSelection = [routeSelection]
elif type(routeSelection) == list:
    pass
else:
    raise ("Route selection is not list or string")

DataYear = request_j['year']

# All the input table column and table name
LaneCode = 'LANE_CODE'
RouteID = 'LINKID'
FromMeasure = 'STA_FROM'
ToMeasure = 'STA_TO'
DataTable = "SMD.RTC_{0}".format(DataYear)
columnDetails = dict()

for route in routeSelection:
    InputDF = event_fc_to_df(DataTable, "*", route, RouteID, dbConnection, is_table=True)

    if len(InputDF) == 0:  # Continue to next route if the InputDF is empty
        continue

    # Initialize the AADT class
    aadt = TrafficSummary(InputDF)

    summaryTable = aadt.daily_aadt()  # Create the daily AADT summary

    # Create the column details
    for col_name in summaryTable.dtypes.to_dict():

        columnDetails[col_name] = dict()  # Create the dictionary for a single column
        col_dtype = summaryTable.dtypes[col_name]  # The Pandas Data Type.

        # Translate the Data Type
        if col_dtype == 'object':
            gdb_dtype = 'string'
        if col_dtype in ['int64', 'float64', 'int32']:
            gdb_dtype = 'long'

        columnDetails[col_name]['dtype'] = gdb_dtype  # Insert the column data type

    # Write to GDB.
    gdb_table_writer(dbConnection, summaryTable, 'SMD.AADT_{0}'.format(DataYear),
                     columnDetails, new_table=False)
