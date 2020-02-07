import sys
sys.path.append('E:/SMD_Script')
from SMD_Package import Kemantapan, gdb_table_writer, event_fc_to_df
from SMD_Package.event_table.traffic.aadt import AADT
from arcpy import GetParameterAsText, env
import json
import os
os.chdir('E:/SMD_Script')

request = GetParameterAsText(0)
request_j = json.loads(request)
with open('smd_config.json') as config_f:
    smd_config = json.load(config_f)

# Get all the request detail
routeSelection = request_j['routes']
if routeSelection == "ALL":
    pass
elif type(routeSelection) == unicode:
    routeSelection = [routeSelection]
elif type(routeSelection) == list:
    pass
else:
    raise ("Route selection is not list or string")

DataYear = request_j['year']
dbConnection = smd_config['smd_database']['instance']

# All the input table column and table name
LaneCode = 'LANE_CODE'
RouteID = 'LINKID'
FromMeasure = 'STA_FROM'
ToMeasure = 'STA_TO'
DataTable = "SMD.RNI_{0}".format(DataYear)
columnDetails = dict()

env.workspace = dbConnection
for route in routeSelection:
    InputDF = event_fc_to_df(DataTable, "*", route, RouteID, dbConnection, is_table=True)

    # Initialize the kemantapan class
    aadt = AADT(InputDF)

    summaryTable = aadt.daily_aadt()
    print route

    # Create the column details
    for col_name in summaryTable.dtypes.to_dict():

        columnDetails[col_name] = dict()  # Create the dictionary for a single column
        col_dtype = summaryTable.dtypes[col_name]  # The Pandas Data Type.

        # Translate the Data Type
        if col_dtype == 'object':
            gdb_dtype = 'string'
        if col_dtype in ['int64', 'float64']:
            gdb_dtype = 'double'

        columnDetails[col_name]['dtype'] = gdb_dtype  # Insert the column data type

    # Write to GDB.
    gdb_table_writer(dbConnection, summaryTable, 'SMD.AADT_{0}'.format(DataYear),
                     columnDetails, new_table=False)
