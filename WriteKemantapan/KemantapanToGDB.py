import sys
sys.path.append('E:/SMD_Script')
from SMD_Package import Kemantapan, gdb_table_writer, event_fc_to_df, GetRoutes
from SMD_Package.event_table.measurement.adjustment import Adjust
from arcpy import GetParameterAsText, env
import json
import os
os.chdir('E:/SMD_Script')

request = GetParameterAsText(0)
data = GetParameterAsText(1)
lane_based = GetParameterAsText(2)
request_j = json.loads(request)
with open('smd_config.json') as config_f:
    smd_config = json.load(config_f)

# Setting up the GDB environment
dbConnection = smd_config['smd_database']['instance']
env.workspace = dbConnection

# Get all the request detail
routeSelection = request_j['routes']
if routeSelection == "ALL":
    lrsNetwork = smd_config["table_names"]["lrs_network"]
    balaiTable = smd_config["table_names"]["balai_table"]
    balaiRouteTable = smd_config["table_names"]["balai_route_table"]
    getRoute = GetRoutes("balai", 'ALL', lrsNetwork, balaiTable, balaiRouteTable)
    routeSelection = getRoute.route_list()
    pass
elif type(routeSelection) == unicode:
    routeSelection = [routeSelection]
elif type(routeSelection) == list:
    pass
else:
    raise ("Route selection is not list or string")

if 'semester' in request_j.keys():
    DataSemester = request_j['semester']
else:
    DataSemester = None
DataYear = request_j['year']

# Determine the grading column
if data == 'ROUGHNESS':
    GradeColumn = 'IRI_POK'
elif data == 'PCI':
    GradeColumn = 'PCI'
else:
    raise('Invalid request {0}'.format(data))

# All the input table column and table name
LaneCode = 'LANE_CODE'
RouteID = 'LINKID'
FromMeasure = 'STA_FROM'
ToMeasure = 'STA_TO'
if DataSemester is None:
    DataTable = 'SMD.{0}_{1}'.format(data, DataYear)
else:
    DataTable = 'SMD.{0}_{1}_{2}_RERUN_2'.format(data, DataSemester, DataYear)

columnDetails = dict()

for route in routeSelection:
    InputDF = event_fc_to_df(DataTable, [RouteID, FromMeasure, ToMeasure, GradeColumn, LaneCode], route, RouteID,
                             dbConnection, is_table=True)
    # adjust = Adjust(InputDF, "LINKID", "STA_FROM", "STA_TO", "LANE_CODE", conversion=1)
    # adjust.trim_to_reference(fit_to='RNI')

    # Initialize the kemantapan class
    if lane_based == 'false':
        outputTable = 'SMD.KEMANTAPAN_{0}_{1}_{2}'.format(data, DataSemester, DataYear)
        kemantapan = Kemantapan(InputDF, GradeColumn, RouteID, FromMeasure, ToMeasure, LaneCode, data, to_km_factor=1)
    else:
        outputTable = 'SMD.KEMANTAPAN_LKM_{0}_{1}_{2}'.format(data, DataSemester, DataYear)
        kemantapan = Kemantapan(InputDF, GradeColumn, RouteID, FromMeasure, ToMeasure, LaneCode, data,
                                lane_based=True, to_km_factor=1)

    if kemantapan.all_match:
        summaryTable = kemantapan.summary().reset_index()
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
        gdb_table_writer(dbConnection, summaryTable, outputTable, columnDetails, new_table=False)
    else:
        kemantapan.merged_df.to_csv("{0}.csv".format(route))
