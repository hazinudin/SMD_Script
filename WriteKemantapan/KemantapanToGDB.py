import sys
sys.path.append('E:/SMD_Script')
from SMD_Package import Kemantapan, gdb_table_writer, event_fc_to_df
from arcpy import GetParameterAsText, env
import json
import os
os.chdir('E:/SMD_Script')

request = GetParameterAsText(0)
data = GetParameterAsText(1)
request_j = json.loads(request)
with open('smd_config.json') as config_f:
    smd_config = json.load(config_f)

routeSelection = request_j['routes']
DataSemester = request_j['semester']
DataYear = request_j['year']

dbConnection = smd_config['smd_database']['instance']

DataTable = 'SMD.{0}_{1}_{2}'.format(data, DataSemester, DataYear)
RouteID = 'LINKID'
FromMeasure = 'STA_FROM'
ToMeasure = 'STA_TO'
GradeColumn = 'IRI'
LaneCode = 'LANE_CODE'

columnDetails = dict()

env.workspace = dbConnection
InputDF = event_fc_to_df(DataTable, [RouteID, FromMeasure, ToMeasure, GradeColumn], routeSelection, RouteID,
                         dbConnection, is_table=True)

kemantapan = Kemantapan(InputDF, GradeColumn, RouteID, FromMeasure, ToMeasure, LaneCode, 'ROUGHNESS', to_km_factor=1)
summaryTable = kemantapan.summary().reset_index()

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
gdb_table_writer(dbConnection, summaryTable, 'SMD.KEMANTAPAN_{0}_{1}_{2}'.format(data, DataSemester, DataYear),
                 columnDetails, new_table=False)
