import sys
sys.path.append('E:/SMD_Script')
from SMD_Package import Kemantapan, gdb_table_writer, event_fc_to_df
from arcpy import GetParameterAsText, env
import json
import os
os.chdir('E:/SMD_Script')

request = GetParameterAsText(0)
request_j = json.loads(request)
with open('smd_config.json') as config_f:
    smd_config = json.load(config_f)

routeSelection = request_j['routes']

dbConnection = smd_config['smd_database']['instance']
rniTable = smd_config['table_names']['rni']
rniRouteID = smd_config['table_fields']['rni']['route_id']
rniFromMeasure = smd_config['table_fields']['rni']['from_measure']
rniToMeasure = smd_config['table_fields']['rni']['to_measure']
rniSurfType = smd_config['table_fields']['rni']['surface_type']

iriTable = 'ELRS.Roughness_National_2'
iriRouteID = 'LINKID'
iriFromMeasure = 'KMPOST'
iriToMeasure = 'KMPOSTTO'
iriGrade = 'IRI'

columnDetails = dict()

env.workspace = dbConnection
rniDf = event_fc_to_df(rniTable, [rniRouteID, rniFromMeasure, rniToMeasure, rniSurfType], routeSelection, rniRouteID,
                       dbConnection, is_table=True)
iriDf = event_fc_to_df(iriTable, [iriRouteID, iriFromMeasure, iriToMeasure, iriGrade], routeSelection, iriRouteID,
                       dbConnection, is_table=False, include_all=False)

kemantapan = Kemantapan(rniDf, iriDf, iriGrade, iriRouteID, iriFromMeasure, iriToMeasure, rniRouteID, rniFromMeasure,
                        rniToMeasure, surftype_col=rniSurfType)
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
gdb_table_writer(dbConnection, summaryTable, 'kemantapan_2018_DEV', columnDetails, new_table=False)
