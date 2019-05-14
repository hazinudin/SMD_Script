import sys
import os
sys.path.append('E:/SMD_Script')
os.chdir('E:/SMD_Script')
from SMD_Package import EventValidation
from pandas import DataFrame
from arcpy import da, env, GetParameterAsText, SetParameter
import json


# Load the RNI Config JSON file
with open('RNICheck/rni_config.json') as config_f:
    rni_config = json.load(config_f)

env.workspace = 'Database Connections/ELRS@GEODBBM 144.sde'
env.overwriteOutput = True
rni_fc = 'ELRS.RNI_National_2'
input_fields = ['LINKID', 'KMPOST', 'KMPOSTTO', 'LANE_CODE', 'ROAD_TYPE', 'MEDWIDTH']
prov_code = GetParameterAsText(0)

# Create the array
ar = da.FeatureClassToNumPyArray(rni_fc, input_fields, where_clause="NOPROP = '{0}'".format(prov_code))
df = DataFrame(ar)  # Create the DataFrame
roadtype_details = rni_config['roadtype_details']
# The column details for header and Dtype check
column_details = {
    "LINKID": {"dtype": "string"},
    "KMPOST": {"dtype": "integer"},
    "KMPOSTTO": {"dtype": "integer"},
    "LANE_CODE": {"dtype": "string"},
    "ROAD_TYPE": {"dtype": "integer"},
    "MEDWIDTH": {"dtype": "double"}
}

TableCheck = EventValidation(df, column_details, 'ELRS.National_Network2018', 'ROUTEID', env.workspace)
TableCheck.measurement_check(rni_fc, 'LINKID', 'TOMEASURE', from_m_col='KMPOST', to_m_col='KMPOSTTO', lane_code='LANE_CODE')
TableCheck.rni_roadtype_check(roadtype_details, from_m_col='KMPOST', to_m_col='KMPOSTTO', lane_codes='LANE_CODE')

DataFrame(TableCheck.altered_route_result()).to_csv("{1}/error_check_{0}.csv".format(prov_code, env.scratchFolder))
SetParameter(1, "{1}/error_check_{0}.csv".format(prov_code, env.scratchFolder))
