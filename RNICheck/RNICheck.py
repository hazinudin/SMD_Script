from arcpy import GetParameterAsText
from SMD_Package.event_table.checks.service import RNICheck

# Get GeoProcessing input parameter
inputJSON = GetParameterAsText(0)
forceWrite = GetParameterAsText(1)

RNICheck(forceWrite, input_json=inputJSON, config_path='RNICheck/rni_config_2020.json', output_table='SMD.RNI_TEST',
         semester_data=False)
