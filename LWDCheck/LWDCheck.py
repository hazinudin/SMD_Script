from arcpy import GetParameterAsText
from SMD_Package.event_table.checks.service import DeflectionCheck

# Get GeoProcessing input parameter
inputJSON = GetParameterAsText(0)
forceWrite = GetParameterAsText(1)

# Load the input JSON
DeflectionCheck(forceWrite, input_json=inputJSON, config_path='LWDCheck/lwd_config_2020.json', smd_dir='E:/SMD_Script',
                output_table='SMD.LWD_TEST')
