from arcpy import GetParameterAsText
from SMD_Package.event_table.checks.service import DeflectionCheck

# Get GeoProcessing input parameter
inputJSON = GetParameterAsText(0)
forceWrite = GetParameterAsText(1)

# Load the input JSON
DeflectionCheck(forceWrite, input_json=inputJSON, config_path='BBCheck/bb_config.json', output_table='SMD.BB_TEST')
