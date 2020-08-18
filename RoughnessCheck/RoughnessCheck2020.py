from arcpy import GetParameterAsText
from SMD_Package.event_table.checks.service import RoughnessCheck

# Get GeoProcessing input parameter
inputJSON = GetParameterAsText(0)
forceWrite = GetParameterAsText(1)

RoughnessCheck(forceWrite, input_json=inputJSON, config_path='RoughnessCheck/roughness_config_2020.json',
               output_table='SMD.ROUGHNESS_TEST', semester_data=True)
