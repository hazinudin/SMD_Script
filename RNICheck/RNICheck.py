from arcpy import GetParameterAsText
from SMD_Package.event_table.checks.service import TableCheckService

# Get GeoProcessing input parameter
inputJSON = GetParameterAsText(0)
forceWrite = GetParameterAsText(1)

check = TableCheckService(inputJSON, 'RNICheck/rni_config_2019App.json', 'SMD.RNI_TEST')
check.rni_check(forceWrite)
