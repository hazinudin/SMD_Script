from arcpy import GetParameterAsText
from SMD_Package.event_table.checks.service import RTCCheck

# Get GeoProcessing input parameter
inputJSON = GetParameterAsText(0)
forceWrite = GetParameterAsText(1)

RTCCheck(forceWrite, input_json=inputJSON, config_path="RTCCheck/rtc_config.json", output_table="SMD.RTC_TEST")
