from SMD_Package.event_table.kemantapan.service import KemantapanService
from arcpy import GetParameterAsText, SetParameterAsText

input_json = GetParameterAsText(0)
kemantapan = KemantapanService(input_json)
SetParameterAsText(1, kemantapan.status_json)
