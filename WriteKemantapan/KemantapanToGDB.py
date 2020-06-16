from SMD_Package.event_table.kemantapan.service import KemantapanService
from arcpy import GetParameterAsText

input_json = GetParameterAsText(0)
kemantapan_service = KemantapanService(input_json)
