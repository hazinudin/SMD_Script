from SMD_Package.event_table.rni.summary import SurfaceTypeSummary
from SMD_Package import input_json_check
from arcpy import GetParameterAsText, SetParameterAsText

input_text = GetParameterAsText(0)
input_j = input_json_check(input_text, 1, req_keys=['routes'])
rekap = SurfaceTypeSummary(**input_j)
SetParameterAsText(1, rekap.status)
