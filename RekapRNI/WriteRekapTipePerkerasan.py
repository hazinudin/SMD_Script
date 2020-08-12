from SMD_Package.event_table.rni import RNISummary
from SMD_Package import input_json_check
from arcpy import GetParameterAsText, SetParameterAsText

input_text = GetParameterAsText(0)
input_j = input_json_check(input_text, 1, req_keys=['routes'])
rekap = RNISummary(**input_j)
rekap.surface_summary(return_df=False)
SetParameterAsText(1, rekap.status)
