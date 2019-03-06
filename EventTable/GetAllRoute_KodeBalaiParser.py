import json
from arcpy import GetParameterAsText, SetParameterAsText

input_json = GetParameterAsText(0)
input_dict = json.loads(input_json)

kode_balai = input_dict['balai']
output_json = json.dumps({"type": "balai", "codes": kode_balai})
output_json = SetParameterAsText(1, output_json)