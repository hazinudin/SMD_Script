from SMD_Package.download.table import DownloadBalaiTable
from SMD_Package import input_json_check
from arcpy import GetParameterAsText, SetParameter, SetParameterAsText

input_json = GetParameterAsText(0)
input_dict = input_json_check(input_json, 1, req_keys=['type', 'codes', 'table_name'])

download = DownloadBalaiTable(**input_dict)

SetParameterAsText(1, download.status)  # To be replaced with message from download class.
SetParameter(2, download.output_zipfile)
