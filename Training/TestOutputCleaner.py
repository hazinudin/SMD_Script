from arcpy import Delete_management, env
import json

with open ('D:/SMD_script/config.json') as config_file:
    config = json.load(config_file)

env.workspace = config['gdb_conn']
FDS_name = "Data_2018_DEV_1"
Delete_management(FDS_name)