import os
import sys
import json
sys.path.insert(0, 'E:\SMD_Script\EventTableCheck')
from EventTableCheck import TableCheck

os.chdir('E:/SMD_Script')

with open('RoughnessCheck/roughness_config.json') as config_f:
    config = json.load(config_f)

column_details = config['column_details']

roughness_check = TableCheck.header_checker('RoughnessCheck/Format Data Rough.xlsx', column_details)
print roughness_check