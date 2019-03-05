import os
import sys
import json
sys.path.insert(0, 'E:\SMD_Script\EventTableCheck')  # Import the EventTable package
from EventTable import TableCheck

os.chdir('E:/SMD_Script')  # Change the directory to the SMD root directory

# Load the roughness script config JSON file
with open('RoughnessCheck/roughness_config.json') as config_f:
    config = json.load(config_f)

ColumnDetails = config['column_details']  # Load the roughness column details dictionary
TablePath = 'RoughnessCheck/Format Data Rough.xlsx'

# Create a EventTableCheck class object
event_check = TableCheck.EventTableCheck(TablePath, ColumnDetails)
event_check.header_check()  # Start check the header

if event_check.header_check_result is None:  # If the header check returns None, the process can continue
    pass
    print event_check.df_string
else:
    # Should return a SetParameterAsText
    print event_check.header_check_result
