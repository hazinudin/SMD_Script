import os
import sys
import json
sys.path.insert(0, 'E:\SMD_Script\EventTableCheck')  # Import the EventTable package
from EventTable import TableCheck

os.chdir('E:/SMD_Script')  # Change the directory to the SMD root directory

# Load the roughness script config JSON file
with open('RoughnessCheck/roughness_config.json') as config_f:
    config = json.load(config_f)

column_details = config['column_details']  # Load the roughness column details dictionary

# Create a EventTableCheck class object
event_check = TableCheck.EventTableCheck('RoughnessCheck/Format Data Rough.xlsx')

# Start checking the event table column name and redundant column in event table
if event_check.header_checker(column_details) is None:
    pass
else:
    # Should return a SetParameterAsText
    print event_check.header_checker(column_details)
