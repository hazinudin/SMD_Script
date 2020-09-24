from SMD_Package import input_json_check, output_message
from SMD_Package.event_table.checks.service import ReviewToGDB
from arcpy import GetParameterAsText, SetParameterAsText
import sys

# Get GeoProcessing input parameter
input_json = GetParameterAsText(0)

# Load the input JSON
input_details = input_json_check(input_json, 1, req_keys=['file_name', 'routes', 'data', 'year', 'balai'])
data_type = input_details["data"]
data_year = input_details["year"]
data_semester = input_details.get("semester")
trim_to = 'RNI'
input_details['semester_data'] = False
input_details['year_sem_check'] = False

if str(data_type) == "RNI":  # If the data is IRI/Roughness
    data_config = 'RNICheck/rni_config_2020.json'
    output_table = "SMD.RNI"
    trim_to = None

    rni_kwargs = {
        'roughness_table': "SMD.ROUGHNESS_1_2020",
        'pci_table': "SMD.PCI_2020",
        'rtc_table': "SMD.RTC_2020",
        'fwd_table': "SMD.FWD_2020",
        'lwd_table': "SMD.LWD_2020",
        'bb_table': "SMD.BB_2020"
    }
    input_details.update(rni_kwargs)

elif str(data_type) == "IRI":  # If the data is RNI
    data_config = 'RoughnessCheck/roughness_config_2020.json'
    output_table = "SMD.ROUGHNESS"
    input_details['semester_data'] = True

elif str(data_type) == "PCI":  # If the data is RNI
    data_config = 'PCICheck/pci_config_2020.json'
    output_table = "SMD.PCI"

elif str(data_type) == "RTC":  # If the data is RNI
    data_config = 'RTCCheck/rtc_config_2020.json'
    output_table = "SMD.RTC"

elif str(data_type) == "FWD":  # If the data is RNI
    data_config = 'FWDCheck/fwd_config_2020.json'
    output_table = "SMD.FWD"
    trim_to = None

elif str(data_type) == "LWD":  # If the data is RNI
    data_config = 'LWDCheck/lwd_config_2020.json'
    output_table = "SMD.LWD"
    trim_to = None

elif str(data_type) == "BB":  # If the data is RNI
    data_config = 'BBCheck/bb_config_2020.json'
    output_table = "SMD.BB"
    trim_to = None

else:  # If other than that, the process will be terminated with an error message.
    message = 'Data type {0} is not supported'.format(data_type)
    SetParameterAsText(1, output_message("Failed", message))
    sys.exit(0)

ReviewToGDB(input_json=input_json, config_path=data_config, output_table=output_table, output_index=1,
            trim_to=trim_to, data_type=data_type, **input_details)

