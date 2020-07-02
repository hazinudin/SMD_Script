from arcpy import GetParameterAsText
from SMD_Package.event_table.checks.service import RNICheck

# Get GeoProcessing input parameter
inputJSON = GetParameterAsText(0)
forceWrite = GetParameterAsText(1)

kwargs = {
    'roughness_table': "SMD.ROUGHNESS_2_2020",
    'pci_table': "SMD.PCI_2020",
    'rtc_table': "SMD.RTC_2020",
    'fwd_table': "SMD.FWD_2020",
    'lwd_table': "SMD.LWD_2020",
    'bb_table': "SMD.BB_2020"
}

RNICheck(forceWrite, input_json=inputJSON, config_path='RNICheck/rni_config_2020.json', output_table='SMD.RNI_TEST',
         semester_data=False, **kwargs)
