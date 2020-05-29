from arcpy import GetParameterAsText
from SMD_Package.event_table.checks.service import PCICheck

# Get GeoProcessing input parameter
inputJSON = GetParameterAsText(0)
forceWrite = GetParameterAsText(1)

PCICheck(forceWrite, input_json=inputJSON, config_path='PCICheck/pci_config_2020.json', output_table="SMD.PCI_TEST")
