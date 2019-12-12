from unittest import TestCase
import os
import json
from SMD_Package import input_json_check, EventValidation, GetRoutes
from SMD_Package.event_table.input_excel import read_input_excel
from arcpy import env
import pandas as pd

os.chdir('E:\SMD_Script')


class TestEventValidation(TestCase):
    @staticmethod
    def read_smdconfig():
        # Load the SMD config JSON file
        with open('smd_config.json') as smd_config_f:
            smd_config = json.load(smd_config_f)

        return smd_config

    @property
    def balai_table(self):
        config = self.read_smdconfig()
        return config['table_names']['balai_table']

    @property
    def balai_route_table(self):
        config = self.read_smdconfig()
        return config['table_names']['balai_route_table']

    @staticmethod
    def read_roughconfig():
        # Load the roughness script config JSON file
        with open('RoughnessCheck/roughness_config.json') as config_f:
            roughness_config = json.load(config_f)

        return roughness_config

    def test_coordinate_segment(self):
        check = self.validation_class(multiply_row=12)
        check.coordinate_check(comparison='RNIseg-LRS')

    def test_coordinate_line(self):
        check = self.validation_class(multiply_row=12)
        check.coordinate_check(comparison='RNIline-LRS')
