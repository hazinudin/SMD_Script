from unittest import TestCase
import os
import json
import time

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
        with open('RNICheck/rni_config_2019App.json') as config_f:
            roughness_config = json.load(config_f)

        return roughness_config

    def validation_class(self, input_json='{"file_name":"//10.10.25.12/smd/excel_survey/rni/rni_3_25-11-2019_015804.xlsx", "balai":"3", "year":2019, "routes":"06001"}',
                         data_type='ROUGHNESS', multiply_row=None):

        if data_type == 'ROUGHNESS':
            data_config = self.read_roughconfig()
        else:
            pass

        smd_config = self.read_smdconfig()

        # The smd config JSON details
        lrs_network = smd_config['table_names']['lrs_network']
        lrs_network_rid = smd_config['table_fields']['lrs_network']['route_id']
        db_connection = smd_config['smd_database']['instance']

        # Load the input JSON
        input_details = input_json_check(input_json, 1, req_keys=['file_name', 'balai', 'year'])
        table_path = input_details["file_name"]
        # All the column details in the roughness_config.json
        column_details = data_config['column_details']  # Load the roughness column details dictionary
        # Set the environment workspace
        env.workspace = db_connection

        # Read the input table
        input_df = read_input_excel(table_path)  # Read the excel file

        if multiply_row is not None:
            input_df = pd.concat([input_df]*multiply_row, ignore_index=True)

        event_check = EventValidation(input_df, column_details, lrs_network, lrs_network_rid, db_connection)

        return event_check

    def test_year_and_semester_check(self):
        check = self.validation_class()  # Create the class

        check.year_and_semester_check(2017, 1)
        self.assertTrue(len(check.route_results) != 0, 'Unmatched year and semester test')
        check.route_results = {}  # Clear the previous result

        check.year_and_semester_check(2019, 1)
        self.assertTrue(len(check.route_results) == 0, 'Matched year and semester test')
        check.route_results = {}

        check.year_and_semester_check(2019, 1, year_check_only=True)
        self.assertTrue(len(check.route_results) == 0, 'Matched year and semester test (year only test)')
        check.route_results = {}

        check.year_and_semester_check(2019, 2, year_check_only=True)
        self.assertTrue(len(check.route_results) == 0., 'Matched year only and not semester semester (year only test)')
        check.route_results = {}

    def test_route_domain(self):
        check = self.validation_class()  # Create the EventValidation class

        balai_code = '7'
        route_list = GetRoutes('balai', balai_code, check.lrs_network, self.balai_table, self.balai_route_table).route_list()
        check.route_domain(balai_code, route_list)
        self.assertTrue(len(check.route_results) == 0, 'Valid routes test')
        check.route_results = {}

        balai_code = '8'
        route_list = GetRoutes('balai', balai_code, check.lrs_network, self.balai_table, self.balai_route_table).route_list()
        check.route_domain(balai_code, route_list)
        self.assertTrue(len(check.route_results) != 0, 'Invalid')
        check.route_results = {}

    def test_range_domain_check(self):
        check = self.validation_class()

        check.df_string['IRI'] = 3.1
        check.range_domain_check()
        self.assertTrue(len(check.route_results) == 0, 'Valid range and domain in input file.')
        check.route_results = {}

        check.df_string['IRI'] = 3.0
        check.range_domain_check()
        self.assertTrue(len(check.route_results) == 0, 'Same with the lower bound value')
        check.route_results = {}

        check.column_details['IRI']['range']['eq_lower'] = False
        check.range_domain_check()
        self.assertFalse(len(check.route_results) == 0, 'Same with the lower bound value, eq_lower=False')
        check.route_results = {}

        check.column_details['IRI']['range']['eq_lower'] = True
        check.column_details['IRI']['range']['review'] = True
        check.df_string['IRI'] = 2.999
        check.range_domain_check()
        self.assertTrue(len(check.altered_route_result()) == 0, 'Lower than the lower bound value')
        check.route_results = {}

    def test_coordinate_segment(self):
        check = self.validation_class(multiply_row=12)
        check.coordinate_check(comparison='RNIseg-LRS')

    def test_coordinate_line(self):
        check = self.validation_class()
        check.coordinate_check(comparison='LRS')
        self.assertTrue(True)
