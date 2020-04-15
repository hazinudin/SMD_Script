from SMD_Package import SMDConfigs, Configs
from SMD_Package import EventValidation, output_message, GetRoutes, gdb_table_writer, input_json_check, verify_balai, \
    read_input_excel
from SMD_Package.event_table.measurement.adjustment import Adjust
from arcpy import SetParameterAsText, env


class TableCheckService(object):
    """
    This class provide class method for multiple data type check.
    """
    def __init__(self, input_json, config_path, output_table, output_index=2, smd_dir='E:/SMD_Script',
                 table_suffix=None, semester_data=False, **kwargs):
        """
        Class initialization.
        :param input_json: The input JSON string.
        :param config_path: The config file path.
        :param output_table: Output table string.
        :param output_index: Index used for writing arcpy.SetParameterAsText output.
        :param smd_dir: The root directory of SMD_Script.
        :param table_suffix: Suffix used for the output table. Default is None.
        :param semester_data: Boolean if the data is semester data. Default is False.
        """
        import os
        import sys
        os.chdir(smd_dir)  # Change the directory to SMD root directory
        smd_config = SMDConfigs()  # Load the SMD config
        data_config = Configs(config_path)  # Load the data config

        if not semester_data:
            input_details = input_json_check(input_json, output_index, req_keys=['file_name', 'balai', 'year',
                                                                                 'routes'])
            data_semester = None
        else:
            input_details = input_json_check(input_json, output_index, req_keys=['file_name', 'balai', 'year', 'routes',
                                                                                 'semester'])
            data_semester = input_details['semester']

        table_path = input_details['file_name']
        data_year = input_details['year']
        kode_balai = input_details['balai']
        route_req = input_details['routes']

        lrs_network = smd_config.table_names['lrs_network']
        balai_table = smd_config.table_names['balai_table']
        balai_kode_balai = smd_config.table_fields['balai_table']['balai_code']
        db_connection = smd_config.smd_database['instance']
        balai_route_table = smd_config.table_names['balai_route_table']

        column_details = data_config.column_details

        env.workspace = db_connection  # Setting up the workspace for arcpy
        input_df = read_input_excel(table_path, parameter_index=output_index)
        route_list = GetRoutes("balai", kode_balai, lrs_network, balai_table, balai_route_table).route_list()
        code_check_result = verify_balai(kode_balai, balai_table, balai_kode_balai, env.workspace)

        if len(code_check_result) != 0:  # Check if the balai code is a valid balai code.
            message = "Kode {0} {1} tidak valid.".format("balai", code_check_result)
            SetParameterAsText(output_index, output_message("Failed", message))
            sys.exit(0)

        # Initialize the event validation class
        self.check = EventValidation(input_df, column_details, lrs_network, 'ROUTEID', db_connection)
        header_check_result = self.check.header_check_result
        dtype_check_result = self.check.dtype_check_result

        if not semester_data:
            output_table = output_table + "_{0}".format(data_year)
            year_sem_check_result = self.check.year_and_semester_check(data_year, None, year_check_only=True)
        else:
            output_table = output_table + "_{0}_{1}".format(data_semester, data_year)
            year_sem_check_result = self.check.year_and_semester_check(data_year, data_semester)

        if table_suffix is not None:
            output_table = output_table + "_{0}".format(table_suffix)

        initial_check_passed = False
        if (header_check_result is None) & (dtype_check_result is None) & (year_sem_check_result is None):
            initial_check_passed = True
        elif dtype_check_result is None:
            SetParameterAsText(output_index, output_message("Rejected", year_sem_check_result))
        else:
            SetParameterAsText(output_index, output_message("Rejected", dtype_check_result))

        self.initial_check_passed = initial_check_passed  # The initial check result
        self.output_table = output_table  # The output table string
        self.route_list = route_list  # Route list from the specified balai in the input JSON
        self.route_req = route_req  # The route selection from input JSON
        self.kode_balai = kode_balai
        self.data_year = data_year
        self.data_semester = data_semester
        self.data_config = data_config
        self.smd_config = smd_config
        self.column_details = column_details
        self.output_index = output_index

    def write_to_table(self, trim_to_reference=None):
        """
        Function to write selected route from valid DataFrame to GDB.
        :param trim_to_reference: The reference used for trimming measurement value. Default is None
        :return:
        """
        if len(self.check.passed_routes) != 0:
            rows = self.check.selected_route_df(self.check.df_valid, self.check.passed_routes)

            if trim_to_reference is not None:
                adjust = Adjust(rows, "LINKID", "STA_FROM", "STA_TO", "LANE_CODE")
                adjust.trim_to_reference(trim_to_reference)
                rows = adjust.df

            gdb_table_writer(env.workspace, rows, self.output_table, self.column_details)

        return self

    def return_all_message(self):
        """
        Function to write all type of message as arcpy.SetParameterAsText.
        :return:
        """
        errors = self.check.altered_route_result(include_valid_routes=True, message_type='error')
        reviews = self.check.altered_route_result(include_valid_routes=False, message_type='ToBeReviewed')
        warning = self.check.altered_route_result(include_valid_routes=False, message_type='VerifiedWithWarning')
        all_messages = errors + reviews + warning

        SetParameterAsText(self.output_index, output_message("Succeeded", all_messages))

        return self

    def return_error_message(self):
        """
        Function to write all error messages as arcpy.SetParameterAsText.
        :return:
        """
        errors = self.check.altered_route_result()
        SetParameterAsText(self.output_index, output_message("Succeeded", errors))

        return self


class RoughnessCheck(TableCheckService):
    """
    Class used for Roughness table check service.
    """
    def __init__(self, force_write, **kwargs):
        super(RoughnessCheck, self).__init__(**kwargs)

        compare_fc = self.data_config.compare_table['table_name']
        comp_routeid = self.data_config.compare_table['route_id']
        comp_from_m = self.data_config.compare_table['from_measure']
        comp_to_m = self.data_config.compare_table['to_measure']
        comp_lane_code = self.data_config.compare_table['lane_code']
        comp_iri = self.data_config.compare_table['iri']

        if self.initial_check_passed:
            self.check.route_domain(self.kode_balai, self.route_list)
            self.check.route_selection(selection=self.route_req)
            self.check.segment_duplicate_check()
            valid_routes = self.check.valid_route

            self.check.range_domain_check(routes=valid_routes)
            self.check.survey_year_check(self.data_year)
            self.check.segment_len_check(routes=valid_routes)
            self.check.measurement_check(routes=valid_routes, compare_to='RNI')

            if str(force_write) == 'false':
                self.check.coordinate_check(routes=valid_routes, comparison='RNIline-LRS',
                                            previous_year_table=compare_fc)

            # REVIEW
            if len(self.check.no_error_route) != 0:
                self.check.compare_kemantapan('IRI', compare_fc, comp_from_m, comp_to_m, comp_routeid, comp_lane_code,
                                              comp_iri, routes=self.check.no_error_route)
                self.return_all_message()

            self.write_to_table('RNI')  # Write passed routes to GDB
            self.return_error_message()


class RNICheck(TableCheckService):
    """
    Class used for RNI table check service.
    """
    def __init__(self, force_write, **kwargs):
        super(RNICheck, self).__init__(**kwargs)

        road_type_details = self.data_config.roadtype_details  # RNI road type details from data config file.
        compare_fc = self.smd_config.table_names['rni']

        if self.initial_check_passed:
            self.check.route_domain(self.kode_balai, self.route_list)
            self.check.route_selection(selection=self.route_req)
            self.check.segment_duplicate_check()
            valid_routes = self.check.valid_route

            self.check.range_domain_check(routes=valid_routes)
            self.check.survey_year_check(self.data_year)
            self.check.segment_len_check(routes=valid_routes)
            self.check.measurement_check(routes=valid_routes, compare_to='LRS')
            self.check.rni_roadtype_check(road_type_details, routes=valid_routes)
            # self.check.side_consistency_check()

            if str(force_write) == 'false':
                self.check.coordinate_check(routes=valid_routes, comparison='LRS', previous_year_table=compare_fc,
                                            previous_data_mfactor=1)

            # REVIEW
            if len(self.check.no_error_route) != 0:
                self.check.rni_compare_surftype(routes=self.check.no_error_route)
                self.return_all_message()

            self.write_to_table()  # Write passed routes to GDB
            self.return_error_message()


class PCICheck(TableCheckService):
    """
    Class used for PCI table check service.
    """
    def __init__(self, force_write, **kwargs):
        super(PCICheck, self).__init__(**kwargs)

        compare_fc = self.data_config.compare_table['table_name']
        # comp_routeid = self.data_config.compare_table['route_id']
        # comp_from_m = self.data_config.compare_table['from_measure']
        # comp_to_m = self.data_config.compare_table['to_measure']
        # comp_lane_code = self.data_config.compare_table['lane_code']
        # comp_iri = self.data_config.compare_table['iri']

        if self.initial_check_passed:
            self.check.route_domain(self.kode_balai, self.route_list)
            self.check.route_selection(selection=self.route_req)
            self.check.segment_duplicate_check()
            valid_routes = self.check.valid_route

            self.check.range_domain_check(routes=valid_routes)
            self.check.survey_year_check(self.data_year)
            self.check.segment_len_check(routes=valid_routes)
            self.check.measurement_check(routes=valid_routes, compare_to='RNI')
            self.check.pci_asp_check(routes=valid_routes)
            self.check.pci_val_check(routes=valid_routes)
            self.check.pci_surftype_check(routes=valid_routes)

            if str(force_write) == 'false':
                self.check.coordinate_check(routes=valid_routes, comparison='RNIline-LRS',
                                            previous_year_table=compare_fc)

            self.write_to_table('RNI')
            self.return_all_message()

