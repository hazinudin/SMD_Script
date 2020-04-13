from SMD_Package import SMDConfigs, Configs
from SMD_Package import EventValidation, output_message, GetRoutes, gdb_table_writer, input_json_check, verify_balai, \
    read_input_excel, convert_and_trim
from arcpy import SetParameterAsText, env


class TableCheckService(object):
    """
    This class provide class method for multiple data type check.
    """
    def __init__(self, input_json, config_path, output_table, output_index=2, smd_dir='E:/SMD_Script',
                 table_suffix=None, semester_data=False):
        """
        Class initialization.
        :param input_json:
        :param config_path:
        :param output_table:
        :param output_index:
        :param smd_dir:
        :param table_suffix:
        :param semester_data:
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
        self.event_check = EventValidation(input_df, column_details, lrs_network, 'ROUTEID', db_connection)
        header_check_result = self.event_check.header_check_result
        dtype_check_result = self.event_check.dtype_check_result

        if data_semester is not None:
            output_table = output_table + "_{0}_{1}".format(data_year, table_suffix)
            year_sem_check_result = self.event_check.year_and_semester_check(data_year, None, year_check_only=True)
        else:
            output_table = output_table + "_{0}_{1}_{2}".format(data_semester, data_year, table_suffix)
            year_sem_check_result = self.event_check.year_and_semester_check(data_year, data_semester)

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
