from SMD_Package import SMDConfigs, Configs
from SMD_Package import EventValidation, output_message, GetRoutes, gdb_table_writer, input_json_check, verify_balai, \
    read_input_excel
from SMD_Package.event_table.measurement.adjustment import Adjust
from SMD_Package.event_table.deflection.deflection import Deflection
from arcpy import SetParameterAsText, env
import pandas as pd
import numpy as np


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
        lrs_routeid = smd_config.table_fields['lrs_network']['route_id']
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
        self.check = EventValidation(input_df, column_details, lrs_network, lrs_routeid, db_connection)
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
        self.kwargs = data_config.kwargs

    def write_to_table(self, trim_to_reference=None):
        """
        Function to write selected route from valid DataFrame to GDB.
        :param trim_to_reference: The reference used for trimming measurement value. Default is None
        :return:
        """
        df_msg = self.return_all_message(return_df=True)
        verified_status = ['verified', 'VerifiedWithWarning']
        passed_routes = df_msg.loc[df_msg['status'].isin(verified_status), 'linkid'].unique().tolist()

        if len(passed_routes) != 0:
            rows = self.check.selected_route_df(self.check.df_valid, passed_routes)

            if trim_to_reference is not None:
                routeid = self.kwargs.get('routeid_col')
                from_m_col = self.kwargs.get('from_m_col')
                to_m_col = self.kwargs.get('to_m_col')
                lane_code = self.kwargs.get('lane_code')

                adjust = Adjust(rows, routeid, from_m_col, to_m_col, lane_code)
                adjust.trim_to_reference(trim_to_reference)
                rows = adjust.df

            gdb_table_writer(env.workspace, rows, self.output_table, self.column_details)

        return self

    def return_all_message(self, selection=True, return_df=False):
        """
        Function to write all type of message as arcpy.SetParameterAsText.
        :return:
        """
        def group_function(series):
            """
            This function checks for every route available message
            :param series: Input series from groupby object.
            :return: Pandas Series.
            """
            d = dict()
            status = 'status'
            d['error'] = np.any(series[status] == 'error')
            d['review'] = np.any(series[status] == 'ToBeReviewed')
            d['warning'] = np.any(series[status] == 'VerifiedWithWarning')
            d['verified'] = np.any(series[status] == 'verified')

            return pd.Series(d, index=['error', 'review', 'warning', 'verified'])

        errors = self.check.altered_route_result(include_valid_routes=True, message_type='error')
        reviews = self.check.altered_route_result(include_valid_routes=False, message_type='ToBeReviewed')
        warning = self.check.altered_route_result(include_valid_routes=False, message_type='VerifiedWithWarning')
        all_messages = errors + reviews + warning
        df = pd.DataFrame(all_messages)

        if selection:
            grouped = df.groupby('linkid')

            for route, group in grouped:
                g_status = group_function(group)
                route_selection = df['linkid'] == route

                if g_status['error']:
                    tobe_dropped = df.loc[route_selection & (df['status'] != 'error')].index
                    df.drop(tobe_dropped, inplace=True)
                    continue

                elif not g_status['verified']:
                    if g_status['review']:
                        tobe_dropped = df.loc[route_selection & (df['status'] != 'ToBeReviewed')].index
                        df.drop(tobe_dropped, inplace=True)
                        continue

            all_messages = df.to_dict(orient='records')

        SetParameterAsText(self.output_index, output_message("Succeeded", all_messages))

        if return_df:
            return df
        else:
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
            self.check.segment_duplicate_check(**self.kwargs)
            valid_routes = self.check.valid_route

            self.check.range_domain_check(routes=valid_routes, **self.kwargs)
            self.check.survey_year_check(self.data_year, **self.kwargs)
            self.check.segment_len_check(routes=valid_routes, **self.kwargs)
            self.check.measurement_check(routes=valid_routes, compare_to='RNI', **self.kwargs)

            if str(force_write) == 'false':
                self.check.coordinate_check(routes=valid_routes, comparison='RNIline-LRS',
                                            previous_year_table=compare_fc,
                                            kwargs_comparison=self.data_config.compare_table, **self.kwargs)

            # REVIEW
            if len(self.check.no_error_route) != 0:
                self.check.compare_kemantapan('IRI', compare_fc, comp_from_m, comp_to_m, comp_routeid, comp_lane_code,
                                              comp_iri, routes=self.check.no_error_route, **self.kwargs)

            self.write_to_table('RNI')  # Write passed routes to GDB
            self.return_all_message()


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
            self.check.segment_duplicate_check(**self.kwargs)
            valid_routes = self.check.valid_route

            self.check.range_domain_check(routes=valid_routes, **self.kwargs)
            self.check.survey_year_check(self.data_year, **self.kwargs)
            self.check.segment_len_check(routes=valid_routes, **self.kwargs)
            self.check.measurement_check(routes=valid_routes, compare_to='LRS', **self.kwargs)
            self.check.rni_roadtype_check(road_type_details, routes=valid_routes, **self.kwargs)

            for col in [["INN_SHTYPE", "INN_SHWIDTH", "OUT_SHTYPE", "OUT_SHWIDTH"],
                        ["INN_DITYPE", "INN_DITDEPTH", "OUT_DITYPE", "OUT_DITDEPTH"],
                        "TERRTYPE",
                        "LANDUSE"]:
                self.check.side_pattern_check(col, **self.kwargs)
                self.check.side_consistency_check(col, **self.kwargs)

            if str(force_write) == 'false':
                self.check.coordinate_check(routes=valid_routes, comparison='LRS', previous_year_table=compare_fc,
                                            previous_data_mfactor=1, kwargs_comparison=self.data_config.compare_table,
                                            **self.kwargs)

            # REVIEW
            if len(self.check.no_error_route) != 0:
                self.check.rni_compare_surftype(routes=self.check.no_error_route, **self.kwargs)

            self.write_to_table()  # Write passed routes to GDB
            self.return_all_message()


class PCICheck(TableCheckService):
    """
    Class used for PCI table check service.
    """
    def __init__(self, force_write, **kwargs):
        super(PCICheck, self).__init__(**kwargs)

        compare_fc = self.data_config.compare_table['table_name']
        asphalt_cols = self.data_config.asphalt_columns
        rigid_cols = self.data_config.rigid_columns

        if self.initial_check_passed:
            self.check.route_domain(self.kode_balai, self.route_list)
            self.check.route_selection(selection=self.route_req)
            self.check.segment_duplicate_check(**self.kwargs)
            valid_routes = self.check.valid_route

            self.check.range_domain_check(routes=valid_routes, **self.kwargs)
            self.check.survey_year_check(self.data_year, **self.kwargs)
            self.check.segment_len_check(routes=valid_routes, **self.kwargs)
            self.check.measurement_check(routes=valid_routes, compare_to='RNI', **self.kwargs)
            self.check.pci_asp_check(routes=valid_routes, asp_pref='VOL_AS', **self.kwargs)
            self.check.pci_val_check(routes=valid_routes, asp_pref='VOL_AS', rg_pref='VOL_RG', **self.kwargs)
            self.check.pci_surftype_check(routes=valid_routes, **self.kwargs)

            # Iterate all asphalt condition and severity column
            for col in asphalt_cols:
                self.check.pci_val_check(rg_pref='-', asp_pref='VOL_AS'+col, pci_col='SEV_AS'+col, max_value='NA',
                                         min_value=None, **self.kwargs)
            for col in rigid_cols:
                self.check.pci_val_check(rg_pref='VOL_RG'+col, as_pref='-', pci_col='SEV_RG'+col, max_value='NA',
                                         min_value=None, **self.kwargs)

            if str(force_write) == 'false':
                self.check.coordinate_check(routes=valid_routes, comparison='RNIline-LRS',
                                            previous_year_table=compare_fc,
                                            kwargs_comparison=self.data_config.compare_table, **self.kwargs)

            self.write_to_table('RNI')
            self.return_all_message()


class RTCCheck(TableCheckService):
    """
    Class used for RTC table check service.
    """
    def __init__(self, force_write, **kwargs):
        super(RTCCheck, self).__init__(**kwargs)

        if self.initial_check_passed:
            self.check.route_domain(self.kode_balai, self.route_list)
            self.check.route_selection(selection=self.route_req)
            valid_routes = self.check.valid_route

            self.check.range_domain_check(from_m_col=None, to_m_col=None, **self.kwargs)
            self.check.rtc_duration_check(routes=valid_routes, **self.kwargs)
            self.check.rtc_time_interval_check(routes=valid_routes, **self.kwargs)

            if str(force_write) == 'false':
                self.check.coordinate_check(from_m_col=None, routes=valid_routes, segment_data=False, lat_col='RTC_LAT',
                                            long_col='RTC_LONG', comparison='RNIline_LRS', **self.kwargs)

            self.write_to_table(None)
            self.return_all_message()


class DeflectionCheck(TableCheckService):
    """
    Class used for Deflection (LWD, FWD, and BB) table check service.
    """
    def __init__(self, force_write, sorting=False, **kwargs):
        super(DeflectionCheck, self).__init__(**kwargs)

        if self.initial_check_passed:
            self.check.route_domain(self.kode_balai, self.route_list)
            self.check.route_selection(selection=self.route_req)
            valid_routes = self.check.valid_route

            self.check.range_domain_check(lane_code='SURVEY_DIREC', **self.kwargs)
            self.check.segment_len_check(routes=valid_routes, segment_len=0.5, lane_code='SURVEY_DIREC', **self.kwargs)
            self.check.median_direction_check(routes=valid_routes, **self.kwargs)

            if str(force_write) == 'false':
                self.check.coordinate_check(routes=valid_routes, segment_data=False, lat_col='DEFL_LAT',
                                            long_col='DEFL_LONG', comparison='RNIline-LRS', **self.kwargs)

            if sorting:
                deflection = Deflection(self.check.df_valid, 'FORCE', 'FWD', 'FWD_D1', 'FWD_D2', 'ASPHALT_TEMP',
                                        routes=self.check.valid_route)
                self.check.df_valid = deflection._sorting()

            self.write_to_table(None)
            self.return_all_message()
