from SMD_Package import SMDConfigs, GetRoutes, event_fc_to_df, Kemantapan, gdb_table_writer, input_json_check
from SMD_Package.event_table.traffic.aadt import TrafficSummary
from SMD_Package.event_table.deflection.deflection import Deflection
from arcpy import env, ListFields, Exists
import json
import pandas as pd
import datetime
import numpy as np


class KemantapanService(object):
    """
    This class provide class method for multiple Kemantapan data type calculation.
    """
    def __init__(self, input_json, **kwargs):
        """
        Class initialization.
        :param input_json: The input JSON. Contain 'routes', 'data_type', 'lane_based' and 'year',
        optional parameter 'semester', 'table_name', 'routeid_col', 'from_m_col', 'to_m_col', 'lane_code_col',
        'output_table' and 'grading_col'.
        """
        import os

        if SMDConfigs.smd_dir() == '':
            pass
        else:
            os.chdir(SMDConfigs.smd_dir())  # Change directory to SMD root dir.

        smd_config = SMDConfigs()
        self.smd_config = smd_config
        db_connection = self.smd_config.smd_database['instance']

        self.satker_ppk_route_table = self.smd_config.table_names['ppk_route_table']
        satker_route_fields = self.smd_config.table_fields['ppk_route_table']
        self.satker_routeid = satker_route_fields['route_id']
        self.satker_ppk_id = satker_route_fields['satker_ppk_id']
        self.satker_id = satker_route_fields['satker_id']
        self.satker_route_from_date = satker_route_fields['from_date']
        self.satker_route_to_date = satker_route_fields['to_date']

        self.balai_table = smd_config.table_names['balai_table']
        self.balai_route_table = smd_config.table_names['balai_route_table']
        balai_table_fields = self.smd_config.table_fields['balai_table']
        balai_route_fields = self.smd_config.table_fields['balai_route_table']
        self.balai_prov_balai_id = balai_table_fields['balai_code']
        self.balai_prov_prov_id = balai_table_fields['prov_code']
        self.balai_prov_from_date = balai_table_fields['from_date']
        self.balai_prov_to_date = balai_table_fields['to_date']
        self.balai_route_balai_id = balai_route_fields['balai_code']
        self.balai_route_route_id = balai_route_fields['route_id']
        self.balai_route_from_date = balai_route_fields['from_date']
        self.balai_route_to_date = balai_route_fields['to_date']

        env.workspace = db_connection

        request_j = input_json_check(input_json, 1, True, ['routes', 'year', 'data_type'])

        with open(os.path.dirname(__file__)+'\\'+'kemantapan_config.json') as config_f:
            config = json.load(config_f)

        try:
            self.routes = request_j['routes']  # Mandatory JSON parameter.
            self.year = request_j['year']
            self.data_type = str(request_j['data_type'])  # 'IRI', 'IRI_POK', 'PCI', 'PCI_POK', 'AADT'

        except KeyError:
            raise  # Maybe add an error message in here.

        self.semester = request_j.get('semester')  # Optional parameter.
        self.table_name = None
        self.grading_col = None
        self.routeid_col = None
        self.from_m_col = None
        self.to_m_col = None
        self.lane_code_col = None
        self.date_col = None
        self.suffix = None
        self.to_km_factor = None
        self.semester_col = 'SEMESTER'
        self.year_col = 'YEAR'
        self.prov_column = 'BM_PROV_ID'
        self.balai_column = 'BALAI_ID'
        self.update_date_col = 'UPDATE_DATE'
        self.segment_len_col = 'SEGMENT_LENGTH'
        self.force_update = False
        self.project_to_sk = False

        # For AADT only
        self.hour_col = None
        self.minute_col = None
        self.survey_direc_col = None
        self.veh_col_prefix = None

        # For Deflection only
        self.force_col = None
        self.d0_col = None
        self.d200_col = None
        self.asp_temp = 'ASPHALT_TEMP'

        # if self.semester is None:
        #     self.__dict__.update(config[str(self.data_type)][str(self.year)])
        # else:
        #     self.__dict__.update(config[str(self.data_type)][str(self.year)+'_'+str(self.semester)])

        self.__dict__.update(config[str(self.data_type)])  # Update class attribute using config.

        self.__dict__.update(request_j)  # Update the class attribute based on the input JSON.
        self.kwargs = kwargs
        grade_col_exist = self.check_grading_column()

        if self.data_type not in ['AADT', 'FWD', 'LWD', 'BB']:  # If the requested summary is other than PCI/IRI.
            request_j = input_json_check(input_json, 1, True, ['routes', 'year',
                                                               'data_type', 'method'])
            self.method = str(request_j['method'])  # 'mean', 'max', 'lane_based'

            if self.method == 'lane_based':
                self.lane_based = True
            else:
                self.lane_based = False

            if self.lane_based:
                self.output_table = 'SMD.KEMANTAPAN_LKM_{0}'.format(self.grading_col)
            else:
                self.output_table = 'SMD.KEMANTAPAN_{0}_{1}'.format(str.upper(self.method), self.grading_col)

        else:  # Includes AADT, LWD, FWD and BB.
            self.lane_based = None
            self.method = None
            self.output_table = 'SMD.{0}'.format(self.data_type)

            if self.data_type == 'LWD':  # The force column for LWD data.
                self.force_col = "LOAD_KG"
                self.d0_col = 'LWD_D0'
                self.d200_col = 'LWD_D1'
            if self.data_type == 'FWD':  # The force column for FWD data.
                self.force_col = 'FORCE'
                self.d0_col = 'FWD_D1'
                self.d200_col = 'FWD_D2'

        if not grade_col_exist and \
                (self.data_type not in ['AADT', 'LWD', 'FWD', 'BB']):  # Check if the grading column does not exist.
            self.status_json = "Kolom {0} tidak dapat ditemukan pada table {1}.".\
                format(self.grading_col, self.table_name)
            return

        # Change the output table suffix if self.project_len is True
        if self.project_to_sk:
            self.suffix = 'SK'

        if self.suffix is not None:
            self.output_table = self.output_table + '_' + self.suffix

        self.summary_result = pd.DataFrame()  # For storing all summary result
        self.route_date = None
        self.failed_route = list()  # For storing route which cannot be calculated.
        self.route_status = pd.DataFrame(columns=[self.routeid_col, 'time', 'status'])  # For storing all status for each requested routes.

        # Select the route request based on source-output update date.
        self.route_selection = self._route_date_selection()

        for route in self.route_selection:
            if self.data_type not in ['AADT', 'LWD', 'FWD', 'BB']:  # For IRI or PCI
                self.data_columns = [self.routeid_col, self.from_m_col, self.to_m_col, self.lane_code_col,
                                     self.grading_col, self.date_col, self.segment_len_col]
                input_df = self.route_dataframe(route)
                self.calculate_kemantapan(input_df, route)

            elif self.data_type == 'AADT':  # For AADT
                self.data_columns = '*'
                input_df = self.route_dataframe(route)
                self.calculate_aadt(input_df, route)

            else:  # For deflection data (LWD, FWD, BB)
                self.data_columns = '*'
                input_df = self.route_dataframe(route)
                self.calculate_defl(input_df, route)
                pass  # Insert the calculation class method for Deflection data.

            self._add_prov_id(input_df, self.routeid_col, self.prov_column)  # Add prov column to the input df.
            self.route_status.loc[len(self.route_status)+1] = [str(route),
                                                               datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                               'Succeeded']

            # Get the survey date for each requested routes.
            # The survey date from each route is used to query the balai and provinsi table.
            if self.route_date is None:
                self.route_date = input_df.groupby(self.routeid_col).\
                    agg({self.date_col: 'max', self.prov_column: 'first'})
            else:
                next_route = input_df.groupby(self.routeid_col).agg({self.date_col: 'max', self.prov_column: 'first'})
                self.route_date = self.route_date.append(next_route)

        self.update_route_status()
        self.success_route = self._success_route()

        if len(self.success_route) > 0:
            self.add_year_semester_col()
            self.add_satker_ppk_id()
            self.add_prov_id()
            self.add_balai_id()
            self.write_summary_to_gdb()

        self.status_json = self.route_status.set_index(self.routeid_col).to_dict(orient='index')

    def calculate_kemantapan(self, input_df, route):
        """
        Used for initiating kemantapan class and calculate the summary DataFrame.
        :param input_df: Input DataFrame.
        :param route: Route being processed.
        :return:
        """
        if input_df.empty:
            self.failed_route.append(route)
            return self

        kemantapan = Kemantapan(input_df, **self.__dict__)

        if kemantapan.all_match:
            summary_table = kemantapan.summary().reset_index()
            self.summary_result = self.summary_result.append(summary_table)

        self.failed_route += kemantapan.no_match_route  # Get all the route which failed when merged to RNI.

        return self

    def calculate_aadt(self, input_df, route):
        """
        Used for initiating AADT class and calculate the daily AADT.
        :param input_df: Input DataFrame.
        :param route: Route being processed.
        :return:
        """
        if input_df.empty:
            self.failed_route.append(route)
            return self

        aadt = TrafficSummary(input_df, self.date_col, self.hour_col, self.minute_col, self.routeid_col, self.survey_direc_col,
                              self.veh_col_prefix)
        summary_table = aadt.daily_aadt()
        self.summary_result = self.summary_result.append(summary_table)

        return self

    def calculate_defl(self, input_df, route):
        """
        Used for initiating Deflection class and calculate Deflection summary.
        :param input_df: Input DataFrame.
        :param route: Route being processed.
        :return:
        """
        if input_df.empty:
            self.failed_route.append(route)
            return self

        deflection = Deflection(input_df, self.force_col, self.data_type, self.d0_col, self.d200_col, self.asp_temp)
        summary_table = deflection.sorted
        self.summary_result = self.summary_result.append(summary_table)

        return self

    def route_dataframe(self, route):
        """
        Get route DataFrame.
        :param route: Requested route (must be string).
        :return:
        """

        if (type(route) != str) and (type(route) != unicode):
            raise ("Route request should be in string.")
        else:
            df = event_fc_to_df(self.table_name, self.data_columns, route, self.routeid_col, env.workspace, True)

        return df

    def add_year_semester_col(self):
        if self.semester is not None:
            self.summary_result[self.semester_col] = pd.Series(self.semester, index=self.summary_result.index)

        self.summary_result[self.year_col] = pd.Series(self.year, index=self.summary_result.index)

        return self

    def add_satker_ppk_id(self):
        if len(self.success_route) < 1000:
            satker_df = event_fc_to_df(self.satker_ppk_route_table,
                                       [self.satker_routeid, self.satker_ppk_id, self.satker_id,
                                        self.satker_route_from_date, self.satker_route_to_date], self.success_route,
                                       self.satker_routeid, env.workspace, True, replace_null=False)
        else:
            satker_df = event_fc_to_df(self.satker_ppk_route_table,
                                       [self.satker_routeid, self.satker_ppk_id, self.satker_id,
                                        self.satker_route_from_date, self.satker_route_to_date], "ALL",
                                       self.satker_routeid, env.workspace, True, replace_null=False)
            satker_df = satker_df.loc[satker_df[self.satker_routeid].isin(self.success_route)]
            satker_df.reset_index(inplace=True)  # Reset the index after loc function.

        satker_df = self.route_date_query(satker_df, self.satker_routeid, self.satker_route_from_date,
                                          self.satker_route_to_date)[[self.satker_routeid, self.satker_ppk_id]]

        self.summary_result = self.summary_result.merge(satker_df, left_on=self.routeid_col, right_on=self.satker_routeid)

        return self

    def route_date_query(self, df, routeid_col, from_date_col, to_date_col, **kwargs):
        if kwargs.get('merge_on') is None:
            merge_on = self.routeid_col
        else:
            merge_on = kwargs.get('merge_on')

        df_joined = df.merge(self.route_date.reset_index(), left_on=routeid_col, right_on=merge_on)

        from_date_q = (df_joined[from_date_col].isnull()) | \
                      (df_joined[from_date_col] <= df_joined[self.date_col])

        to_date_q = (df_joined[to_date_col].isnull()) | \
                    (df_joined[to_date_col] > df_joined[self.date_col])

        return df.loc[from_date_q & to_date_q]

    def add_prov_id(self):
        self._add_prov_id(self.summary_result, self.routeid_col, self.prov_column)
        # self.summary[self.prov_column] = self.summary[self.routeid_col].apply(lambda x: str(x[:2]))

        return self

    @staticmethod
    def _add_prov_id(df, routeid_col, prov_column):
        df[prov_column] = df[routeid_col].apply(lambda x: str(x[:2]))

    def add_balai_id(self):
        input_provs = self.summary_result[self.prov_column].unique().tolist()
        input_routes = self.success_route

        # Get the Database table
        balai_prov_df = event_fc_to_df(self.balai_table, [self.balai_prov_balai_id, self.balai_prov_prov_id,
                                                          self.balai_prov_from_date, self.balai_prov_to_date],
                                       input_provs, self.balai_prov_prov_id, env.workspace, True, replace_null=False)

        if len(input_routes) >= 1000:
            balai_route_df = event_fc_to_df(self.balai_route_table, [self.balai_route_balai_id, self.balai_route_route_id,
                                                                     self.balai_route_from_date, self.balai_route_to_date],
                                            "ALL", self.balai_route_route_id, env.workspace, True, replace_null=False)
            balai_route_df = balai_route_df.loc[balai_route_df[self.balai_route_route_id].isin(input_routes)]
            balai_route_df.reset_index(inplace=True)
        else:
            balai_route_df = event_fc_to_df(self.balai_route_table, [self.balai_route_balai_id, self.balai_route_route_id,
                                                                     self.balai_route_from_date, self.balai_route_to_date],
                                            input_routes, self.balai_route_route_id, env.workspace, True, replace_null=False)

        # Query based on the route survey date value.
        balai_prov_df = self.route_date_query(balai_prov_df, self.balai_prov_prov_id, self.balai_prov_from_date,
                                              self.balai_prov_to_date, merge_on=self.prov_column)[
            [self.balai_prov_prov_id, self.balai_prov_balai_id]]
        balai_route_df = self.route_date_query(balai_route_df, self.balai_route_route_id, self.balai_route_from_date,
                                               self.balai_route_to_date)[[self.balai_route_route_id,
                                                                         self.balai_route_balai_id]]

        self.summary_result = self.summary_result.merge(balai_prov_df, left_on=self.prov_column, right_on=self.balai_prov_prov_id)
        self.summary_result.set_index(self.routeid_col, inplace=True)

        if not balai_route_df.empty:
            balai_route_df.rename(columns={self.balai_route_balai_id: self.balai_prov_balai_id}, inplace=True)
            balai_route_df.set_index(self.balai_route_route_id, inplace=True)

            self.summary_result.update(balai_route_df)

        self.summary_result[self.balai_prov_balai_id] = self.summary_result[self.balai_prov_balai_id].astype(int)
        self.summary_result.drop_duplicates(inplace=True)
        self.summary_result.reset_index(inplace=True)

        return self

    def update_route_status(self):
        self.route_status.loc[self.route_status[self.routeid_col].isin(self.failed_route), ['status']] = 'Failed'

        return self

    def _success_route(self):
        return self.route_status.loc[self.route_status['status'] == 'Succeeded', self.routeid_col].tolist()

    def _route_date_selection(self):
        if self.routes == 'ALL':
            routes = self.routes
        elif (type(self.routes) == unicode) or (type(self.routes) == str):
            routes = [self.routes]
        elif type(self.routes) == list:
            routes = self.routes
        elif self.routes is None:
            raise ("No Route parameter in the input JSON.")
        else:
            raise ("Route selection is neither list or string.")

        req_columns = [self.update_date_col, self.routeid_col]
        source_date = event_fc_to_df(self.table_name, req_columns, routes, self.routeid_col, env.workspace, True,
                                     sql_prefix='MAX ({0})'.format(self.update_date_col),
                                     sql_postfix='GROUP BY ({0})'.format(self.routeid_col))

        if (not self.force_update) and (Exists(self.output_table)):
            output_date = event_fc_to_df(self.output_table, req_columns, routes, self.routeid_col, env.workspace, True)
            merged = pd.merge(source_date, output_date, on=self.routeid_col, how='outer', suffixes=('_SOURCE', '_TARGET'))
            selection = merged.loc[(merged['UPDATE_DATE_SOURCE'] > merged['UPDATE_DATE_TARGET']) |
                                   (merged['UPDATE_DATE_TARGET'].isnull())]
            routes = selection[self.routeid_col].tolist()
        else:
            routes = source_date[self.routeid_col].tolist()

        self.route_status['LINKID'] = np.setdiff1d(source_date[self.routeid_col], routes)  # Update for route which is not updated.
        self.route_status['time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.route_status['status'] = 'Not updated'

        return routes

    def check_grading_column(self):
        list_fields = ListFields(self.table_name)
        fields = [f.name for f in list_fields]
        col_exist = self.grading_col in fields

        return col_exist

    def write_summary_to_gdb(self):
        col_details = dict()

        for col_name in self.summary_result.dtypes.to_dict():
            col_details[col_name] = dict()
            col_dtype = self.summary_result.dtypes[col_name]

            # Translate to GDB data type.
            if col_dtype == 'object':
                gdb_dtype = 'string'
            elif col_dtype == 'float64':
                gdb_dtype = 'double'
            elif col_dtype in ['int64', 'int32']:
                gdb_dtype = 'long'
            else:
                pass

            col_details[col_name]['dtype'] = gdb_dtype

        if self.semester is not None:
            gdb_table_writer(env.workspace, self.summary_result, self.output_table, col_details,
                             replace_key=[self.routeid_col, self.year_col, self.semester_col])
        else:
            gdb_table_writer(env.workspace, self.summary_result, self.output_table, col_details,
                             replace_key=[self.routeid_col, self.year_col])

