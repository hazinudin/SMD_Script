from SMD_Package import SMDConfigs, GetRoutes, event_fc_to_df, Kemantapan, gdb_table_writer
from SMD_Package.event_table.traffic.aadt import AADT
from arcpy import env
import json
import pandas as pd


class KemantapanService(object):
    """
    This class provide class method for multiple Kemantapan data type calculation.
    """
    def __init__(self, input_json):
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
        self.balai_route_balai_id = balai_route_fields['balai_code']
        self.balai_route_route_id = balai_route_fields['route_id']

        env.workspace = db_connection

        request_j = json.loads(input_json)

        with open(os.path.dirname(__file__)+'\\'+'kemantapan_config.json') as config_f:
            config = json.load(config_f)

        try:
            self.routes = request_j['routes']  # Mandatory JSON parameter.
            self.year = request_j['year']
            self.data_type = request_j['data_type']  # 'IRI', 'IRI_POK', 'PCI', 'PCI_POK', 'AADT'

            if self.data_type != 'AADT':
                self.lane_based = request_j['lane_based']
                self.method = request_j['method']  # 'mean', 'max', 'lane_based'

                if self.lane_based:
                    self.output_table = 'SMD.KEMANTAPAN_LKM_{0}'.format(self.data_type)
                else:
                    self.output_table = 'SMD.KEMANTAPAN_{0}'.format(self.data_type)

            else:
                self.lane_based = None
                self.method = None
                self.output_table = 'SMD.AADT_{0}'.format(self.year)

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
        self.output_suffix = None
        self.to_km_factor = None
        self.semester_col = 'SEMESTER'
        self.year_col = 'YEAR'
        self.prov_column = 'BM_PROV_ID'
        self.balai_column = 'BALAI_ID'

        # For AADT only
        self.hour_col = None
        self.minute_col = None
        self.survey_direc_col = None
        self.veh_col_prefix = None

        if self.semester is None:
            self.__dict__.update(config[str(self.data_type)][str(self.year)])
        else:
            self.__dict__.update(config[str(self.data_type)][str(self.year)+'_'+str(self.semester)])

        if self.routes == 'ALL':
            lrs_network = smd_config.table_names['lrs_network']
            get_route = GetRoutes("balai", "ALL", lrs_network, self.balai_table, self.balai_route_table)
            self.route_selection = get_route.route_list()
        elif type(self.routes) == unicode:
            self.route_selection = [self.routes]
        elif type(self.routes) == list:
            self.route_selection = self.routes
        elif self.routes is None:
            raise ("No Route parameter in the input JSON.")
        else:
            raise ("Route selection is neither list or string.")

        self.__dict__.update(request_j)  # Update the class attribute based on the input JSON.

        if self.output_suffix is not None:
            self.output_table = self.output_table + '_' + self.output_suffix

        self.summary = pd.DataFrame()  # For storing all summary result

        for route in self.route_selection:
            if self.data_type != 'AADT':
                self.data_columns = [self.routeid_col, self.from_m_col, self.to_m_col, self.lane_code_col,
                                     self.grading_col]
                input_df = self.route_dataframe(route)
                self.calculate_kemantapan(input_df)
            else:
                self.data_columns = '*'
                input_df = self.route_dataframe(route)
                self.calculate_aadt(input_df)

        self.add_year_semester_col()
        self.add_satker_ppk_id()
        self.add_prov_id()
        self.add_balai_id()

        self.write_summary_to_gdb()

    def calculate_kemantapan(self, input_df):
        """
        Used for initiating kemantapan class and calculate the summary DataFrame.
        :param input_df: Input DataFrame.
        :return:
        """
        if input_df.empty:
            return self

        kemantapan = Kemantapan(input_df, self.grading_col, self.routeid_col, self.from_m_col, self.to_m_col,
                                self.lane_code_col, self.data_type, self.lane_based, to_km_factor=self.to_km_factor,
                                agg_method=self.method)
        if kemantapan.all_match:
            summary_table = kemantapan.summary().reset_index()
            self.summary = self.summary.append(summary_table)

        return self

    def calculate_aadt(self, input_df):
        """
        Used for initiating AADT class and calculate the daily AADT.
        :param input_df: Input DataFrame.
        :return:
        """
        if input_df.empty:
            return self

        aadt = AADT(input_df, self.date_col, self.hour_col, self.minute_col, self.routeid_col, self.survey_direc_col,
                    self.veh_col_prefix)
        self.summary = aadt.daily_aadt()

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
            self.summary[self.semester_col] = pd.Series(self.semester, index=self.summary.index)

        self.summary[self.year_col] = pd.Series(self.year, index=self.summary.index)

        return self

    def add_satker_ppk_id(self):
        satker_df = event_fc_to_df(self.satker_ppk_route_table,
                                   [self.satker_routeid, self.satker_ppk_id, self.satker_id], 'ALL',
                                   self.satker_routeid, env.workspace, True)
        self.summary = self.summary.merge(satker_df, left_on=self.routeid_col, right_on=self.satker_routeid)

        return self

    def add_prov_id(self):
        self.summary[self.prov_column] = self.summary[self.routeid_col].apply(lambda x: str(x[:2]))

        return self

    def add_balai_id(self):
        input_provs = self.summary[self.prov_column].tolist()
        input_routes = self.route_selection

        balai_prov_df = event_fc_to_df(self.balai_table, [self.balai_prov_balai_id, self.balai_prov_prov_id],
                                       input_provs, self.balai_prov_prov_id, env.workspace, True)
        balai_route_df = event_fc_to_df(self.balai_route_table, [self.balai_route_balai_id, self.balai_route_route_id],
                                        input_routes, self.balai_route_route_id, env.workspace, True)

        self.summary = self.summary.merge(balai_prov_df, left_on=self.prov_column, right_on=self.balai_prov_prov_id)
        self.summary.set_index(self.routeid_col, inplace=True)

        if not balai_route_df.empty:
            balai_route_df.rename(columns={self.balai_route_balai_id: self.balai_prov_balai_id}, inplace=True)
            balai_route_df.set_index(self.balai_route_route_id, inplace=True)

            self.summary.update(balai_route_df)

        self.summary[self.balai_prov_balai_id] = self.summary[self.balai_prov_balai_id].astype(int)
        self.summary.drop_duplicates(inplace=True)
        self.summary.reset_index(inplace=True)

        return self

    def write_summary_to_gdb(self):
        col_details = dict()

        for col_name in self.summary.dtypes.to_dict():
            col_details[col_name] = dict()
            col_dtype = self.summary.dtypes[col_name]

            # Translate to GDB data type.
            if col_dtype == 'object':
                gdb_dtype = 'string'
            if col_dtype == 'float64':
                gdb_dtype = 'double'
            if col_dtype == 'int64':
                gdb_dtype = 'short'

            col_details[col_name]['dtype'] = gdb_dtype

        if self.semester is not None:
            gdb_table_writer(env.workspace, self.summary, self.output_table, col_details,
                             replace_key=[self.routeid_col, self.year_col, self.semester_col])
        else:
            gdb_table_writer(env.workspace, self.summary, self.output_table, col_details,
                             replace_key=[self.routeid_col, self.year_col])

