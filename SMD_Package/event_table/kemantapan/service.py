from SMD_Package import SMDConfigs, GetRoutes, event_fc_to_df, Kemantapan, gdb_table_writer
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
        self.satker_route_from_date = satker_route_fields['from_date']
        self.satker_route_to_date = satker_route_fields['to_date']

        env.workspace = db_connection

        request_j = json.loads(input_json)

        with open(os.path.dirname(__file__)+'\\'+'kemantapan_config.json') as config_f:
            config = json.load(config_f)

        self.routes = request_j['routes']  # Mandatory JSON parameter.
        self.year = request_j['year']
        self.data_type = request_j['data_type']
        self.lane_based = request_j['lane_based']

        self.semester = request_j.get('semester')  # Optional parameter.
        self.table_name = None
        self.grading_column = None
        self.routeid_col = None
        self.from_m_col = None
        self.to_m_col = None
        self.lane_code_col = None
        self.output_suffix = None
        self.to_km_factor = None
        self.semester_col = 'SEMESTER'
        self.year_col = 'YEAR'

        if self.semester is None:
            self.__dict__.update(config[str(self.data_type)][str(self.year)])
        else:
            self.__dict__.update(config[str(self.data_type)][str(self.year)+'_'+str(self.semester)])

        if self.routes == 'ALL':
            lrs_network = smd_config.table_names['lrs_network']
            balai_table = smd_config.table_names['balai_table']
            balai_route_table = smd_config.table_names['balai_route_table']
            get_route = GetRoutes("balai", "ALL", lrs_network, balai_table, balai_route_table)
            self.route_selection = get_route.route_list()
        elif type(self.routes) == unicode:
            self.route_selection = [self.routes]
        elif type(self.routes) == list:
            self.route_selection = self.routes
        elif self.routes is None:
            raise ("No Route parameter in the input JSON.")
        else:
            raise ("Route selection is neither list or string.")

        if self.lane_based:
            self.output_table = 'SMD.KEMANTAPAN_LKM_{0}'.format(self.data_type)
        else:
            self.output_table = 'SMD.KEMANTAPAN_{0}'.format(self.data_type)

        self.__dict__.update(request_j)  # Update the class attribute based on the input JSON.

        if self.output_suffix is not None:
            self.output_table = self.output_table + '_' + self.output_suffix

        self.summary = pd.DataFrame()  # For storing all summary result
        for route in self.route_selection:
            self.calculate_kemantapan(route)

        self.add_year_semester_col()
        self.add_satker_ppk_id()
        self.write_summary_to_gdb()

    def calculate_kemantapan(self, route):
        """
        Used for initiating kemantapan class and calculate the summary DataFrame.
        :param route: The requested route
        :return:
        """
        input_df = self.route_dataframe(route)
        kemantapan = Kemantapan(input_df, self.grading_column, self.routeid_col, self.from_m_col, self.to_m_col,
                                self.lane_code_col, self.data_type, self.lane_based, to_km_factor=self.to_km_factor)
        if kemantapan.all_match:
            summary_table = kemantapan.summary().reset_index()
            self.summary = self.summary.append(summary_table)

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
            df = event_fc_to_df(self.table_name, [self.routeid_col, self.from_m_col, self.to_m_col, self.lane_code_col,
                                                  self.grading_column], route, self.routeid_col, env.workspace, True)

        return df

    def add_year_semester_col(self):
        if self.semester is not None:
            self.summary[self.semester_col] = pd.Series(self.semester, index=self.summary.index)

        self.summary[self.year_col] = pd.Series(self.year, index=self.summary.index)

        return self

    def add_satker_ppk_id(self):
        satker_df = event_fc_to_df(self.satker_ppk_route_table, [self.satker_routeid, self.satker_ppk_id], 'ALL',
                                   self.satker_routeid, env.workspace, True)
        self.summary = self.summary.merge(satker_df, left_on=self.routeid_col, right_on=self.satker_routeid)

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

