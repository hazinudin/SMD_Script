from SMD_Package import SMDConfigs, GetRoutes, event_fc_to_df
from arcpy import env
import json


class KemantapanService(object):
    """
    This class provide class method for multiple Kemantapan data type calculation.
    """
    def __init__(self, input_json, data_type, lane_based):
        """
        Class initialization.
        :param input_json: The input JSON. Contain 'routes' and 'year', optional parameter 'semester', 'table_name',
         'routeid_col', 'from_m_col', 'to_m_col', 'lane_code_col', 'output_table' and 'grading_col'.
        :param data_type: The data type (ROUGHNESS or PCI).
        :param lane_based: If True then the output calculation will be in lane based format.
        """
        import os

        if SMDConfigs.smd_dir() == '':
            pass
        else:
            os.chdir(SMDConfigs.smd_dir())  # Change directory to SMD root dir.

        smd_config = SMDConfigs()
        db_connection = smd_config.smd_database['instance']
        env.workspace = db_connection

        request_j = json.loads(input_json)

        with open(os.path.dirname(__file__)+'\\'+'kemantapan_config.json') as config_f:
            config = json.load(config_f)

        self.routes = request_j['routes']
        self.year = request_j['year']
        self.semester = request_j.get('semester')
        self.table_name = None
        self.grading_column = None
        self.route_id_col = None
        self.from_m_col = None
        self.to_m_col = None
        self.lane_code_col = None

        if self.semester is None:
            self.__dict__.update(config[str(data_type)][str(self.year)])
        else:
            self.__dict__.update(config[str(data_type)][str(self.year)+'_'+str(self.semester)])

        self.__dict__.update(request_j)  # Setting the class attribute based on the input JSON.

        if self.routes == 'ALL':
            lrs_network = smd_config.table_names['lrs_network']
            balai_table = smd_config.table_names['balai_table']
            balai_route_table = smd_config.table_names['balai_route_table']
            get_route = GetRoutes("balai", "ALL", lrs_network, balai_table, balai_route_table)
            self.route_selection = get_route.route_list()
        elif type(self.routes) == unicode:
            self.route_selection = [self.routes]
        elif type(self.routes) == list:
            pass
        elif self.routes is None:
            raise ("No Route parameter in the input JSON.")
        else:
            raise ("Route selection is neither list or string.")

        if lane_based:
            self.output_table = 'SMD.KEMANTAPAN_LKM_{0}'.format(data_type)
        else:
            self.output_table = 'SMD.KEMANTAPAN_{0}'.format(data_type)

        self.lane_based = lane_based

    def route_dataframe(self, route):
        """
        Get route DataFrame.
        :param route: Requested route (must be string).
        :return:
        """

        if (type(route) != str) or (type(route) != unicode):
            raise ("Route request should be in string")
        else:
            df = event_fc_to_df(self.table_name, [self.routeid_col, self.from_m_col, self.to_m_col, self.lane_code,
                                                  self.grading_column], route, self.routeid_col, env.workspace, True)

        return df
