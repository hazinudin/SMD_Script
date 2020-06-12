from SMD_Package import SMDConfigs, GetRoutes
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

        route_request = request_j['routes']
        self.data_year = request_j['year']
        self.data_semester = request_j.get('semester')
        self.grading_column = request_j.get['grading_col']
        self.table_name = request_j.get['table_name']
        self.routeid_col = request_j.get['routeid_col']
        self.from_m_col = request_j.get['from_m_col']
        self.to_m_col = request_j.get['to_m_col']
        self.lane_code = request_j.get['lane_code_col']
        self.output_table = request_j.get['output_table']

        if route_request == 'ALL':
            lrs_network = smd_config.table_names['lrs_network']
            balai_table = smd_config.table_names['balai_table']
            balai_route_table = smd_config.table_names['balai_route_table']
            get_route = GetRoutes("balai", "ALL", lrs_network, balai_table, balai_route_table)
            self.route_selection = get_route.route_list()
        elif type(route_request) == unicode:
            self.route_selection = [route_request]
        elif type(route_request) == list:
            pass
        else:
            raise ("Route selection is neither list or string")

        if self.grading_column is None:
            if data_type == 'ROUGHNESS':  # If grading column is not defined in input JSON.
                self.grading_column = 'IRI'
            elif data_type == 'PCI':
                self.grading_column = 'PCI'
            else:
                raise ('Invalid data type request data type = {0}'.format(data_type))

        if self.table_name is None:
            if data_type == 'ROUGHNESS':  # If table name is not defined in input JSON.
                self.table_name = 'SMD.{0}_{1}_{2}'.format(data_type, self.data_semester, self.data_year)
            elif data_type =='PCI':
                self.table_name = 'SMD.{0}_{1}'.format(data_type, self.data_year)

        self.lane_based = lane_based
