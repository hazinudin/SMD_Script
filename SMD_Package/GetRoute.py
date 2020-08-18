import json
from arcpy import da, env
from pandas import DataFrame
from SMD_Package.load_config import SMDConfigs
from SMD_Package import event_fc_to_df


class GetRoutes(object):
    """
    This Object is used for RouteID query based on requesty type either Province Code or Balai Code.
    """
    def __init__(self, query_type, query_value):
        """
        :param query_type: The query type, either 'no_prov' or 'balai'
        :param query_value: The query value.
        """
        import os

        if SMDConfigs.smd_dir() == '':
            pass
        else:
            os.chdir(SMDConfigs.smd_dir())

        smd_configs = SMDConfigs()
        env.workspace = smd_configs.smd_database['instance']

        self.route_lintas_table = smd_configs.table_names['route_lintas_table']
        self.lintas_ref_table = smd_configs.table_names['lintas_ref_table']
        self.lintas_id = smd_configs.table_fields['lintas_ref_table']['lintas_id']
        self.lintas_name = smd_configs.table_fields['lintas_ref_table']['lintas_name']

        lrs_network = smd_configs.table_names['lrs_network']
        lrs_routeid = smd_configs.table_fields['lrs_network']['route_id']
        lrs_prov_code = smd_configs.table_fields['lrs_network']['prov_code']
        lrs_route_name = smd_configs.table_fields['lrs_network']['route_name']

        balai_table = smd_configs.table_names['balai_table']
        balai_prov = smd_configs.table_fields['balai_table']['prov_code']
        balai_code = smd_configs.table_fields['balai_table']['balai_code']

        balai_route_table = smd_configs.table_names['balai_route_table']
        balai_route_routeid = smd_configs.table_fields['balai_route_table']['route_id']
        balai_route_code = smd_configs.table_fields['balai_route_table']['balai_code']

        # Check for query value type
        if query_value == "ALL":  # If the query value is 'ALL'
            pass
        else:
            if type(query_value) == unicode or type(query_value) == str:
                query_value = [str(query_value)]
            elif type(query_value) == list:
                query_value = [str(x) for x in query_value]

        self.string_json_output = None
        self.query_value = query_value
        self.lrs_routeid = lrs_routeid

        prov_balai_dict = {}  # Create a "prov": "kode_balai" dictionary
        balai_route_dict = {}  # Creating a "prov":"route" dictionary to map the province and route relation

        self.code_route_dict = {}  # Create a "prov": [list of route_id] dictionary
        self.query_type = query_type

        # If the query type is based on province code and not all province are requested
        if query_type == 'no_prov' and query_value != "ALL":
            sql_statement = '{0} in ({1})'.format(balai_prov, str(query_value).strip('[]'))
        else:
            if query_value == "ALL":  # If the requested value is all the same
                sql_statement = None
            if query_type == 'balai' and query_value != "ALL":
                sql_statement = '{0} in ({1})'.format(balai_code, str(query_value).strip('[]'))

        # Start the balai and prov query from the balai_prov table in geodatabase
        _arr = da.FeatureClassToNumPyArray(balai_table, [balai_prov, balai_code], where_clause=sql_statement)
        _df = DataFrame(_arr)

        # Create a list [{"kode_prov":prov_code, "kode_balai":balai_code}, ...]
        prov_balai_dict = _df.astype(str).to_dict(orient='records')

        # Start iterating over the requested province
        for prov_code in prov_balai_dict:
            if query_type == 'balai':
                codes = prov_code[balai_code]  # The Balai Code

                # Read the Balai-Route Table
                _arr = da.FeatureClassToNumPyArray(balai_route_table, [balai_route_code, balai_route_routeid])
                _df = DataFrame(_arr)
                in_route_map = _df[balai_route_code].isin([codes]).any()  # True if the codes exist in Balai-Route Map Table

                # If code exist Balai-Route Mapping Table.
                if in_route_map:
                    routes = _df.loc[_df[balai_route_code] == codes, balai_route_routeid].tolist()

            if query_type == 'no_prov':
                codes = prov_code[balai_prov]

            # Start accessing the LRS Network Feature Class
            if query_type == 'balai' and in_route_map:
                in_field = lrs_routeid
                search_val = str(routes).strip('[]')
            else:
                in_field = lrs_prov_code
                search_val = prov_code[balai_prov]

            where_statement = '({0} in ({1}))'.format(in_field, search_val)
            req_columns = [lrs_routeid, lrs_route_name]

            date_query = "({0} is null or {0}<=CURRENT_TIMESTAMP) and ({1} is null or {1}>CURRENT_TIMESTAMP)". \
                format("FROMDATE", "TODATE")

            with da.SearchCursor(lrs_network, req_columns,
                                 where_clause=where_statement + " and " + date_query) as search_cursor:
                if codes not in balai_route_dict:
                    balai_route_dict[codes] = [{"route_id": str(row[0]), "route_name": str(row[1])} for row in search_cursor]
                else:
                    balai_route_dict[codes] += [{"route_id": str(row[0]), "route_name": str(row[1])} for row in search_cursor]

        self.code_route_dict = balai_route_dict
        self.prov_balai_dict = prov_balai_dict

    def create_json_output(self, detailed=False):
        """
        This funtion create the JSON string to be used as the output of the script
        """
        from numpy import nan

        results_list = []
        route_lintas = self.route_lintas().set_index('route_id')

        for code in self.query_value:
            if code in self.code_route_dict:
                route_dict = self.code_route_dict[code]
                df = DataFrame.from_dict(route_dict)
                if not detailed:
                    route_list = df['route_id'].tolist()
                    result_object = {"code": str(code), "routes": route_list}
                    results_list.append(result_object)
                else:
                    df_route_id = df.set_index('route_id')
                    df_route_id = df_route_id.join(route_lintas)
                    df_route_id.replace(nan, "-", inplace=True)  # Replace NaN value with "-"
                    df_route_id.rename(columns={self.lintas_name: "lintas"}, inplace=True)
                    detailed_dict = df_route_id.T.to_dict()
                    result_object = {"code": str(code), "routes": detailed_dict}
                    results_list.append(result_object)
            else:
                result_object = {"code": str(code), "routes": "kode {0} {1} tidak valid".
                                 format(self.query_type, code)}
                results_list.append(result_object)

        return self.results_output("Succeeded", self.query_type, results_list)

    def route_list(self, req_balai='ALL'):
        """
        This function return a list from the requested codes in the __init__, if the 'req_balai' is 'ALL' then all the
        route from requested route in __init__ will be returned. If the req_balai is specified, then only the specified
        route from selected domain will be returned. req_balai has to be a member of the requested balai submitted in
        __init__ function.
        :param req_balai: the requested balai code, has to be a member of requested balai submitted in the __init__ func
        :return: a list containing all requested routes. If the requested balai is more than one, all routes will be
        merged into single list.
        """
        route_list = []
        if req_balai == 'ALL':
            for balai in self.code_route_dict:
                df = DataFrame.from_dict(self.code_route_dict[balai])
                routes = df['route_id'].tolist()
                route_list += routes
        else:
            for code in req_balai:
                if code in self.code_route_dict.keys():
                    df = DataFrame.from_dict(self.code_route_dict[code])
                    routes = df['route_id'].tolist()
                    route_list += routes
                else:
                    pass

        return route_list

    def route_lintas(self):
        values = list()

        for code in self.code_route_dict:
            values += self.code_route_dict[code]

        df = DataFrame.from_dict(values)
        routes = df["route_id"].tolist()

        route_lintas = event_fc_to_df(self.route_lintas_table, '*', routes, self.lrs_routeid, env.workspace, True)
        ref_lintas = event_fc_to_df(self.lintas_ref_table, '*', 'ALL', self.lintas_id, env.workspace, True)

        merged = df.merge(route_lintas, left_on="route_id", right_on=self.lrs_routeid)
        merged = merged.merge(ref_lintas, on=self.lintas_id)

        return merged[['route_id', self.lintas_name]]

    @staticmethod
    def results_output(status, type, results):
        """Create the results of the query."""
        results_dict = {
            "status": status,
            "type": type,
            "results": results
        }

        return json.dumps(results_dict)