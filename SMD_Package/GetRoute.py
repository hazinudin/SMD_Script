import json
from arcpy import da
from pandas import DataFrame


class GetRoutes(object):
    def __init__(self, query_type, query_value, lrs_network, balai_table, lrs_routeid='ROUTEID', lrs_prov_code='NOPROP',
                 lrs_route_name='ROUTE_NAME', lrs_lintas='ID_LINTAS', balai_code='NOMOR_BALAI', balai_prov='NO_PROV'):

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
        with da.SearchCursor(balai_table, [balai_prov, balai_code], where_clause=sql_statement,
                             sql_clause=('DISTINCT', None)) as search_cursor:
            for row in search_cursor:
                prov_balai_dict[str(row[0])] = str(row[1])  # Create a "prov":"kode_balai" dictionary

        # Start iterating over the requested province
        for prov_code in prov_balai_dict:
            if query_type == 'balai':
                codes = prov_balai_dict[prov_code]
            if query_type == 'no_prov':
                codes = prov_code
            with da.SearchCursor(lrs_network, [lrs_routeid, lrs_route_name, lrs_lintas],
                                 where_clause='{0}=({1})'.format(lrs_prov_code, prov_code)) as search_cursor:
                if codes not in balai_route_dict:
                    balai_route_dict[codes] = [{"route_id": str(row[0]), "route_name": str(row[1]), "lintas": str(row[2])} for row in search_cursor]
                else:
                    balai_route_dict[codes] += [{"route_id": str(row[0]), "route_name": str(row[1]), "lintas": str(row[2])} for row in search_cursor]

        self.code_route_dict = balai_route_dict
        self.prov_balai_dict = prov_balai_dict

    def create_json_output(self, detailed=False):
        """
        This funtion create the JSON string to be used as the output of the script
        """
        results_list = []

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

    @staticmethod
    def results_output(status, type, results):
        """Create the results of the query."""
        results_dict = {
            "status": status,
            "type": type,
            "results": results
        }

        return json.dumps(results_dict)