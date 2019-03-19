import json
from arcpy import da


class GetRoutes(object):
    def __init__(self, query_type, query_value, lrs_network, balai_table, lrs_routeid='ROUTEID', lrs_prov_code='NOPROP',
                 balai_code='NOMOR_BALAI', balai_prov='NO_PROV'):

        # Check for query value type
        if query_value == "ALL":  # If the query value is 'ALL'
            pass
        else:
            if type(query_value) == unicode:
                query_value = [str(query_value)]
            elif type(query_value) == list:
                query_value = [str(x) for x in query_value]

        self.string_json_output = None

        balai_prov_dict = {}  # Create a "prov": "kode_balai" dictionary
        balai_route_dict = {}  # Creating a "prov":"route" dictionary to map the province and route relation

        self.balai_route_dict = {}  # Create a "prov": [list of route_id] dictionary
        self.results_list = [] # Create a list for storing the route query result for every code
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
                balai_prov_dict[str(row[0])] = str(row[1])  # Create a "prov":"kode_balai" dictionary

        # Start iterating over the requested province
        for prov_code in balai_prov_dict:
            kode_balai = balai_prov_dict[prov_code]
            with da.SearchCursor(lrs_network, [lrs_routeid],
                                 where_clause='{0}=({1})'.format(lrs_prov_code, prov_code)) as search_cursor:
                if kode_balai not in balai_route_dict:
                    balai_route_dict[kode_balai] = [str(row[0]) for row in search_cursor]
                else:
                    balai_route_dict[kode_balai] += [str(row[0]) for row in search_cursor]

        self.balai_route_dict = balai_route_dict

    def create_json_output(self):
        """
        This funtion create the JSON string to be used as the output of the script
        """
        for balai in self.balai_route_dict:
            route_list = self.balai_route_dict[balai]
            result_object = {"code":str(balai), "routes":route_list}
            self.results_list.append(result_object)

        string_json_output = self.results_output("Succeeded", self.query_type, self.results_list)
        return string_json_output

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
            for balai in self.balai_route_dict:
                route_list += self.balai_route_dict[balai]
        else:
            for code in req_balai:
                if code in self.balai_route_dict.keys():
                    route_list += self.balai_rotue_dict[code]
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