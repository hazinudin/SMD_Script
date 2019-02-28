"""
This script get all route from the specified code(Kode Balai or Kode Provinsi) in the input JSON and return the query
result in a form of string JSON.
"""

from arcpy import da, GetParameterAsText, SetParameterAsText, Exists, env, AddMessage
import json
import os


def output_message(status, message):
    """Create the output message JSON for geoprocessing."""
    output_dict = {
        "status": status,
        "message": message
    }

    return json.dumps(output_dict)


def results_output(status, type, results):
    """Create the results of the query."""
    results_dict = {
        "status": status,
        "type": type,
        "results": results
    }

    return json.dumps(results_dict)


class RouteFinder(object):
    def __init__(self, query_type, query_value, lrs_network, lrs_routeid, lrs_prov_code, balai_table,
                 balai_code_field, balai_prov_field):

        # Check for query value type
        if query_value == "ALL":  # If the query value is 'ALL'
            pass
        else:
            if type(query_value) == unicode:
                query_value = [str(query_value)]
            elif type(query_value) == list:
                query_value = [str(x) for x in query_value]

        self.lrs_network = lrs_network
        self.lrs_routeid = lrs_routeid
        self.lrs_prov_code = lrs_prov_code
        self.query_type = query_type
        self.string_json_output = None

        balai_prov_dict = {}  # Create a "prov": "kode_balai" dictionary
        self.balai_route_dict = {}  # Create a "prov": [list of route_id] dictionary
        self.results_list = [] # Create a list for storing the route query result for every code

        # If the query type is based on province code and not all province are requested
        if query_type == 'no_prov' and query_value != "ALL":
            sql_statement = '{0} in ({1})'.format(balai_prov_field, str(query_value).strip('[]'))
        else:
            if query_value == "ALL":  # If the requested value is all the same
                sql_statement = None
            if query_type == 'balai' and query_value != "ALL":
                sql_statement = '{0} in ({1})'.format(balai_code_field, str(query_value).strip('[]'))

        # Start the balai and prov query from the balai_prov table in geodatabase
        with da.SearchCursor(balai_table, [balai_prov_field, balai_code_field], where_clause=sql_statement,
                             sql_clause=('DISTINCT', None)) as search_cursor:
            for row in search_cursor:
                balai_prov_dict[str(row[0])] = str(row[1])

        self.balai_prov_dict = balai_prov_dict  # Return a kode balai and province relation dictionary

    def route_query(self):
        """
        This function will find the route based on requested province
        """
        balai_route_dict = {}  # Creating a "prov":"route" dictionary to map the province and route relation

        # Start iterating over the requested province
        for prov_code in self.balai_prov_dict:
            kode_balai = self.balai_prov_dict[prov_code]
            with da.SearchCursor(self.lrs_network, [self.lrs_routeid],
                                 where_clause='{0}=({1})'.format(self.lrs_prov_code, prov_code)) as search_cursor:
                if kode_balai not in balai_route_dict:
                    balai_route_dict[kode_balai] = [str(row[0]) for row in search_cursor]
                else:
                    balai_route_dict[kode_balai] += [str(row[0]) for row in search_cursor]

        self.balai_route_dict = balai_route_dict
        return self

    def create_json_output(self):
        """
        This funtion create the JSON string to be used as the output of the script
        """
        for balai in self.balai_route_dict:
            route_list = self.balai_route_dict[balai]
            result_object = {"code":str(balai), "routes":route_list}
            self.results_list.append(result_object)

        self.string_json_output = results_output("Succeeded", self.query_type, self.results_list)
        return self


# Change the directory to the SMD Script root folder
os.chdir('E:\SMD_Script')

# Get the parameter for script
inputJSON = GetParameterAsText(0)

# Load the SMD config JSON file
with open('smd_config.json') as config_f:
    config = json.load(config_f)

# Load the input JSON string as dictionary
# input JSON loaded as input_details
try:
    input_details = json.loads(inputJSON)
except TypeError:
    message = "Cannot load input string JSON, incorrect JSON format"
    SetParameterAsText(1, output_message("Failed", message))
    raise
except ValueError:
    message = "No JSON object could be decoded"
    SetParameterAsText(1, output_message("Failed", message))
    raise

# Check if the input has all the required keys
reqKeys = ['type', 'codes', 'routes']
for req_key in reqKeys:
    if req_key not in input_details:
        message = "Required key is missing from the input JSON. Missing key=[{0}]".format(req_key)
        SetParameterAsText(1, output_message("Failed", message))
        raise Exception(message)

# Define variable
queryType = input_details['type']
queryValue = input_details['codes']
lrsNetwork = config['table_names']['lrs_network']
balaiTable = config['table_names']['balai_table']
lrs_RouteID_field = 'ROUTEID'
lrs_ProvCode_field = 'NOPROP'
balaiTable_BalaiCode_field = 'NOMOR_BALAI'
balaiTable_ProvCode_field = 'NO_PROV'

env.workspace = config['smd_database']['instance']

# Check for input value validity
if queryType in ['balai', 'no_prov']:
    pass
else:
    message = "{0} is not a valid input".format(queryType)
    SetParameterAsText(1, output_message("Failed", message))
    raise Exception(message)

# Check the SDE geodatabase connection and feature class availability
for table in [balaiTable, lrsNetwork]:
    try:
        if Exists(table):
            pass
    except:
        message = "{0} does not exist".format(table)
        SetParameterAsText(1, output_message("Failed", message))
        raise

# Creating the route query request object
route_query_request = RouteFinder(queryType, queryValue, lrsNetwork, lrs_RouteID_field, lrs_ProvCode_field, balaiTable,
                                  balaiTable_BalaiCode_field, balaiTable_ProvCode_field)
route_query_request.route_query()  # Start the query process
route_query_request.create_json_output()  # Creating the string json output
SetParameterAsText(1, route_query_request.string_json_output)