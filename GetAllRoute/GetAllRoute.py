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


class RouteFinder(object):
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

        balai_route_dict = {}  # Creating a "prov":"route" dictionary to map the province and route relation

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

    @staticmethod
    def results_output(status, type, results):
        """Create the results of the query."""
        results_dict = {
            "status": status,
            "type": type,
            "results": results
        }

        return json.dumps(results_dict)


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
reqKeys = ['type', 'codes']
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

lrs_RID = 'ROUTEID'
lrs_provcode = 'NOPROP'
balaiTableBalaiCode = 'NOMOR_BALAI'
balaiTableProvCode = 'NO_PROV'

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
route_query = RouteFinder(queryType, queryValue, lrsNetwork, balaiTable, lrs_routeid=lrs_RID,
                          lrs_prov_code=lrs_provcode, balai_code=balaiTableBalaiCode, balai_prov=balaiTableProvCode)
SetParameterAsText(1, route_query.create_json_output())