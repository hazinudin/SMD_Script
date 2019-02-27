from arcpy import da, GetParameterAsText, SetParameterAsText, Exists, env
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


def route_prov_query(LRS_Network_FC, LRS_Network_FC_ProvCode, LRS_Network_FC_RouteID, ALL=True, prov_codes=None):
    """Select all route from selected province."""
    query_rslt = []

    if ALL:
        with da.SearchCursor(LRS_Network_FC, [LRS_Network_FC_ProvCode], where_clause="1=1") as prov_cursor:
            for prov_row in prov_cursor:
                prov_code = prov_row[0]
                routes = []

                # Only load route in the current province
                with da.SearchCursor(LRS_Network_FC, [LRS_Network_FC_RouteID],
                                     where_clause='{0} = {1}'.format(LRS_Network_FC_ProvCode, prov_code)) as route_cursor:
                    for route_row in route_cursor:
                        routes.append(route_row[0])

                result = {"code": prov_code, "routes": routes}

                query_rslt.append(result)
    else:

        # Only load route in the current province
        for prov_code in prov_codes:
            routes = []
            with da.SearchCursor(LRS_Network_FC, [LRS_Network_FC_RouteID],
                                 where_clause='{0} = {1}'.format(LRS_Network_FC_ProvCode, prov_code)) as route_cursor:
                for route_row in route_cursor:
                    routes.append(route_row[0])

            result = {"code": prov_code, "routes": routes}

            query_rslt.append(result)

    return query_rslt


def route_balai_query(LRS_Network_FC, balai_table, BalaiTable_Prov, BalaiTable_Code, LRS_Network_FC_ProvCode,
                      LRS_Network_FC_RouteID, ALL=True, balai_list=None):
    """Select all route from selected balai"""
    query_rslt = []

    if ALL:

        # Create a list containing all distinct Balai code
        all_balai_list = []
        with da.SearchCursor(balai_table, [BalaiTable_Code], sql_clause=('DISTINCT', None)) as All_cursor:
            for row in All_cursor:
                all_balai_list.append(row[0])

        # Loop for every balai code available
        for balai in all_balai_list:
            with da.SearchCursor(balai_table, [BalaiTable_Prov],
                                 where_clause="{0}='{1}'".format(BalaiTable_Code, balai)) as balai_cursor:
                for row in balai_cursor:
                    # Get the province code
                    prov_code = row[0]
                    routes = []
                    with da.SearchCursor(LRS_Network_FC, [LRS_Network_FC_RouteID],
                                         where_clause="{0}='{1}'".format(LRS_Network_FC_ProvCode,
                                                                         prov_code)) as route_cursor:
                        for route_row in route_cursor:
                            routes.append(route_row[0])

            result = {"code": balai, "routes": routes}

            query_rslt.append(result)
    else:

        for balai in balai_list:
            with da.SearchCursor(balai_table, [BalaiTable_Prov],
                                 where_clause="{0}='{1}'".format(BalaiTable_Code, balai)) as balai_cursor:
                for row in balai_cursor:
                    prov_code = row[0]
                    routes = []
                    with da.SearchCursor(LRS_Network_FC, [LRS_Network_FC_RouteID],
                                         where_clause="{0}='{1}'".format(LRS_Network_FC_ProvCode,
                                                                         prov_code)) as route_cursor:
                        for route_row in route_cursor:
                            routes.append(route_row[0])

            result = {"code": balai, "routes": routes}

            query_rslt.append(result)

    return query_rslt


# Change the directory to the SMD Script root folder
os.chdir('D:\SMD_Script')

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


# Start the query process
# If the query type is based on province code
if queryType == 'no_prov':

    # Get all route from all province
    if queryValue == 'ALL':
        query_result = route_prov_query(lrsNetwork, lrs_ProvCode_field, lrs_RouteID_field, ALL=True)
        SetParameterAsText(1, results_output("Succeeded", queryType, query_result))

    # If the value is not ALL then select based on defined province in the input JSON
    else:
        provList = []

        if type(queryValue) == str:
            provList.append(str(queryValue))
        else:
            for prov in queryValue:
                provList.append(str(prov))

        query_result = route_prov_query(lrsNetwork, lrs_ProvCode_field, lrs_RouteID_field, ALL=False,
                                        prov_codes=provList)
        SetParameterAsText(1, results_output("Succeeded", queryType, query_result))


# If the query is based on balai based
elif queryType == 'balai':

    # Get all route from all Balai
    if queryValue == 'ALL':
        query_result = route_balai_query(lrsNetwork, balaiTable, balaiTable_ProvCode_field, balaiTable_BalaiCode_field,
                                         lrs_ProvCode_field, lrs_RouteID_field, ALL=True)
        SetParameterAsText(1, results_output("Succeeded", queryType, query_result))

    # If the value is not all then select based on defined Balai in the input JSON
    else:
        balaiList = []

        if type(queryValue) == str:
            balaiList.append(str(queryValue))
        else:
            for prov in queryValue:
                balaiList.append(str(prov))

        query_result = route_balai_query(lrsNetwork, balaiTable, balaiTable_ProvCode_field, balaiTable_BalaiCode_field,
                                         lrs_ProvCode_field, lrs_RouteID_field, ALL=False, balai_list=balaiList)
        SetParameterAsText(1, results_output("Succeeded", queryType, query_result))