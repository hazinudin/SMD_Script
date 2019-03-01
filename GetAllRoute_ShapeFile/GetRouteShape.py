"""
This script get all route shape and create a new Shapefile, from the specified route/s contained in the JSON output
from GetAllRoute script.
"""

from arcpy import env, GetParameterAsText, SetParameter, da, CreateFeatureclass_management, AddField_management, \
    Describe, PointGeometry, SetParameterAsText, Exists
import os
import json
import pandas as pd
import zipfile


def results_output(status, results):
    """Create the results of the query."""
    results_dict = {
        "message": status,
        "results": results
    }

    return json.dumps(results_dict)


class SDE_TableConnection(object):
    """
    Object for checking the requested table existence in a SDE connection
    Could be also used to check the SDE connection itself
    """
    def __init__(self, sde_connection, tables):
        """
        If all_connected is false then there is a table which does not exist in the SDE connection
        Further details about every table connection can be accessed in table_status dictionary
        :param sde_connection:
        :param tables:
        """
        env.workspace = sde_connection
        missing_table = []
        all_connected = True

        if type(tables) == list:
            for table in tables:
                if Exists(table):
                    pass
                else:
                    missing_table.append(table)
                    all_connected = False

        self.all_connected = all_connected
        self.missing_table = missing_table


def request_check(get_all_route_result, route_request_type, all_route_res_code='code', all_route_res_routes='routes'):
    """
    This function check the route request type, whether if the requested routes is in the Get All Route result, if the
    requested routes does not exist in the Get All Route result then the script will not proceed creating the shapefile
    """
    # Extract all route from the GetAllRoute script
    all_routes_str = []  # Contain all route from the GetAllRoute script in string not unicode
    for routes_and_codes in get_all_route_result:
        code = routes_and_codes[all_route_res_code]
        all_routes_unicd = routes_and_codes[all_route_res_routes]

        str_routes = [str(x) for x in all_routes_unicd]
        all_routes_str += all_routes_str + str_routes

    # Start checking the request
    if route_request_type == 'ALL':
        return str(all_routes_str).strip('[]')
    else:
        if type(route_request_type) == unicode:
            requested_routes = [(str(route_request_type))]
        if type(route_request_type) == list:
            requested_routes = [str(a) for a in route_request_type]

    # Check for any intersection between available route from Get All Result and the requested route
    route_intersection = set(all_routes_str).intersection(requested_routes)

    if len(route_intersection) == 0:
        return None  # If there is no intersection between the Get All Result and the requested routes, then return None
    else:
        route_interection_list = list(route_intersection)
        return str(route_interection_list).strip('[]')


def fc_to_dataframe(gdb_table, search_field, route_selection, route_identifier, orderby='FROMMEASURE'):
    """Create a Pandas dataframe from ArcGIS feature class/table."""

    rows = []
    table_search = da.SearchCursor(gdb_table, search_field, where_clause="LINKID IN ({0})".format(route_selection),
                                   sql_clause=(None, 'ORDER BY {0}, {1}'.format(route_identifier, orderby)))
    for row in table_search:
        rows.append(row)

    # Creating dataframe from the extracted data from RNI Tables in geodatabase
    df = pd.DataFrame(rows, columns=search_field)

    return df


def rni_segment_dissolve(df_rni, groupby_field, code_lane_field, route_id_field, from_m_field='FROMMEASURE',
                         to_m_field='TOMEASURE'):
    """
    Dissolve the segment in RNI table if the segment has a same lane code combination with the
    next segment in single route. This function return a dictionary with a groupby group as a key=(route_id, code_lane)
    and rni_groupped index as value.

    Key and Value example = (u'6201611', "[u'L1', u'L2', u'R1', u'R2']"): [[7668L, 7681L], [7683L, 7686L]]

    The group is lane from route '6201611' with lane code combination of [u'L1', u'L2', u'R1', u'R2']. This lane code
    combination for this route start from row 7668 to 7681, ending at row 7682, and start again at 7683 to 7686.
    The pattern end at 7682 because that row contains different lane code combination.
    """

    # Groupped the RNI dataframe based on groupby_field
    rni_groupped = df_rni.groupby(by=groupby_field)[code_lane_field].unique().reset_index()

    # Sort the list value in the Lane Code column
    rni_groupped[code_lane_field] = rni_groupped[code_lane_field].apply(lambda x: sorted(x))
    rni_groupped[code_lane_field] = rni_groupped[code_lane_field].astype(str)

    # Basically do another groupby to the result of the first groupby, to get the group of segment with same lane code
    lane_code_combination_groups = rni_groupped.groupby(by=[route_id_field, code_lane_field]).groups

    # Dictionary for storing the result
    dissolved_segment = {}
    # Iterate over the group of segment with same lane code combination
    for group in lane_code_combination_groups:
        segment_index_list = lane_code_combination_groups[group]
        segment_index_list = sorted(segment_index_list)

        if group not in dissolved_segment:
            dissolved_segment[group] = []

        reset_sequence = True
        # Iterate over segment within group to check the continuity of the segment with the same lane code combination
        for segment_index in segment_index_list:
            if reset_sequence:
                last_index = segment_index
                from_index = segment_index
                reset_sequence = False

                if len(segment_index_list) == 1:
                    from_measure = rni_groupped.at[from_index, from_m_field]
                    to_measure = rni_groupped.at[from_index, to_m_field]
                    from_to_measurement = [from_measure, to_measure]

                    dissolved_segment[group].append(from_to_measurement)
                    reset_sequence = True

            else:

                if segment_index - last_index == 1:
                    last_index = segment_index

                    if segment_index_list.index(segment_index) == len(segment_index_list) - 1:
                        from_measure = rni_groupped.at[from_index, from_m_field]
                        to_measure = rni_groupped.at[last_index, to_m_field]
                        from_to_measurement = [from_measure, to_measure]

                        dissolved_segment[group].append(from_to_measurement)

                elif segment_index - last_index > 1:
                    from_measure = rni_groupped.at[from_index, from_m_field]
                    to_measure = rni_groupped.at[last_index, to_m_field]
                    from_to_measurement = [from_measure, to_measure]

                    dissolved_segment[group].append(from_to_measurement)
                    from_index = segment_index
                    last_index = segment_index

    # Return the result
    return dissolved_segment


class DictionaryToFeatureClass(object):

    def __init__(self, lrs_network, lrs_routeid, lrs_routename, segment_dict, outpath=env.scratchFolder):
        """
        Define LRS Network for the geometry shape source
        Segment dictionary from the segment dissolve process
        The default outpath is the env.scratchFolder
        """
        self.lrs_network = lrs_network
        self.lrs_routeid = lrs_routeid
        self.lrs_routename = lrs_routename
        self.segment_dict = segment_dict
        self.outpath = outpath
        self.spatial_reference = Describe(lrs_network).spatialReference

        route_list = []
        for segment in self.segment_dict:
            route_list.append(str(segment[0]))
        self.route_list_sql = str(route_list).strip('[]')  # Contain route list without square bracket '01001','01002'..
        self.route_list = route_list

        self.polyline_output = None
        self.point_output = None
        self.zip_output = None

    def create_segment_polyline(self):
        """
        This function gets all the shape from every segments from the LRS network feature class, then write it to a
        output shapefile.
        """
        shapefile_name = 'Segmen_Lane.shp'
        # Create new empty shapefile
        CreateFeatureclass_management(self.outpath, shapefile_name, geometry_type='POLYLINE', has_m='ENABLED',
                                      spatial_reference=self.spatial_reference)

        # Define field names and types for the new shapefile
        field_name_and_type = {
            'ROUTEID': 'TEXT',
            'FROM_M': 'DOUBLE',
            'TO_M': 'DOUBLE',
            'LANES': 'TEXT',
            'PJG_RUAS': 'DOUBLE'
        }

        # Insert cursor field
        insert_field = ['SHAPE@', 'ROUTEID', 'FROM_M', 'TO_M', 'LANES', 'PJG_RUAS']

        # Add new field to the shapefile
        for field_name in field_name_and_type:
            field_type = field_name_and_type[field_name]
            AddField_management("{0}\{1}".format(self.outpath, shapefile_name), field_name, field_type)

        # Create an insert cursor
        insert_cursor = da.InsertCursor("{0}\{1}".format(self.outpath, shapefile_name), insert_field)

        polyline_feature_count = 0  # Count inserted polyline feature

        # Iterate over available segment group
        for segment in self.segment_dict:
            route_id = str(segment[0])
            lane_codes = segment[1].strip('[]').replace('u', '')

            # Iterate over the LRS network feature class to get the route shape geometry
            with da.SearchCursor(self.lrs_network, 'SHAPE@',
                                 where_clause="{0} = '{1}'".format(self.lrs_routeid, route_id))\
                    as search_cursor:
                for search_row in search_cursor:

                    # Get the segment geometry based on the segment from measure and to measure
                    for segment_measurement in self.segment_dict[segment]:
                        from_m_km = segment_measurement[0]  # From measure in kilometers
                        to_m_km = segment_measurement[1]  # To measure in kilometers

                        from_m_meter = segment_measurement[0]*1000  # From measure in meters
                        to_m_meter = segment_measurement[1]*1000  # To measure in meters

                        route_length = search_row[0].length  # Route geometry length in meters

                        # Geometry object of the segment
                        segment_geom_obj = search_row[0].segmentAlongLine(from_m_meter, to_m_meter)

                        # Start inserting new row to the shapefile
                        new_row = [segment_geom_obj, route_id, from_m_km, to_m_km, lane_codes, route_length]
                        insert_cursor.insertRow(new_row)
                        polyline_feature_count += 1

        # Return the polyline attribute
        self.polyline_output = "{0}\{1}".format(self.outpath, shapefile_name)  # Shapefile path + name
        self.polyline_count = polyline_feature_count  # Shapefile feature count
        self.polyline_fc_name = shapefile_name  # Shapefile name

    def create_start_end_point(self):
        """
        This function creates a shapefile containing the start and end point for every requested routes
        """
        shapefile_name = 'Titik_Awal_Akhir_Ruas.shp'
        # Create a new empty shapefile
        CreateFeatureclass_management(self.outpath, shapefile_name, geometry_type='POINT',
                                      spatial_reference=self.spatial_reference)

        # Define field name and type for the new shapefile
        field_name_and_type = {
            'ROUTEID': 'TEXT',
            'ROUTE_NAME': 'TEXT',
            'STATUS': 'TEXT',
            'START_X': 'TEXT',
            'START_Y': 'TEXT',
            'END_X': 'TEXT',
            'END_Y': 'TEXT'
        }

        # Insert cursor field
        insert_field = ['SHAPE@', 'ROUTEID', 'ROUTE_NAME', 'STATUS', 'START_X', 'START_Y', 'END_X', 'END_Y']

        # Add new field to the shapefile
        for field_name in field_name_and_type:
            field_type = field_name_and_type[field_name]
            AddField_management("{0}\{1}".format(self.outpath, shapefile_name), field_name, field_type)

        # Create an insert cursor for the newly created point shapefile
        insert_cursor = da.InsertCursor("{0}\{1}".format(self.outpath, shapefile_name), insert_field)

        point_feature_count = 0

        # Iterate over the LRS Network feature class
        with da.SearchCursor(self.lrs_network, ['SHAPE@', self.lrs_routeid, self.lrs_routename],
                             where_clause="{0} IN ({1})".format(self.lrs_routeid, self.route_list_sql)) as search_cursor:
            for search_row in search_cursor:
                # Start and end point converted to arcpy point geometry object
                start_point_geom = PointGeometry(search_row[0].firstPoint).projectAs(self.spatial_reference)
                last_point_geom = PointGeometry(search_row[0].lastPoint).projectAs(self.spatial_reference)

                for end_point in [start_point_geom, last_point_geom]:
                    # Re-project the point geometry with WGS 1984 coordinate system
                    point_gcs = end_point.projectAs('4326')
                    point_gcs_dict = json.loads(point_gcs.JSON)
                    x_coord = point_gcs_dict["x"]
                    y_coord = point_gcs_dict["y"]
                    route_id = search_row[1]
                    route_name = search_row[2]

                    if end_point == start_point_geom:
                        new_row = [end_point, route_id, route_name, 'Awal ruas', x_coord, y_coord, '', '']
                    else:
                        new_row = [end_point, route_id, route_name, 'Akhir ruas', '', '', x_coord, y_coord]

                    insert_cursor.insertRow(new_row)
                    point_feature_count += 1

        # Return the point attribute
        self.point_output = "{0}\{1}".format(self.outpath, shapefile_name)  # Shapefile path + name
        self.point_feature_count = point_feature_count  # Shapefile feature count
        self.point_fc_name = shapefile_name  # Shapefile name

    def output_message(self):
        """
        This function will create the output message from this class
        result_format = {"requested_routes":route_list, "polyline_fc":polyline feature class name,
        "polyline_count":count, "point_fc":point feature class name, "point_count":count}
        """
        result = {"requested_routes": self.route_list, "polyline_fc": self.polyline_fc_name,
                  "polyline_count": self.polyline_count, "point_fc": self.point_fc_name,
                  "point_count": self.point_feature_count}

        if self.polyline_count == 0 or self.point_feature_count == 0:
            output_json_string = results_output("Empty output", result)
        else:
            output_json_string = results_output("Succeeded", result)

        return output_json_string

    def create_zipfile(self):
        """
        This class method will create a zipfile within the scratchFolder directory of the class.
        :return:
        """

        zip_output = 'GetRouteResults'
        zip_output_path = '{0}/{1}'.format(self.outpath, zip_output)

        with zipfile.ZipFile(zip_output_path+'.zip', 'w') as newzip:
            for fc_name in [self.polyline_fc_name, self.point_fc_name]:
                fc_name = fc_name.replace('.shp', '')
                for file_extension in ['.cpg', '.dbf', '.shp', '.shx', '.prj', '.shp.xml']:
                    newzip.write('{0}/{1}'.format(self.outpath, fc_name+file_extension), fc_name+file_extension)

        self.zip_output = zip_output_path+'.zip'
        return self


# Change the directory
os.chdir('E:/SMD_Script')

# Get the script parameter
inputJSON = GetParameterAsText(0)
getAllRouteResult = GetParameterAsText(1)

# Load the input JSON, result from GetAllRoute and config JSON
input_details = json.loads(inputJSON)
allRouteQueryResult = json.loads(getAllRouteResult)
with open('smd_config.json') as config_f:
    config = json.load(config_f)

# Set the environment workspace
env.workspace = config['smd_database']['instance']
env.overwriteOutput = True

# Define variable
requestedRoutes = input_details['routes']
outputSHPName = 'Routes'
allResults = allRouteQueryResult['results']
allRouteResultsKey_code = 'code'
allRouteResultsKey_routes = 'routes'

# Tabel and column used in this script
lrsNetwork = config['table_names']['lrs_network']
lrsNetwork_RouteID = config['table_fields']['lrs_network']['route_id']
lrsNetwork_RouteName = config['table_fields']['lrs_network']['route_name']

rniTable = config['table_names']['rni']
rniSearchField = ['LINKID', 'FROMMEASURE', 'TOMEASURE', 'LANE_CODE']
rniGroupbyField = ['LINKID', 'FROMMEASURE', 'TOMEASURE']
rniCodeLane = config['table_fields']['rni']['lane_code']
rniRouteID = config['table_fields']['rni']['route_id']

# Checking the existence of all required table
ConnectionCheck = SDE_TableConnection(env.workspace, [rniTable, lrsNetwork])
if ConnectionCheck.all_connected:

    # Checking the intersection between reqeusted routes and the available route
    RequestCheckResult = request_check(allResults, requestedRoutes)
    if RequestCheckResult is not None:

        # Create a Pandas dataframe from the RNI table in geodatabase
        RNI_DataFrame = fc_to_dataframe(rniTable, rniSearchField, RequestCheckResult, rniRouteID)
        DissolvedSegmentDict = rni_segment_dissolve(RNI_DataFrame, rniGroupbyField, rniCodeLane, rniRouteID)

        # Create the shapefile from the segment created by the dissolve segment function
        RouteGeometries = DictionaryToFeatureClass(lrsNetwork, lrsNetwork_RouteID, lrsNetwork_RouteName,
                                                   DissolvedSegmentDict)
        RouteGeometries.create_segment_polyline()  # Create the polyline shapefile
        RouteGeometries.create_start_end_point()  # Create the point shapefile

        SetParameter(2, RouteGeometries.polyline_output)
        SetParameter(3, RouteGeometries.point_output)
        SetParameterAsText(4, RouteGeometries.output_message())
        SetParameter(5, RouteGeometries.create_zipfile().zip_output)

    elif RequestCheckResult is None:
        SetParameter(2, None)
        SetParameter(3, None)
        SetParameterAsText(4, results_output("Failed", None))
        SetParameter(5, None)

else:
    SetParameter(2, None)
    SetParameter(3, None)
    SetParameterAsText(4, results_output("Required table are missing.{0}".format(ConnectionCheck.missing_table), None))
    SetParameter(5, None)
