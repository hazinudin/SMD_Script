"""
This script get all route shape and create a new Shapefile, from the specified route/s contained in the JSON output
from GetAllRoute script.
"""

from arcpy import env, GetParameterAsText, SetParameter, da, CreateFeatureclass_management, AddField_management, \
    Describe, PointGeometry, SetParameterAsText, Exists, AddMessage
import os
import sys
import json
import zipfile
import numpy as np
from datetime import datetime
sys.path.append('E:\SMD_Script')
from SMD_Package import rni_segment_dissolve, GetRoutes, event_fc_to_df, input_json_check, output_message


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
        return all_routes_str
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
        return route_interection_list


class DictionaryToFeatureClass(object):

    def __init__(self, lrs_network, lrs_routeid, lrs_routename, segment_dict, outpath=env.scratchFolder,
                 missing_route=None, missing_msg='Data RNI tidak ditemukan'):
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
        self.missing_route = missing_route
        self.missing_msg = missing_msg

        self.polyline_output = None
        self.point_output = None
        self.csv_output = None
        self.zip_output = None

    def create_segment_polyline(self, feature_class_name):
        """
        This function gets all the shape from every segments from the LRS network feature class, then write it to a
        output shapefile.
        """
        shapefile_name = "{0}.shp".format(feature_class_name)  # The name of the output feature class (.shp)
        # Create new empty shapefile
        CreateFeatureclass_management(self.outpath, shapefile_name, geometry_type='POLYLINE', has_m='ENABLED',
                                      spatial_reference=self.spatial_reference)

        # Define field names and types for the new shapefile
        field_name_and_type = {
            'LINKID': 'TEXT',
            'STA_FROM': 'DOUBLE',
            'STA_TO': 'DOUBLE',
            'LANE_CODES': 'TEXT'
        }

        # Insert cursor field
        insert_field = ['SHAPE@', 'LINKID', 'STA_FROM', 'STA_TO', 'LANE_CODES']

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
                                 where_clause="{0} = '{1}'".format(self.lrs_routeid, route_id))as search_cursor:
                for search_row in search_cursor:

                    # Get the segment geometry based on the segment from measure and to measure
                    for segment_measurement in self.segment_dict[segment]:
                        from_m_km = segment_measurement[0]  # From measure in kilometers
                        to_m_km = segment_measurement[1]  # To measure in kilometers

                        from_m_meter = segment_measurement[0]*1000  # From measure in meters
                        to_m_meter = segment_measurement[1]*1000  # To measure in meters

                        # Geometry object of the segment
                        segment_geom_obj = search_row[0].segmentAlongLine(from_m_meter, to_m_meter)

                        # Start inserting new row to the shapefile
                        new_row = [segment_geom_obj, route_id, from_m_km, to_m_km, lane_codes]
                        insert_cursor.insertRow(new_row)
                        polyline_feature_count += 1

        # Check if there is route with missing data
        if self.missing_route is not None:
            for route in self.missing_route:
                message = self.missing_msg
                new_row = [None, route, 0, 0, message]
                insert_cursor.insertRow(new_row)
                polyline_feature_count += 1

        # Return the polyline attribute
        self.polyline_output = "{0}\{1}".format(self.outpath, shapefile_name)  # Shapefile path + name
        self.polyline_count = polyline_feature_count  # Shapefile feature count
        self.polyline_fc_name = shapefile_name  # Shapefile name

    def create_start_end_point(self, feature_class_name):
        """
        This function creates a shapefile containing the start and end point for every requested routes
        """
        shapefile_name = '{0}.shp'.format(feature_class_name)  # The output feature class name (.shp)
        # Create a new empty shapefile
        CreateFeatureclass_management(self.outpath, shapefile_name, geometry_type='POINT',
                                      spatial_reference=self.spatial_reference)

        # Define field name and type for the new shapefile
        field_name_and_type = {
            'LINKID': 'TEXT',
            'ROUTE_NAME': 'TEXT',
            'KETERANGAN': 'TEXT',
            'STA_LAT': 'TEXT',
            'STA_LONG': 'TEXT',
        }

        # Insert cursor field
        insert_field = ['SHAPE@', 'LINKID', 'ROUTE_NAME', 'KETERANGAN', 'STA_LAT', 'STA_LONG']

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
                        new_row = [end_point, route_id, route_name, 'Awal ruas', x_coord, y_coord]
                    else:
                        new_row = [end_point, route_id, route_name, 'Akhir ruas', x_coord, y_coord]

                    insert_cursor.insertRow(new_row)
                    point_feature_count += 1

        # Check if there is route with missing data
        if self.missing_route is not None:
            for route in self.missing_route:
                message = self.missing_msg
                new_row = [None, route, message, message, 0, 0]
                insert_cursor.insertRow(new_row)
                point_feature_count += 1

        # Return the point attribute
        self.point_output = "{0}\{1}".format(self.outpath, shapefile_name)  # Shapefile path + name
        self.point_feature_count = point_feature_count  # Shapefile feature count
        self.point_fc_name = shapefile_name  # Shapefile name

    def create_rni_csv(self, rni_df):
        df = rni_df  # The RNI DataFrame
        self.csv_output = "{0}/{1}".format(self.outpath, 'RNITable.csv')
        df.to_csv(self.csv_output)  # Creating the CSV file from the DataFrame

        return self

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
            output_json_string = output_message("Empty output", result)
        else:
            output_json_string = output_message("Succeeded", result)

        return output_json_string

    def create_zipfile(self, zipfile_name):
        """
        This class method will create a zipfile within the scratchFolder directory of the class.
        :return:
        """

        zip_output = zipfile_name  # The zip file name
        zip_output_path = '{0}/{1}'.format(self.outpath, zip_output)  # The zip file target directory

        with zipfile.ZipFile(zip_output_path+'.zip', 'w') as newzip:  # Creating new empty zip file
            for fc_name in [self.polyline_fc_name, self.point_fc_name]:  # Iterate over the ShapeFile output
                fc_name = fc_name.replace('.shp', '')
                # Iterate for every file component of the ShapeFile
                for file_extension in ['.cpg', '.dbf', '.shp', '.shx', '.prj']:
                    # Insert every file component to the archive
                    newzip.write('{0}/{1}'.format(self.outpath, fc_name+file_extension), fc_name+file_extension)

            newzip.write(self.csv_output, 'RNI_table.csv')

        self.zip_output = zip_output_path+'.zip'
        return self


# Change the directory
os.chdir('E:/SMD_Script')

# Get the script parameter
inputJSON = GetParameterAsText(0)

# Load the input JSON, result from GetAllRoute and config JSON
input_details = input_json_check(inputJSON, 1, req_keys=['type', 'codes', 'routes'])

with open('smd_config.json') as config_f:
    config = json.load(config_f)


# Tabel and column used in this script
lrsNetwork = config['table_names']['lrs_network']
lrsNetwork_RouteID = config['table_fields']['lrs_network']['route_id']
lrsNetwork_RouteName = config['table_fields']['lrs_network']['route_name']

balaiTable = config['table_names']['balai_table']

rniTable = config['table_names']['rni']
rniSearchField = ['LINKID', 'FROMMEASURE', 'TOMEASURE', 'LANE_CODE']
rniGroupbyField = ['LINKID', 'FROMMEASURE', 'TOMEASURE']
rniCodeLane = config['table_fields']['rni']['lane_code']
rniRouteID = config['table_fields']['rni']['route_id']
dbConnection = config['smd_database']['instance']

# Set the environment workspace
env.workspace = dbConnection
env.overwriteOutput = True

getAllRouteResult = GetRoutes(input_details['type'], input_details["codes"], lrsNetwork, balaiTable).create_json_output()
allRouteQueryResult = json.loads(getAllRouteResult)

# Define variable
requestedRoutes = input_details['routes']
outputSHPName = 'Routes'
allResults = allRouteQueryResult['results']

# Checking the existence of all required table
ConnectionCheck = SDE_TableConnection(env.workspace, [rniTable, lrsNetwork])
if ConnectionCheck.all_connected:

    # Checking the intersection between reqeusted routes and the available route
    RequestCheckResult = request_check(allResults, requestedRoutes)
    if RequestCheckResult is not None:

        # Create a Pandas dataframe from the RNI table in geodatabase
        RNI_df = event_fc_to_df(rniTable, rniSearchField, RequestCheckResult, rniRouteID, dbConnection,
                                is_table=True)
        RNI_df_rename = RNI_df.rename(columns={'FROMMEASURE': 'STA_FROM', 'TOMEASURE': 'STA_TO'})
        DissolvedSegmentDict = rni_segment_dissolve(RNI_df, rniGroupbyField, rniCodeLane, rniRouteID)

        available_rni = np.array(RNI_df_rename['LINKID'].tolist())
        request_route = np.array(RequestCheckResult)
        missing_rni = np.setdiff1d(request_route, available_rni).tolist()

        # Create the shapefile from the segment created by the dissolve segment function
        RouteGeometries = DictionaryToFeatureClass(lrsNetwork, lrsNetwork_RouteID, lrsNetwork_RouteName,
                                                   DissolvedSegmentDict, missing_route=missing_rni)

        if input_details["type"] == "balai":
            req_type = 'Balai'
        elif input_details["type"] == "no_prov":
            req_type = "Prov"
        else:
            req_type = ""

        if type(input_details["codes"]) == list:
            req_codes = str(input_details["codes"]).strip("[]").replace("'", "").replace(', ','_').replace('u', '')
        else:
            req_codes = str(input_details["codes"])

        current_year = datetime.now().year
        RouteGeometries.create_segment_polyline("SegmenRuas_"+str(current_year))  # Create the polyline shapefile
        RouteGeometries.create_start_end_point("AwalAkhirRuas_"+str(current_year))  # Create the point shapefile
        RouteGeometries.create_rni_csv(RNI_df_rename)  # Create the RNI DataFrame

        SetParameterAsText(1, RouteGeometries.output_message())
        SetParameter(2, RouteGeometries.create_zipfile("Data_{0}_{1}_2018".format(req_type, req_codes)).zip_output)

    elif RequestCheckResult is None:
        SetParameterAsText(1, output_message("Failed", "The requested route/s are not valid"))

else:
    SetParameterAsText(1, output_message("Failed", "Required table are missing.{0}".format(ConnectionCheck.missing_table)))
