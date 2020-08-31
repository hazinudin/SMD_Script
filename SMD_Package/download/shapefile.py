from arcpy import Describe, CreateFeatureclass_management, AddField_management, da, env, PointGeometry
from SMD_Package import SMDConfigs, output_message
import json
import zipfile


class LRSShapeFile(object):

    def __init__(self, routes, outpath=env.scratchFolder):
        """
        Define LRS Network for the geometry shape source
        Segment dictionary from the segment dissolve process
        The default outpath is the env.scratchFolder
        """
        smd_config = SMDConfigs()
        self.lrs_network = smd_config.table_names['lrs_network']
        self.lrs_routeid = smd_config.table_fields['lrs_network']['route_id']
        self.lrs_routename = smd_config.table_fields['lrs_network']['route_name']
        self.lrs_from_date = smd_config.table_fields['lrs_network']['from_date']
        self.lrs_to_date = smd_config.table_fields['lrs_network']['to_date']

        self.outpath = outpath
        self.spatial_reference = Describe(self.lrs_network).spatialReference

        self.routes = routes
        self.route_list_sql = str(routes).strip('[]')
        self.polyline_output = None
        self.polyline_fc_name = None
        self.polyline_count = 0
        self.point_output = None
        self.point_fc_name = None
        self.point_count = 0
        self.csv_output = None
        self.zip_output = None

    @property
    def da_cursor(self):
        date_query = "({0} is null or {0}<=CURRENT_TIMESTAMP) and ({1} is null or {1}>CURRENT_TIMESTAMP)".\
            format(self.lrs_from_date, self.lrs_to_date)
        where_statement = "({0} IN ({1}))".format(self.lrs_routeid, self.route_list_sql)

        cursor = da.SearchCursor(self.lrs_network, ['SHAPE@', self.lrs_routeid, self.lrs_routename],
                                 where_clause=where_statement + " and " + date_query)

        return cursor

    def centerline_shp(self, feature_class_name):

        shp_name = "{0}.shp".format(feature_class_name)  # The name of the output feature class (.shp)
        self.polyline_fc_name = shp_name

        # Create new empty shapefile
        CreateFeatureclass_management(self.outpath, shp_name, geometry_type='POLYLINE', has_m='ENABLED',
                                      spatial_reference=self.spatial_reference)

        field_name_and_type = {
            'LINKID': 'TEXT',
            'ROUTE_NAME': 'TEXT',
        }

        # Insert cursor field
        insert_field = ['SHAPE@', 'LINKID', 'ROUTE_NAME']

        # Add new field to the shapefile
        for field_name in field_name_and_type:
            field_type = field_name_and_type[field_name]
            AddField_management("{0}\{1}".format(self.outpath, shp_name), field_name, field_type)

        # Create an insert cursor
        insert_cursor = da.InsertCursor("{0}\{1}".format(self.outpath, shp_name), insert_field)

        polyline_feature_count = 0  # Count inserted polyline feature

        for cursor in self.da_cursor:

            route_geom = cursor[0]  # The whole route geometry
            route_id = cursor[1]
            route_name = cursor[2]

            # Creating new row object to be inserted to the ShapeFile
            new_row = [route_geom, route_id, route_name]
            no_null = [val if val is not None else "Data tidak tersedia." for val in new_row]
            insert_cursor.insertRow(no_null)
            polyline_feature_count += 1

        self.polyline_count = polyline_feature_count
        return self

    def lrs_endpoint_shp(self, feature_class_name):
        """
        This function creates a shapefile containing the start and end point for every requested routes
        """
        shp_name = '{0}.shp'.format(feature_class_name)  # The output feature class name (.shp)
        self.point_fc_name = shp_name

        # Create a new empty shapefile
        CreateFeatureclass_management(self.outpath, shp_name, geometry_type='POINT',
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
            AddField_management("{0}\{1}".format(self.outpath, shp_name), field_name, field_type)

        # Create an insert cursor for the newly created point shapefile
        insert_cursor = da.InsertCursor("{0}\{1}".format(self.outpath, shp_name), insert_field)

        point_feature_count = 0

        # Iterate over the LRS Network feature class
        for search_row in self.da_cursor:
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

        self.point_count = point_feature_count
        return self

    def create_zipfile(self, zipfile_name, added_file=None):
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

            for addition in added_file or []:
                newzip.write('{0}/{1}'.format(self.outpath, addition), addition)

        self.zip_output = zip_output_path+'.zip'
        return self

    def output_message(self):
        """
        This function will create the output message from this class
        result_format = {"requested_routes":route_list, "polyline_fc":polyline feature class name,
        "polyline_count":count, "point_fc":point feature class name, "point_count":count}
        """
        result = {
            "requested_routes": self.routes,
            "polyline_fc": self.polyline_fc_name,
            "point_fc": self.point_fc_name
        }

        if self.polyline_count == 0 or self.point_count == 0:
            output_json_string = output_message("Empty output", result)
        else:
            output_json_string = output_message("Succeeded", result)

        return output_json_string

