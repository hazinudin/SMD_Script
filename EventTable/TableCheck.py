from arcpy import env, da, Point, PointGeometry
import numpy as np
import pandas as pd
import json


class EventTableCheck(object):
    """
    This class will be used for event table review, consist of table columns review and row by row review.
    """
    def __init__(self, event_table_path, column_details, lrs_network, db_conn):
        """
        Initialize EventTable class
        the header_check and dtype_check also called when the class is initialized
        """
        self.file_format = str(event_table_path).split('.')[1]  # Get the table file format
        if self.file_format in ['xls', 'xlsx']:

            df_self_dtype = pd.read_excel(event_table_path)
            s_converter = {col: str for col in list(df_self_dtype)}  # Create a string converters for read_excel
            del df_self_dtype

            df_string = pd.read_excel(event_table_path, converters=s_converter)
            df_string.columns = df_string.columns.str.upper()
            self.df_string = df_string  # df_string is dataframe which contain all data in string format
        else:
            self.df_string = None

        self.column_details = column_details  # Dictionary containing req col names and dtypes
        self.lrs_network = lrs_network  # Specified LRS Network feature class in SDE database
        self.sde_connection = db_conn  # The specifier gdb connection incl db version and username

        self.header_check_result = self.header_check()  # The header checking result
        self.dtype_check_result = self.dtype_check()  # The data type checking result
        self.df_valid = None  # df_valid is pandas DataFrame which has the correct data type and value for all columns
        self.error_list = []  # List for storing the error message for all checks
        self.missing_route = []  # List for storing all route which is not in the balai route domain
        self.valid_route = []  # List for storing all route which is in the balai route domain

    def header_check(self):
        """
        This function check for the input table header name and any redundant column in the input table.
        :return:
        """

        error_message = ''

        # Check the file format
        if self.df_string is not None:
            df = self.df_string  # Get the string data frame

            table_header = list(df)
            missing_column = []  # List for storing the missing columns

            # Check if the required header is not in the input header
            for req_header in self.column_details:
                if req_header not in table_header:
                    missing_column.append(str(req_header))
            if len(missing_column) != 0:
                error_message += 'Table input tidak memiliki kolom {0}.'.format(missing_column)

            # Check if the amount of header is the same as the requirement
            if len(table_header) != len(self.column_details.keys()):
                error_message += 'Table input memiliki jumlah kolom yang berlebih.'

        else:
            error_message += 'Tabel input tidak berformat .xls atau .xlsx.'

        if error_message == '':
            return None
        else:
            return reject_message(error_message)

    def dtype_check(self):
        """
        This function check the input table column data type and the data contained in that row.

        If there is a value which does not comply to the stated data type, then input table will be rejected and a
        message stating which row is the row with error.
        :return:
        """
        error_list = []

        # Run the header check method
        self.header_check()
        if self.header_check_result is None:  # If there is no problem with the header then continue

            df = self.df_string.copy(deep=True)
            for col in self.column_details:  # Iterate over every column in required col dict
                col_name = col  # Column name
                col_dtype = self.column_details[col]['dtype']  # Column data types

                if col_dtype in ["integer", "double"]:  # Check for numeric column

                    # Convert the column to numeric
                    # If the column contain non numerical value, then change that value to Null
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                    error_i = df.loc[df[col_name].isnull()].index.tolist()  # Find the index of the null

                    # If there is an error
                    if len(error_i) != 0:
                        excel_i = [x + 2 for x in error_i]
                        error_list.append('{0} memiliki nilai non-numeric pada baris{1}.'
                                          .format(col_name, str(excel_i)))

                elif col_dtype == 'date':  # Check for date column

                    # Convert the column to a date data type
                    # If the column contain an invalid date format, then change that value to Null
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce', format='d%/m%/%y')
                    error_i = df.loc[df[col_name].isnull()].index.tolist()  # Find the index of the null

                    # If there is an error
                    if len(error_i) != 0:
                        excel_i = [x + 2 for x in error_i]
                        error_list.append('{0} memiliki tanggal yang tidak sesuai dengan format pada baris{1}.'
                                          .format(col_name, str(excel_i)))

            # If the check does not detect error then return None
            if len(error_list) == 0:
                self.df_valid = df  # Assign the df (the check result) as self.df_valid
                return None
            else:
                return reject_message(error_list)

        else:
            return self.header_check_result

    def year_and_semester_check(self, year_input, semester_input, year_col='YEAR', sem_col='SEMESTER'):
        """
        This function check if the inputted data year and semester in JSON match with the data in input table
        :param year_input: The input year mentioned in the input JSON.
        :param semester_input: The input semester mentioned in the the input JSON.
        :return: the excel row index which has a mismatch year or semester data
        """
        df = self.copy_valid_df()

        # the index of row with bad val
        error_i = df.loc[
            (df[year_col] != year_input) | (df[sem_col] != semester_input)].index.tolist()

        # If  there is an error
        if len(error_i) != 0:
            excel_i = [x + 2 for x in error_i]
            error_message = '{0} atau {1} tidak sesuai dengan input ({3}/{4}) pada baris{2}.'.\
                format(year_col, sem_col, excel_i, year_input, semester_input)

            self.error_list.append(error_message)

        return self

    def route_domain(self, balai_code, balai_route_domain, routeid_col='LINKID'):
        """
        This function check if the route id submitted in the input table is in the domain of balai submitted
        :param routeid_col:
        :param balai_route:
        :return:
        """
        df = self.df_string
        input_routes = df[routeid_col].unique().tolist()  # All Route included in the input table

        for route in input_routes:
            if route not in balai_route_domain:
                self.missing_route.append(route)  # Append route which does not exist in the balai route domain
            else:
                self.valid_route.append(route)  # Append route which exist in the balai route domain

        if len(self.missing_route) != 0:
            # Create error message
            error_message = '{0} tidak ada pada domain rute balai {1}'.format(self.missing_route, balai_code)
            self.error_list.append(error_message)  # Append error message

        return self

    def value_range_check(self, lower, upper, d_column):
        """
        This function checks every value in a specified data column, to match the specified range value defined by
        parameter upper and lower (lower < [value] < upper).
        :param lower: Allowed lower bound
        :param upper: Allowed upper bound
        :param d_column: Specified data column to be checked
        :return:
        """
        df = self.copy_valid_df()

        # Get all the row with invalid value
        error_i = df.loc[(df[d_column] < lower) | (df[d_column] > upper)].index.tolist()
        excel_i = [x+2 for x in error_i]  # Create row for excel file index

        if len(error_i) != 0:
            # Create error message
            error_message = '{0} memiliki nilai yang berada di luar rentang ({1}<{0}<{2}), pada baris {3}'.\
                format(d_column, lower, upper, excel_i)
            self.error_list.append(error_message)  # Append to the error message

        return self

    def segment_len_check(self, lrs_routeid, segment_len=0.1, route_col='LINKID', from_col='STA_FR', to_col='STA_TO',
                          length_col='LENGTH'):
        """
        This function check for every segment length. The segment lenght has to be 100 meters, and stated segment length
        has to match the stated From Measure and To Measure
        :param segment_len: Required semgent legnth, the default value is 100 meters
        :param from_col: From Measure column
        :param to_col: To Measure column
        :param length_col: Segment length column
        :return:
        """
        env.workspace = self.sde_connection  # Setting up the env.workspace
        df = self.copy_valid_df()  # Create a copy of the valid DataFrame

        df[from_col] = pd.Series(df[from_col]/100)  # Convert the from measure to Km
        df[to_col] = pd.Series(df[to_col]/100)  # Convert the to measure to Km
        df['diff'] = pd.Series(df[to_col]-df[from_col])  # Create a diff column for storing from-to difference

        for route in df[route_col].unique().tolist():  # Iterate over all available row

            network_feature_found = False  # Variable for determining if the requested route exist in LRS Network
            with da.SearchCursor(self.lrs_network, 'SHAPE@', where_clause="{0}='{1}'".
                                 format(lrs_routeid, route))as cursor:
                for fc_row in cursor:
                    network_feature_found = True
                    route_geom = fc_row[0]  # Route geometry object

            if network_feature_found:
                df_route = df.loc[df[route_col] == route]  # Create a selected route DataFrame
                max_to_m = df_route.at[df_route[to_col].idxmax(), to_col]  # Get the max To measure value of a route
                lrs_max_to_m = route_geom.lastPoint.M  # Get the max measurement value for route in LRS Network

                # If the max length from survey data is less then the max measurement in LRS network
                # then create an error message
                if max_to_m < lrs_max_to_m:
                    len_diff = lrs_max_to_m - max_to_m  # Len difference is in Kilometers
                    error_message = "Data survey pada rute {0} tidak mencakup seluruh ruas. Panjang segmen yang tidak tercakup adalah {1} Kilometer".\
                        format(route, len_diff.round(3))
                    self.error_list.append(error_message)

                # Find the row with segment len error, find the index
                error_i = df_route.loc[~(np.isclose(df_route['diff'], df_route[length_col], rtol=0.001) &
                                       (np.isclose(df_route[length_col], segment_len, rtol=0.001)))].index.tolist()

            if len(error_i) != 0:
                excel_i = [x+2 for x in error_i]  # Create the index for excel table
                # Create error message
                error_message = 'Segmen pada baris {2} tidak memiliki panjang = 0.1km atau nilai {0} dan {1} tidak sesuai dengan panjang segmen'.\
                    format(from_col, to_col, excel_i)
                self.error_list.append(error_message)  # Append the error message

        return self

    def measurement_check(self, routes='ALL', from_m_col='STA_FR', to_m_col='STA_TO', route_col='LINKID'):
        """
        This function checks all event segment measurement value (from and to) for gaps, uneven increment, and final
        measurement should match the route M-value where the event is assigned to.
        :return:
        """
        env.workspace = self.sde_connection  # Setting up the env.workspace
        df = self.copy_valid_df()  # Create a valid DataFrame with matching DataType with requirement

        if routes == 'ALL':  # Only process selected routes, if 'ALL' then process all routes in input table
            pass
        else:
            df = self.selected_route_df(df, routes)

        # Sort the DataFrame based on the RouteId and FromMeasure
        df.sort_values(by=[route_col, from_m_col], inplace=True)

        # Iterate over valid row in the input table
        for route in self.valid_route:
            df_route = df.loc[df[route_col] == route, [route_col, from_m_col, to_m_col]]  # Create a route DataFrame
            df_route.reset_index(inplace=True)  # Reset the df_route index, preserve the index column

            for index, row in df_route.iterrows():
                if index == 0:
                    from_m = row[from_m_col]
                    to_m = row[to_m_col]
                else:
                    pass

    def coordinate_check(self, lrs_routeid, routes='ALL', route_col="LINKID", long_col="LONGITUDE", lat_col="LATITUDE",
                         from_m_col='STA_FR', to_m_col='STA_TO', lane_code='CODE_LANE', input_projection='4326',
                         threshold=30):
        """
        This function checks whether if the segment starting coordinate located not further than
        30meters from the LRS Network.
        :param lrs_routeid: Column in LRS Feature Class which contain the LRS Network Route ID
        :param routes: The requested routes
        :param route_col: Column in the input table which contain the route id
        :param long_col: Column in the input table which contain the longitude value
        :param lat_col: Column in the input table which contain the latitude value
        :param from_m_col: Column in the input table which contain the from measure value of a segment
        :param input_projection: The coordinate system used to project the lat and long value from the input table
        :param threshold: The maximum tolerated distance for a submitted coordinate (in meters)
        :return:
        """
        env.workspace = self.sde_connection  # Setting up the env.workspace
        df = self.copy_valid_df()
        error_i = []  # list for storing the row with error

        if routes == 'ALL':  # Only process selected routes, if 'ALL' then process all routes in input table
            route_list = df[route_col].unique().tolist()  # Create a route list
        else:
            df = self.selected_route_df(df, routes)
            route_list = df[route_col].unique().tolist()

        # Iterate for every requested routes
        for route in route_list:
            # Create a selected route DF
            df_route = df.loc[df[route_col] == route, [route_col, long_col, lat_col, from_m_col, lane_code]]

            # Acquire the LRS Network for the requested route
            with da.SearchCursor(self.lrs_network, "SHAPE@", where_clause="{0}='{1}'".
                                 format(lrs_routeid, route)) as cur:
                for s_row in cur:
                    route_geom = s_row[0]
                    route_spat_ref = route_geom.spatialReference

            for index, row in df_route.iterrows():
                point = Point(row[long_col], row[lat_col])  # Create a point object

                # Create a point geom object with WGS 1984 by default
                point_geom = PointGeometry(point).projectAs(input_projection)
                # Re-project the point geometry using the lrs spat ref
                point_geom = point_geom.projectAs(route_spat_ref)

                # The starting point of a segment in LRS
                if row[lane_code][0] == 'L':
                    measurement = row[from_m_col] * 10  # Convert the measurement value to meters (from decimeters)
                elif row[lane_code][0] == 'R':
                    measurement = row[to_m_col] * 10
                    
                ref_point = route_geom.positionAlongLine(measurement)  # Create a ref point geometry
                distance_to_ref = point_geom.distanceTo(ref_point)  # The point_geom to ref_point distance

                if distance_to_ref > threshold:
                    error_i.append(index)  # Append the index of row with coordinate error

        if len(error_i) != 0:
            excel_i = [x+2 for x in error_i]
            error_message = 'Koordinat awal pada baris {0} berjarak lebih dari {1} meter dari titik awal segmen.'.\
                format(excel_i, threshold)
            self.error_list.append(error_message)

        return self

    def copy_valid_df(self):
        """
        This function create a valid DataFrame from the dtype check class method, which ensures every column match the
        required DataType
        :return:
        """
        if self.dtype_check() is None:  # If there is a problem with the data type check then return the df_string
            df = self.df_valid
        else:
            df = self.df_string

        return df.copy(deep=True)

    @staticmethod
    def selected_route_df(df, routes, route_col="LINKID"):
        """
        This static method selects only route which is defined in the routes parameter
        :param df:
        :param routes:
        :param route_col:
        :return:
        """
        route_list = []  # List for storing all requested routes
        if type(routes) != list:
            route_list.append(routes)  # Append the requested routes to the list
        else:
            route_list = routes  # If the requested routes is in list format

        df = df.loc[df[route_col].isin(route_list)]
        return df  # Return the DataFrame with dropped invalid route


def reject_message(err_message):
    message = {
        'status': 'rejected',
        'msg': err_message
    }

    return json.dumps(message)
