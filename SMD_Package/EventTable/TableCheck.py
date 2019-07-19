from arcpy import env, da, Point, PointGeometry, AddMessage
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from SMD_Package.FCtoDataFrame import event_fc_to_df
from Kemantapan import Kemantapan
from RNITable import RNIRouteDetails


def read_input_excel(event_table_path):
    """
    This function will read the submitted excel file by the SMD user, the file format has to be '.xls' or '.xlsx', if
    any other file format is submitted then this function will return None.
    :param event_table_path: The excel file path.
    :return: Pandas DataFrame or NoneType.
    """
    file_format = str(event_table_path)[-4:]
    if file_format in ['xls', 'xlsx']:

        df_self_dtype = pd.read_excel(event_table_path)
        s_converter = {col: str for col in list(df_self_dtype)}  # Create a string converters for read_excel
        del df_self_dtype

        df_string = pd.read_excel(event_table_path, converters=s_converter)  # Convert all column to 'str' type.
        df_string.columns = df_string.columns.str.upper()  # Uppercase all the column name
        return df_string  # df_string is DataFrame which contain all data in string format
    else:
        return None


class EventValidation(object):
    """
    This class will be used for event table review, consist of table columns review and row by row review.
    """
    def __init__(self, input_df, column_details, lrs_network, lrs_routeid, db_conn):
        """
        Initialize EventTableCheck class
        the header_check and dtype_check also called when the class is initialized
        """
        self.df_string = input_df
        self.column_details = column_details  # Dictionary containing req col names and dtypes
        self.lrs_network = lrs_network  # Specified LRS Network feature class in SDE database
        self.lrs_routeid = lrs_routeid  # The LRS Network RouteID column
        self.sde_connection = db_conn  # The specifier gdb connection incl db version and username

        self.error_list = []  # List for storing the error message for all checks
        self.route_results = {}
        self.header_check_result = self.header_check()  # The header checking result
        self.dtype_check_result = self.dtype_check(write_error=True)  # The data type checking result
        self.df_valid = None  # df_valid is pandas DataFrame which has the correct data type and value for all columns
        self.missing_route = []  # List for storing all route which is not in the balai route domain
        self.valid_route = []  # List for storing all route which is in the balai route domain

    def header_check(self):
        """
        This function check for the input table header name and any redundant column in the input table.
        :return:
        """

        error_list = []

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
                error_list.append('Table input tidak memiliki kolom {0}.'.format(missing_column))

            # Check if the amount of header is the same as the requirement
            if len(table_header) != len(self.column_details.keys()):
                excess_cols = set(table_header).difference(set(self.column_details.keys()))
                error_list.append('Table input memiliki jumlah kolom yang berlebih {0}.'.format(excess_cols))

        else:
            error_list.append('Tabel input tidak berformat .xls atau .xlsx.')

        if len(error_list) == 0:
            return None
        else:
            return error_list

    def dtype_check(self, routeid_col='LINKID', write_error=True):
        """
        This function check the input table column data type and the data contained in that row.

        If there is a value which does not comply to the stated data type, then input table will be rejected and a
        message stating which row is the row with error.
        :param routeid_col: The Route ID column in the input table.
        :param write_error: If True then this method will write error message to class attribute.
        :return: self
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
                    error_row = df.loc[df[col_name].isnull(), [routeid_col, col_name]]  # Find the row with Null value

                    # If there is an error
                    if len(error_row) != 0:
                        excel_i = [x + 2 for x in error_row.index.tolist()]
                        error_message = '{0} memiliki nilai non-numeric pada baris{1}.'\
                            .format(col_name, str(excel_i))
                        error_list.append(error_message)

                        if write_error:
                            for index, row in error_row.iterrows():
                                result = 'Rute {0} pada kolom {1} memiliki nilai non-numeric pada baris {2}.'. \
                                    format(row[routeid_col], col_name, index + 2)
                                self.insert_route_message(row[routeid_col], "error", result)

                            self.error_list.append(error_message)

                elif col_dtype == 'date':  # Check for date column

                    # Convert the column to a date data type
                    # If the column contain an invalid date format, then change that value to Null
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce', format='%d/%m/%Y')
                    error_row = df.loc[df[col_name].isnull(), [routeid_col, col_name]]  # Find the row with Null value
                    error_i = error_row.index.tolist()  # Find the index of the null

                    # If there is an error
                    if len(error_i) != 0:
                        excel_i = [x + 2 for x in error_i]
                        error_message = '{0} memiliki tanggal yang tidak sesuai dengan format pada baris{1}.'\
                            .format(col_name, str(excel_i))
                        error_list.append(error_message)

                        if write_error:
                            for index, row in error_row.iterrows():
                                result = 'Rute {0} pada kolom {1} memiliki tanggal yang tidak sesuai dengan format baris{2}.'. \
                                    format(row[routeid_col], col_name, index + 2)
                                self.insert_route_message(row[routeid_col], "error", result)

                            self.error_list.append(error_message)

            self.df_valid = df  # Assign the df (the check result) as self.df_valid

            # If the check does not detect error then return None
            if len(error_list) == 0:
                return None
            else:
                return error_list

        else:
            return self.header_check_result

    def year_and_semester_check(self, year_input, semester_input, year_col='SURVEY_YEAR', sem_col='SURVEY_SMS',
                                routeid_col='LINKID', from_m_col='STA_FR', to_m_col='STA_TO', lane_code='LANE_CODE',
                                year_check_only=False):
        """
        This function check if the inputted data year and semester in JSON match with the data in input table.
        This function also check if the input year in the table is less than the current year.
        :param year_input: The input year mentioned in the input JSON.
        :param semester_input: The input semester mentioned in the the input JSON.
        :param year_col: The year column in the input table.
        :param sem_col: The semester column in the input table.
        :param routeid_col: The Route ID column in the input table.
        :param from_m_col: The From Measure column in the input table.
        :param to_m_col: The To Measure column in the input table.
        :param lane_code: The lane code column in the input table.
        :return: self
        """
        df = self.copy_valid_df()

        if df is None:  # This means no rows passed the data type check
            return None  # Return None

        # Get the current year
        cur_year = datetime.now().year

        # the index of row with bad val
        if year_check_only:
            error_row = df.loc[(df[year_col] != year_input)]
        else:
            error_row = df.loc[(df[year_col] != year_input) | (df[sem_col] != semester_input) | df[year_col] > cur_year]

        # If  there is an error
        if len(error_row) != 0:
            excel_i = [x + 2 for x in error_row.index.tolist()]
            error_message = '{0} atau {1} tidak sesuai dengan input ({3}/{4}) pada baris{2}.'.\
                format(year_col, sem_col, excel_i, year_input, semester_input)

            for index, row in error_row.iterrows():

                if year_check_only:
                    result = "Rute {0} memiliki {1} yang tidak sesuai dengan input {2} pada segmen {3}-{4} {5}.".\
                        format(row[routeid_col], year_col, year_input, row[from_m_col], row[to_m_col], row[lane_code])
                    self.insert_route_message(row[routeid_col], 'error', result)
                else:
                    result = "Rute {0} memiliki {1} atau {2} yang tidak sesuai dengan input {3}/{4} pada segmen {5}-{6} {7}.".\
                        format(row[routeid_col], year_col, sem_col, year_input, semester_input, row[from_m_col],
                               row[to_m_col], row[lane_code])
                    self.insert_route_message(row[routeid_col], 'error', result)

            return error_message  # If there is an error
        else:
            return None  # If there is no error

    def route_domain(self, balai_code, balai_route_list, routeid_col='LINKID'):
        """
        This function check if the route id submitted in the input table is in the domain of balai submitted
        :param balai_code: The balai code in the input JSON.
        :param balai_route_list: The balai route domain.
        :param routeid_col: The Route ID column in the input table.
        :return: self
        """
        df = self.df_string
        input_routes = df[routeid_col].unique().tolist()  # All Route included in the input table

        for route in input_routes:
            if route not in balai_route_list:
                self.missing_route.append(route)  # Append route which does not exist in the balai route domain
            else:
                self.valid_route.append(route)  # Append route which exist in the balai route domain

        if len(self.missing_route) != 0:
            # Create error message
            string_routes = str(self.missing_route).strip('[]')
            error_message = '{0} tidak ada pada domain rute balai {1}.'.format(string_routes, balai_code)
            self.error_list.append(error_message)  # Append error message

            for missing_route in self.missing_route:
                result = "Rute {0} tidak ada pada domain rute balai {1}.".format(missing_route, balai_code)
                self.insert_route_message(missing_route, 'error', result)

        return self

    def range_domain_check(self, routeid_col='LINKID', from_m_col='STA_FR', to_m_col='STA_TO',
                           lane_code='LANE_CODE'):
        """
        This function checks every value in a specified data column, to match the specified range value defined by
        parameter upper and lower (lower < [value] < upper).
        :param routeid_col: The Route ID column in the input table.
        :param from_m_col: The From Measure column in the input table.
        :param to_m_col: The To Measure column in the input table.
        :param lane_code: The lane code column in the input table.
        :return: self
        """
        df = self.copy_valid_df()

        for column in self.column_details.keys():
            if 'range' in self.column_details[column].keys():
                range_details = self.column_details[column]['range']
                upper_bound = range_details['upper']  # The range upper bound
                lower_bound = range_details['lower']  # The range lower bound
                eq_upper = range_details['eq_upper']  # Equal with the upper bound
                eq_lower = range_details['eq_lower']  # Equal with the lower bound
                for_review = range_details['review']  # As To Be Reviewed message or as an Error Message

                # The upper value mask
                if eq_upper:
                    upper_mask = df[column] > upper_bound
                else:
                    upper_mask = df[column] >= upper_bound

                # The lower value mask
                if eq_lower:
                    lower_mask = df[column] < lower_bound
                else:
                    lower_mask = df[column] <= lower_bound

                error_row = df.loc[lower_mask | upper_mask]  # Find the faulty row

                if len(error_row) != 0:
                    # Create error message
                    excel_i = [x + 2 for x in error_row.index.tolist()]  # Create row for excel file index
                    error_message = '{0} memiliki nilai yang berada di luar rentang ({1}<{0}<{2}), pada baris {3}'. \
                        format(column, lower_bound, upper_bound, excel_i)
                    self.error_list.append(error_message)  # Append to the error message

                    for index, row in error_row.iterrows():
                        result = "Rute {0} memiliki nilai {1} yang berada di luar rentang ({2}<{1}<{3}), pada segmen {4}-{5} {6}". \
                            format(row[routeid_col], column, lower_bound, upper_bound, row[from_m_col], row[to_m_col],
                                   row[lane_code])

                        # Insert the error message depend on the message status (as an Error or Review)
                        if for_review:
                            self.insert_route_message(row[routeid_col], 'ToBeReviewed', result)
                        else:
                            self.insert_route_message(row[routeid_col], 'error', result)

            if 'domain' in self.column_details[column].keys():
                val_domain = self.column_details[column]['domain']  # The domain list
                error_row = df.loc[~df[column].isin(val_domain)]  # Find the faulty row

                if len(error_row) != 0:
                    for index, row in error_row.iterrows():
                        result = "Rute {0} memiliki nilai {1} yang tidak termasuk di dalam domain, pada segmen {2}-{3} {4}.".\
                            format(row[routeid_col], column, row[from_m_col], row[to_m_col], row[lane_code])
                        self.insert_route_message(row[routeid_col], 'error', result)

        return self

    def segment_len_check(self, routes='ALL', segment_len=0.1, routeid_col='LINKID', from_m_col='STA_FR',
                          to_m_col='STA_TO', lane_code='LANE_CODE', length_col='SEGMENT_LENGTH'):
        """
        This function check for every segment length. The segment length has to be 100 meters, and stated segment length
        has to match the stated From Measure and To Measure.
        :param segment_len: Required segment length, the default value is 100 meters
        :param from_m_col: From Measure column
        :param to_m_col: To Measure column
        :param length_col: Segment length column
        :param routes: The specified routes to be processed, if 'ALL' then all route in the input table
        will be processed.
        :param routeid_col: The Route ID column in the input table.
        :param lane_code: The lane code column in the input table.
        :return: self
        """
        env.workspace = self.sde_connection  # Setting up the env.workspace
        df = self.copy_valid_df()  # Create a copy of the valid DataFrame

        df[from_m_col] = pd.Series(df[from_m_col] / 100)  # Convert the from measure to Km
        df[to_m_col] = pd.Series(df[to_m_col] / 100)  # Convert the to measure to Km
        df['diff'] = pd.Series(df[to_m_col] - df[from_m_col])  # Create a diff column for storing from-to difference

        if routes == 'ALL':
            pass
        else:
            df = self.selected_route_df(df, routes, routeid_col=routeid_col)

        for route, lane in self.route_lane_tuple(df, routeid_col, lane_code):  # Iterate over all route and lane

                df_route_lane = df.loc[(df[lane_code] == lane) & (df[routeid_col] == route)]
                max_to_ind = df_route_lane[to_m_col].idxmax()
                last_segment_len = df_route_lane.at[max_to_ind, 'diff']  # The last segment real length
                last_segment_statedlen = df_route_lane.at[max_to_ind, length_col]  # The last segment stated len
                last_from = df_route_lane.at[max_to_ind, from_m_col]*100  # Last segment from measure in Decameters
                last_to = df_route_lane.at[max_to_ind, to_m_col]*100  # Last segment to measure in Decameters
                last_interval = '{0}-{1}'.format(last_from, last_to)  # The interval value in string

                # Find the row with segment len error, find the index
                error_i = df_route_lane.loc[~(np.isclose(df_route_lane['diff'], df_route_lane[length_col], rtol=0.001) &
                                            (np.isclose(df_route_lane[length_col], segment_len, rtol=0.001)))].index

                # Pop the last segment from the list of invalid segment
                error_i_pop_last = np.setdiff1d(error_i, max_to_ind)

                if len(error_i_pop_last) != 0:
                    excel_i = [x+2 for x in error_i_pop_last]  # Create the index for excel table
                    # Create error message
                    error_message = 'Segmen pada baris {2} tidak memiliki panjang = {3}km atau nilai {0} dan {1} tidak sesuai dengan panjang segmen.'.\
                        format(from_m_col, to_m_col, excel_i, segment_len)
                    self.error_list.append(error_message)  # Append the error message
                    self.insert_route_message(route, 'error', error_message)

                # Check whether the last segment fulfill the check criteria (length should not exceed 'segment_len')
                if last_segment_len > segment_len:
                    # Create error message
                    error_message = 'Segmen akhir di rute {0} pada lane {1} memiliki panjang yang lebih dari {2}km'.\
                        format(route, lane, last_segment_statedlen)
                    self.error_list.append(error_message)
                    self.insert_route_message(route, 'error', error_message)

                # Check whether the stated length for the last segment match the actual length
                if not np.isclose(last_segment_len, last_segment_statedlen, rtol=0.001):
                    # Create error message
                    error_message = 'Segmen akhir {0} di rute {1} pada lane {2} memiliki panjang yang berbeda dengan yang tertera pada kolom {3}'.\
                        format(last_interval, route, lane, length_col)
                    self.error_list.append(error_message)
                    self.insert_route_message(route, 'error', error_message)

        return self

    def measurement_check(self, rni_table, rni_routeid, rni_to_m, routes='ALL', from_m_col='STA_FR', to_m_col='STA_TO',
                          routeid_col='LINKID', lane_code='LANE_CODE', compare_to='RNI'):
        """
        This function checks all event segment measurement value (from and to) for gaps, uneven increment, and final
        measurement should match the route M-value where the event is assigned to.
        :return:
        """
        env.workspace = self.sde_connection  # Setting up the env.workspace
        df = self.copy_valid_df()  # Create a valid DataFrame with matching DataType with requirement
        groupby_cols = [routeid_col, from_m_col, to_m_col]

        if routes == 'ALL':  # Only process selected routes, if 'ALL' then process all routes in input table
            pass
        else:
            df = self.selected_route_df(df, routes)

        # Iterate over valid row in the input table
        for route in self.route_lane_tuple(df, routeid_col, lane_code, route_only=True):
            # Create a route DataFrame
            df_route = df.loc[df[routeid_col] == route, [routeid_col, from_m_col, to_m_col, lane_code]]
            df_groupped = df_route.groupby(by=groupby_cols)[lane_code].unique().\
                reset_index()  # Group the route df

            # Sort the DataFrame based on the RouteId and FromMeasure
            df_groupped.sort_values(by=[routeid_col, from_m_col, to_m_col], inplace=True)
            df_groupped.reset_index(drop=True)

            max_to_ind = df_groupped[to_m_col].idxmax()  # The index of segment with largest To Measure
            max_to_meas = float(df_groupped.at[max_to_ind, to_m_col]) / 100  # The largest To Measure value

            # Comparison based on the 'compare_to' parameter
            if compare_to == 'RNI':
                # Get the RNI Max Measurement
                rni_df = event_fc_to_df(rni_table, [rni_routeid, rni_to_m], route, rni_routeid, self.sde_connection,
                                        is_table=False, include_all=True, orderby=None)  # The RNI DataFrame

                if len(rni_df) == 0:  # If the RNI Table does not exist for a route
                    comparison = None  # The comparison value will be None
                else:
                    rni_max_m = rni_df.at[rni_df[rni_to_m].argmax(), rni_to_m]  # The Route RNI maximum measurement
                    comparison = rni_max_m

            if compare_to == 'LRS':
                # Get the LRS Network route length
                lrs_route_len = self.route_geometry(route, self.lrs_network, self.lrs_routeid).lastPoint.M
                comparison = lrs_route_len

            # If the comparison value is not available.
            if comparison is None:
                pass

            # If the largest To Measure value is less than the selected comparison then there is a gap at the end
            elif (max_to_meas < comparison) and not(np.isclose(max_to_meas, comparison, rtol=0.01)):
                # Create an error message
                error_message = 'Tidak ditemukan data survey pada rute {0} dari Km {1} hingga {2}. (Terdapat gap di akhir ruas)'.\
                    format(route, max_to_meas, comparison)
                self.error_list.append(error_message)
                self.insert_route_message(route, 'error', error_message)

            for index, row in df_groupped.iterrows():

                if index == 0:  # Initialize the from_m and to_m value with the first row of a route
                    from_m = row[from_m_col]
                    to_m = row[to_m_col]
                    if from_m != 0:
                        error_message = 'Data survey pada rute {0} tidak dimulai dari 0.'.format(route)
                        self.error_list.append(error_message)
                        self.insert_route_message(route, 'error', error_message)
                else:
                    # Make sure the from measure is smaller than to measure, and
                    # the next row from measure is the same as previous row to measure (no gaps).
                    if (row[from_m_col] < row[to_m_col]) & (np.isclose(to_m, row[from_m_col], rtol=0.01)):
                        # This means OK
                        # Rewrite the To Measure and From Measure variable
                        from_m = row[from_m_col]
                        to_m = row[to_m_col]

                    elif row[from_m_col] > row[to_m_col]:
                        # Create an error message
                        error_message = 'Segmen {0}-{1} pada rute {2} memiliki arah segmen yang terbalik, {3} > {4}.'.\
                            format(row[from_m_col], row[to_m_col], route, from_m_col, to_m_col)
                        self.error_list.append(error_message)
                        self.insert_route_message(route, 'error', error_message)
                        # Rewrite the To Measure and From Measure variable
                        to_m = row[from_m_col]
                        from_m = row[to_m_col]

                    elif not np.isclose(to_m, row[from_m_col], rtol=0.01):
                        if to_m < row[from_m_col]:
                            # Create an error message
                            error_message = 'Tidak ditemukan data survey pada rute {0} dari Km {1} hingga {2}. (Terdapat gap di tengah ruas)'.\
                                format(route, float(to_m)/100, float(row[from_m_col])/100)
                            self.error_list.append(error_message)
                            self.insert_route_message(route, 'error', error_message)
                            # Rewrite the To Measure and From Measure variable

                        if to_m > row[from_m_col]:
                            # Create an error message
                            error_message = 'Terdapat tumpang tindih antara segmen {0}-{1} dengan {2}-{3} pada rute {4}'.\
                                format(from_m, to_m, row[from_m_col], row[to_m_col], route)
                            self.error_list.append(error_message)
                            self.insert_route_message(route, 'error', error_message)

                        # Rewrite the To Measure and From Measure variable
                        from_m = row[from_m_col]
                        to_m = row[to_m_col]

        return self

    def coordinate_check(self, routes='ALL', routeid_col="LINKID", long_col="STATO_LONG", lat_col="STATO_LAT",
                         from_m_col='STA_FR', to_m_col='STA_TO', lane_code='LANE_CODE', input_projection='4326',
                         threshold=30, at_start=True):
        """
        This function checks whether if the segment starting coordinate located not further than
        30meters from the LRS Network.
        :param routes: The requested routes
        :param routeid_col: Column in the input table which contain the route id
        :param long_col: Column in the input table which contain the longitude value
        :param lat_col: Column in the input table which contain the latitude value
        :param from_m_col: The From Measure column in the input table.
        :param to_m_col: The To Measure column in the input table.
        :param lane_code: The lane code column in the input table.
        :param input_projection: The coordinate system used to project the lat and long value from the input table
        :param threshold: The maximum tolerated distance for a submitted coordinate (in meters)
        :param at_start: If True then the inputted coordinate is assumed to be generated at the beginning of a segment.
        :return:
        """
        env.workspace = self.sde_connection  # Setting up the env.workspace
        df = self.copy_valid_df()
        df['measureOnLine'] = pd.Series(np.nan, dtype=np.float)  # Create a new column for storing coordinate m-value
        error_i = []  # list for storing the row with error

        if routes == 'ALL':  # Only process selected routes, if 'ALL' then process all routes in input table
            pass
        else:
            df = self.selected_route_df(df, routes)

        # Iterate for every requested routes
        for route in self.route_lane_tuple(df, routeid_col, lane_code, route_only=True):
            # Create a selected route DF
            df_route = df.loc[df[routeid_col] == route, [routeid_col, long_col, lat_col, from_m_col, to_m_col, lane_code]]

            route_geom = self.route_geometry(route, self.lrs_network, self.lrs_routeid)
            route_spat_ref = route_geom.spatialReference
            route_max_m = route_geom.lastPoint.M

            # Iterate over all available segment in the route
            for index, row in df_route.iterrows():
                point = Point(row[long_col], row[lat_col])  # Create a point object

                # Create a point geom object with WGS 1984 by default
                point_geom = PointGeometry(point).projectAs(input_projection)
                # Re-project the point geometry using the lrs spat ref
                point_geom = point_geom.projectAs(route_spat_ref)

                if at_start:
                    # The starting point of a segment in LRS
                    if row[lane_code][0] == 'L':
                        measurement = row[from_m_col] * 10  # Convert the measurement value to meters (from decimeters)
                    elif row[lane_code][0] == 'R':
                        measurement = row[to_m_col] * 10
                else:
                    # The end point of a segment in LRS
                    if row[lane_code][0] == 'L':
                        measurement = row[to_m_col] * 10  # Convert the measurement value to meters (from decimeters)
                    elif row[lane_code][0] == 'R':
                        measurement = row[from_m_col] * 10

                if measurement/1000 > route_max_m:  # If the measurement value is beyond the route max m then pass
                    pass
                else:
                    ref_point = route_geom.positionAlongLine(measurement)  # Create a ref point geometry
                    distance_to_ref = point_geom.distanceTo(ref_point)  # The point_geom to ref_point distance
                    df_route.loc[index, 'measureOnLine'] = route_geom.measureOnLine(point_geom)  # Insert the m-value

                    if distance_to_ref > threshold:
                        error_i.append(index)  # Append the index of row with coordinate error

                        if at_start:
                            error_message = 'Koordinat awal segmen {0}-{1} di lajur {2} pada rute {3} berjarak lebih dari {4} meter dari titik awal segmen.'.\
                                format(row[from_m_col], row[to_m_col], row[lane_code], route, threshold)
                            self.error_list.append(error_message)
                            self.insert_route_message(row[routeid_col], 'error', error_message)

                        if not at_start:
                            error_message = 'Koordinat awal segmen {0}-{1} di lajur {2} pada rute {3} berjarak lebih dari {4} meter dari titik akhir segmen.'.\
                                format(row[from_m_col], row[to_m_col], row[lane_code], route, threshold)
                            self.error_list.append(error_message)
                            self.insert_route_message(row[routeid_col], 'error', error_message)

            for lane in df_route[lane_code].unique().tolist():
                df_lane = df_route.loc[df_route[lane_code] == lane]  # Create a DataFrame for every available lane
                df_lane.sort_values(by=[from_m_col, to_m_col], inplace=True)  # Sort the DataFrame
                monotonic_check = np.diff(df_lane['measureOnLine']) > 0
                check_unique = np.unique(monotonic_check)

                if check_unique.all():  # Check whether the result only contain True
                    pass  # This means OK
                elif len(check_unique) == 1:  # Else if only contain one value, then the result is entirely False
                    error_message = 'Lajur {0} pada rute {1} memiliki arah survey yang terbalik.'.format(lane, route)
                    self.error_list.append(error_message)
                    self.insert_route_message(route, 'error', error_message)
                else:  # If not entirely False then give the segment which has the faulty measurement
                    faulty_index = np.where(monotonic_check is False)  # Get the index of the faulty segment
                    faulty_segment = df_lane.loc[faulty_index]  # DataFrame of all faulty segment

                    for index, row in faulty_segment.iterrows():  # Iterate for all available faulty segment
                        from_meas = row[from_m_col]
                        to_meas = row[to_m_col]
                        error_message = 'Segmen {0}-{1} pada lane {1} di rute {2} memiliki arah survey yang tidak monoton.'.\
                            format(from_meas, to_meas, lane, route)
                        self.error_list.append(error_message)
                        self.insert_route_message(route, 'error', error_message)

        return self

    def lane_code_check(self, rni_table, routes='ALL', routeid_col='LINKID', lane_code='LANE_CODE', from_m_col='STA_FR',
                        to_m_col='STA_TO', rni_route_col='LINKID', rni_from_col='FROMMEASURE', rni_to_col='TOMEASURE',
                        rni_lane_code='LANE_CODE', find_no_match=False):
        """
        This function checks the lane code combination for all segment in the input table, the segment interval value
        has to be the same with interval value in the RNI Table.
        :param rni_table: RNI event table
        :param routes: requested routes, if 'ALL' then all routes in the input table will be processed
        :param lane_code: lane code column in the input table
        :param routeid_col: The Route ID column in the input table.
        :param from_m_col: Column in the input table which contain the From Measurement value.
        :param to_m_col: Column in the input table which  contain the To Measurement value.
        :param rni_route_col: The Route ID column in the RNI Table.
        :param rni_from_col: The From Measure column in the RNI Table.
        :param rni_to_col: The To Measure column in the RNI Table.
        :param rni_lane_code: The lane code column in the RNI Table.
        :param find_no_match: If True this method will create an error message if there is unmatched interval in the
        input table.
        :return:
        """
        df = self.copy_valid_df()  # Get a copy of valid DataFrame
        df[from_m_col] = pd.Series(df[from_m_col]).astype(int)
        df[to_m_col] = pd.Series(df[to_m_col]).astype(int)
        env.workspace = self.sde_connection  # Setting up the SDE Connection workspace

        if routes == 'ALL':  # If 'ALL' then process all available route in input table
            pass
        else:
            # Else then only process the selected routes
            df = self.selected_route_df(df, routes)

        # Iterate over all requested routes
        for route in self.route_lane_tuple(df, routeid_col, lane_code, route_only=True):
            df_route = df.loc[df[routeid_col] == route]  # Create a DataFrame containing only selected routes

            # The RNI DataFrame
            search_field = [rni_route_col, rni_from_col, rni_to_col, rni_lane_code]
            df_rni = event_fc_to_df(rni_table, search_field, route, rni_route_col, self.sde_connection, is_table=True,
                                    orderby=None)
            df_rni[rni_from_col] = pd.Series(df_rni[rni_from_col]*100).round(2).astype(int)
            df_rni[rni_to_col] = pd.Series(df_rni[rni_to_col]*100).round(2).astype(int)

            if len(df_rni) == 0:  # Check if the route exist in the RNI Table
                error_message = "Ruas {0} tidak terdapat pada table RNI.".format(route)  # Create an error message
                self.error_list.append(error_message)
                self.insert_route_message(route, 'error', error_message)
            else:
                # Create the join key for both DataFrame
                input_merge_key = [routeid_col, from_m_col, to_m_col]
                rni_merge_key = [rni_route_col, rni_from_col, rni_to_col]

                # Create a groupby DataFrame
                input_groupped = df_route.groupby(by=input_merge_key)[lane_code].unique().reset_index()
                rni_groupped = df_rni.groupby(by=rni_merge_key)[rni_lane_code].unique().reset_index()

                # Start the merge process between the input table
                df_merge = pd.merge(input_groupped, rni_groupped, how='outer', left_on=input_merge_key,
                                    right_on=rni_merge_key, indicator=True, suffixes=['_INPUT', '_RNI'])
                df_both = df_merge.loc[df_merge['_merge'] == 'both']  # Interval found on both input table and RNI
                df_input_only = df_merge.loc[df_merge['_merge'] == 'left_only']  # Interval found only on the input

                if len(df_input_only) != 0 and find_no_match:
                    missing_segments = df_input_only.groupby(by=[from_m_col, to_m_col]).groups.keys()
                    str_segment = [str(x).replace(', ', '-') for x in missing_segments]
                    error_message = "Segmen di rute {0} pada interval {1} tidak memiliki pasangan pada table RNI.".\
                        format(route, str(str_segment).strip('[]'))
                    self.error_list.append(error_message)
                    self.insert_route_message(route, 'error', error_message)

                # Modify the lane_code variable
                if lane_code == rni_lane_code:
                    lane_code = lane_code+'_INPUT'
                    rni_lane_code = rni_lane_code+'_RNI'

                # Create a column containing intersection count of lane code combination
                # between the input table and RNI Table
                df_both.loc[:, 'lane_intersect_count'] = pd.Series([len(set(a).intersection(b)) for a, b in
                                                                    zip(df_both[lane_code], df_both[rni_lane_code])],
                                                                   index=df_both.index)

                # Create a column containing the lane diff of Input - RNI
                df_both.loc[:, 'input-RNI'] = pd.Series([np.setdiff1d(a, b) for a, b in
                                                         zip(df_both[lane_code], df_both[rni_lane_code])],
                                                        index=df_both.index)

                # Create a column containing the lane diff of RNI - Input
                df_both.loc[:, 'RNI-input'] = pd.Series([np.setdiff1d(b, a) for a, b in
                                                         zip(df_both[lane_code], df_both[rni_lane_code])],
                                                        index=df_both.index)

                df_both[from_m_col] = pd.Series(df_both[from_m_col]).astype(str)
                df_both[to_m_col] = pd.Series(df_both[to_m_col]).astype(str)
                df_both['segment'] = pd.Series((df_both[from_m_col])+'-'+(df_both[to_m_col]))
                df_both.set_index(['segment'], inplace=True)  # Make segment as the index

                # Zero match mean there is no intersection between input and RNI segment
                zero_match_i = df_both.loc[df_both['lane_intersect_count'] == 0].index.tolist()
                for i in zero_match_i:
                    error_message = 'Segmen {0} pada rute {1} memiliki kombinasi lane yang tidak cocok dengan RNI.'.\
                        format(i, route)
                    self.error_list.append(error_message)
                    self.insert_route_message(route, 'error', error_message)

                # 1st partial match case is where there is a partial match and input have excess lane
                # and also missing lane
                partial_1st = df_both.loc[(df_both['lane_intersect_count'] != 0) &
                                          (df_both['RNI-input'].str.len() != 0) &
                                          (df_both['input-RNI'].str.len() != 0)].index.tolist()
                for i in partial_1st:
                    excess_lane = [str(x) for x in df_both.at[i, 'input-RNI']]
                    missing_lane = [str(x) for x in df_both.at[i, 'RNI-input']]
                    error_message = 'Segmen {0] pada rute {1} tidak memiliki lane {2} dan memiliki lane {3} yang tidak terdapat pada tabel RNI.'.\
                        format(i, route, missing_lane, excess_lane)
                    self.error_list.append(error_message)
                    self.insert_route_message(route, 'error', error_message)

                # 2nd partial match case is where there is a partial match and input have excess lane
                partial_2nd = df_both.loc[(df_both['lane_intersect_count'] != 0) &
                                          (df_both['RNI-input'].str.len() == 0) &
                                          (df_both['input-RNI'].str.len() != 0)].index.tolist()
                for i in partial_2nd:
                    excess_lane = [str(x) for x in df_both.at[i, 'input-RNI']]
                    error_message = 'Lane {0} pada segmen {1} di rute {2} tidak terdapat pada tabel RNI.'.\
                        format(excess_lane, i, route)
                    self.error_list.append(error_message)
                    self.insert_route_message(route, 'error', error_message)

                # 3rd partial match case is where there is a partial match and input have a missing lane
                partial_3rd = df_both.loc[(df_both['lane_intersect_count'] != 0) &
                                          (df_both['RNI-input'].str.len() != 0) &
                                          (df_both['input-RNI'].str.len() == 0)].index.tolist()
                for i in partial_3rd:
                    missing_lane = [str(x) for x in df_both.at[i, 'RNI-input']]
                    error_message = 'Segmen {0} pada rute {1} tidak memiliki lane {2}.'. \
                        format(i, route, missing_lane)
                    self.error_list.append(error_message)
                    self.insert_route_message(route, 'error', error_message)

        return self

    def lane_direction_check(self, routes='ALL', routeid_col='LINKID', lane_code='LANE_CODE', from_m_col='STA_FR',
                             to_m_col='STA_TO', direction_col='SURVEY_DIREC',):
        """
        This class method will check for consistency between the stated lane and the direction. The Left lane e.g(L1,
        L2, L3, etc) should has a N(Normal) direction. Meanwhile, the Right lane e.g(R1, R2, R3, etc) should has a
        O(Opposite) direction.
        :param routes: The routes that will be checked.
        :param routeid_col: The RouteID column in the input DataFrame.
        :param lane_code: The Lane Code column in the input DataFrame.
        :param from_m_col: The From Measure column in the input DataFrame.
        :param to_m_col: The To Measure column in the input DataFrame.
        :param direction_col: The direction column in the input DataFrame.
        :return:
        """
        df = self.copy_valid_df()  # Create a valid DataFrame copy

        # If there is a route selection request
        if routes == 'ALL':
            pass
        else:
            df = self.selected_route_df(df, routes)

        # The False row
        false_row_l = df.loc[(df[lane_code].str.startswith("L")) & (df[direction_col] == "O")]  # The L lane with O dir
        false_row_r = df.loc[(df[lane_code].str.startswith("R")) & (df[direction_col] == "N")]  # The R lane with N dir
        false_row = pd.concat([false_row_l, false_row_r])  # All false row

        for index, row in false_row.iterrows():  # Iterate over all row in the False DataFrame.
            route = row[routeid_col]
            from_m = row[from_m_col]
            to_m = row[to_m_col]
            lane = row[lane_code]

            message = "{0} pada segmen {1}-{2} {3} memiliki arah yang tidak konsisten dengan kode lajur.".\
                format(route, from_m, to_m, lane)
            self.insert_route_message(route, 'error', message)  # Insert the route's error message.

        return self

    def rni_availability(self, rni_table, routes='ALL', routeid_col='LINKID', rni_route_col='LINKID'):
        """
        This class method will check the route availability in the RNI Event Table.
        :param routes: The route selection to be processed.
        :param routeid_col: The RouteID column of the input table.
        :param rni_route_col: The RNI Table RouteID column.
        :return:
        """
        df = self.copy_valid_df()  # Create a valid DataFrame copy

        # If there is a route selection request
        if routes == 'ALL':
            pass
        else:
            df = self.selected_route_df(df, routes)

        input_routes = df[routeid_col]  # The routes in the input table in np.array format
        rni_df = event_fc_to_df(rni_table, rni_route_col, input_routes.tolist(), rni_route_col, self.sde_connection,
                                is_table=True, orderby=None)  # The input routes in RNI Table DF in np.array format
        rni_routes = np.array(rni_df[rni_route_col])  # The available routes in the RNI Table in np.array
        missing_route = np.setdiff1d(input_routes, rni_routes)  # The unavailable route in the RNI Table

        for route in self.missing_route:
            message = "Rute {0} belum memiliki data RNI.".format(route)  # Create an error message
            self.insert_route_message(route, 'error', message)  # Insert the error message

        return missing_route.tolist()

    def rni_roadtype_check(self, road_type_details, routes='ALL', routeid_col='LINKID', from_m_col='STA_FR', to_m_col='STA_TO', lane_codes='LANE_CODE',
                           median_col='MEDWIDTH', road_type_col='ROAD_TYPE'):
        """
        This class method will check the consistency of stated roadtype code with other details such as the lane count,
        the median information, and the segment direction.
        :param road_type_details: The details about all available road type in RNI.
        :param from_m_col: The from measure column in the RNI DataFrame.
        :param to_m_col: The to measure column in the RNI DataFrame.
        :param lane_codes: The lane code column in the RNI DataFrame.
        :param median_col: The median column in the RNI DataFrame.
        :param road_type_col: The road type column in the RNI DataFrame.
        :return:
        """
        df = self.copy_valid_df()

        if routes == 'ALL':
            pass
        elif len(routes) == 0:
            return self
        else:
            df = self.selected_route_df(df, routes)  # Create the DataFrame with only requested routes

        # The groupped DataFrame by RouteID, FromMeasure, and ToMeasure
        df_groupped = df.groupby(by=[routeid_col, from_m_col, to_m_col]).\
            agg({lane_codes: ['nunique', 'unique'], median_col: 'sum', road_type_col: 'unique'}).reset_index()

        for index, row in df_groupped.iterrows():
            route = str(row[routeid_col][0])
            from_m = str(row[from_m_col][0])
            to_m = str(row[to_m_col][0])
            road_type_code_list = row[road_type_col]['unique']  # Get the road type list

            # If the road type is more than one in a single segment then give error message
            if len(road_type_code_list) == 1:

                road_type_code = str(road_type_code_list[0])  # The road type in string
                if road_type_code not in road_type_details.keys():
                    result = "Rute {0} pada segmen {1}-{2} memiliki kode {3} yang tidak valid. Kode {4}".\
                        format(route, from_m, to_m, road_type_col, road_type_code)
                    self.insert_route_message(route, 'error', result)
                    continue

                lane_count = road_type_details[road_type_code]['lane_count']  # The required lane count for specified type
                direction = road_type_details[road_type_code]['direction']  # The direction required
                median_exist = road_type_details[road_type_code]['median']  # The median existence requirement

                input_lane_count = row[lane_codes]['nunique']  # The lane count from the input
                input_direction = len(set([x[0] for x in row[lane_codes]['unique']]))  # The direction from input 1 or 2 dir
                input_median = row[median_col]['sum']  # The total median from the input

                if input_lane_count != lane_count:
                    result = "Rute {0} pada segmen {1}-{2} memiliki jumlah lane ({3} lane) yang tidak sesuai dengan road type {4} ({5} lane)".\
                        format(route, from_m, to_m, input_lane_count, road_type_code, lane_count)
                    self.insert_route_message(route, 'error', result)

                if input_direction != direction:
                    result = "Rute {0} pada segmen {1}-{2} arah ({3} arah) yang tidak sesuai dengan road type {4} ({5} arah)".\
                        format(route, from_m, to_m, input_direction, road_type_code, direction)
                    self.insert_route_message(route, 'error', result)

                if (median_exist and (input_median == 0)) or (not median_exist and (input_median != 0)):
                    result = "Rute {0} pada segmen {1}-{2} memiliki median yang tidak sesuai dengan road type {3}.".\
                        format(route, from_m, to_m, road_type_code)
                    self.insert_route_message(route, 'error', result)
            else:
                result = "Rute {0} pada segmen {1}-{2} memiliki road type yang tidak konsisten {3}".\
                    format(route, from_m, to_m, road_type_code_list)
                self.insert_route_message(route, 'error', result)

        return self

    def rtc_duration_check(self, duration=3, routes='ALL', routeid_col='LINKID', surv_date_col='DATE', hours_col='HOURS',
                           minute_col='MINUTE', direction_col='DIRECTION', interval=15):
        """
        This class method will check the RTC survey direction for every available route in the input table.
        :param duration: The survey duration (in days), the default is 3 days.
        :param routes: The selected routes to be processed.
        :param routeid_col: The RouteID column in the event DataFrame.
        :param surv_date_col: The survey date column in the event DataFrame.
        :param hours_col: The hours column in the event DataFrame.
        :param minute_col: The minute column in the event DataFrame.
        :return:
        """
        df = self.copy_valid_df()  # Create a copy of input table DataFrame

        if routes == 'ALL':
            pass
        else:
            df = self.selected_route_df(df, routes)  # If there is a route request then only process the selected route

        for route, direction in self.route_lane_tuple(df, routeid_col, direction_col):  # Iterate over all available route
            df_route_dir = df.loc[(df[routeid_col] == route) & (df[direction_col] == direction)].reset_index()  # Create a route and lane DataFrame

            survey_start_index = df_route_dir[surv_date_col].idxmin()  # The survey start row index
            survey_start_date = df_route_dir.at[survey_start_index, surv_date_col]  # The survey start date
            survey_start_hour = df_route_dir.at[survey_start_index, hours_col]  # The survey start hours
            survey_start_minutes = df_route_dir.at[survey_start_index, minute_col] - interval  # The survey start minutes
            start_timestamp = self.rtc_time_stamp(survey_start_date, survey_start_hour, survey_start_minutes)

            survey_end_index = df_route_dir[surv_date_col].idxmax()  # The survey end row index
            survey_end_date = df_route_dir.at[survey_end_index, surv_date_col]  # The survey end date
            survey_end_hour = df_route_dir.at[survey_end_index, hours_col]  # The survey end hours
            survey_end_minutes = df_route_dir.at[survey_end_index, minute_col]  # The survey end minutes
            end_timestamp = self.rtc_time_stamp(survey_end_date, survey_end_hour, survey_end_minutes)

            required_delta = timedelta(minutes=duration*24*60)  # The required survey duration

            if end_timestamp < (required_delta+start_timestamp):
                actual_delta = (end_timestamp-(required_delta+start_timestamp)).seconds/60  # The actual survey duration in minutes
                duration_in_h = duration*24  # The required survey duration in hours
                result = "Rute {0} pada arah {1} memiliki kekurangan durasi survey RTC sebanyak {2} menit dari total {3} jam yang harus dilakukan.".\
                    format(route, direction, actual_delta, duration_in_h)
                self.insert_route_message(route, 'error', result)

        return self

    def rtc_time_interval_check(self, interval=15, routes='ALL', routeid_col='LINKID', surv_date_col='DATE',
                                hours_col='HOURS', minute_col='MINUTE', direction_col='DIRECTION'):
        """
        This class method will check the RTC survey time interval.
        :param interval: The survey interval time (in minutes), the default value is 15 minutes.
        :param routeid_col: The RouteID column in the event DataFrame.
        :param surv_date_col: The survey date column in the event DataFrame.
        :param hours_col: The hours column in the event DataFrame.
        :param minute_col: The minute column in the event DataFrame.
        :param direction_col: The direction column in the event DataFrame.
        :return:
        """
        df = self.copy_valid_df()

        if routes == 'ALL':
            pass
        else:
            df = self.selected_route_df(df, routes)  # If there is a route request then only process the selected route

        for route, direction in self.route_lane_tuple(df, routeid_col, direction_col):  # Iterate over all available route
            df_route_dir = df.loc[(df[routeid_col] == route) & (df[direction_col] == direction)]
            df_route_dir.reset_index(inplace=True)  # Reset the index

            for index, row in df_route_dir.iterrows():
                row_timestamp = self.rtc_time_stamp(row[surv_date_col], row[hours_col], row[minute_col])
                date_timestamp_isof = row[surv_date_col].date().isoformat()
                row_timestamp_isof = row_timestamp.date().isoformat()

                if index == 0:
                    # The survey start time
                    start_timestamp = row_timestamp
                else:
                    delta_start_end = (row_timestamp - start_timestamp).seconds/60  # Interval in minutes

                    if delta_start_end != interval:  # If the delta does not match the requirement
                        end_time_str = row_timestamp.strftime('%d/%m/%Y %H:%M')  # Start time in string format
                        start_time_str = start_timestamp.strftime('%d/%m/%Y %H:%M')  # End time in string format
                        result = "Survey RTC di rute {0} pada arah {1} di interval survey {2} - {3} tidak berjarak {4} menit.".\
                            format(route, direction, start_time_str, end_time_str, interval)
                        self.insert_route_message(route, 'error', result)

                if date_timestamp_isof != row_timestamp_isof:  # Find the date which does not match with the hours
                    result = "Waktu survey RTC di rute {0} {1} pada tanggal {2} jam {3} menit {4} seharusnya memiliki tanggal {5}.".\
                        format(route, direction, date_timestamp_isof, row[hours_col], row[minute_col], row_timestamp_isof)
                    self.insert_route_message(route, 'error', result)

                start_timestamp = row_timestamp

        return self

    def rni_compare_surftype_len(self, comp_fc, comp_route_col, comp_from_col, comp_to_col, comp_surftype_col, year_comp,
                                 comp_lane_code, rni_route_col='LINKID', rni_from_col='STA_FR', rni_to_col='STA_TO',
                                 rni_surftype_col='SURFTYPE', rni_lane_code='LANE_CODE', routes='ALL'):
        """
        This class method will compare the surface type length of a route to previous year data. If there is a
        difference in the surface type length of a route, then an error message will be written.
        :param comp_fc: Feature Class used for comparison.
        :param comp_route_col: RouteID field in the comparison feature class.
        :param comp_from_col: From Measure field in the comparison feature class.
        :param comp_to_col: To Measure field in the comparison feature class.
        :param comp_surftype_col: Surface Type field in the comparison feature class.
        :param year_comp: The year of comparison feature class.
        :param comp_lane_code: Lane Code field in the comparison feature class.
        :param rni_route_col: The input RNI route id column.
        :param rni_from_col: The input RNI From Measure column.
        :param rni_to_col: The input RNI To Measure column.
        :param rni_surftype_col: The input RNI Surface type column.
        :param rni_lane_code: The input RNI Lane Code column.
        :param routes: The requested routes
        :return:
        """
        df = self.selected_route_df(self.copy_valid_df(), routes)  # Create a copy of valid DataFrame
        route_list = self.route_lane_tuple(df, rni_route_col, None, True)  # List of all route in the input DataFrame.

        for route in route_list:
            df_comp = event_fc_to_df(comp_fc, [comp_route_col, comp_from_col, comp_to_col, comp_surftype_col, comp_lane_code],
                                     route, comp_route_col, self.sde_connection, include_all=True, orderby=None)
            df_route = self.selected_route_df(df, route)  # The input selected route DataFrame

            if len(df_comp) == 0:
                # If the route does not exist in the comparison table
                result = 'Rute {0} tidak terdapat pada data tahun {1}.'.format(route, year_comp)
                self.insert_route_message(route, 'error', result)
                continue

            for _, lane in self.route_lane_tuple(df_route, rni_route_col, rni_lane_code):
                df_comp_lane = df_comp.loc[df_comp[comp_lane_code] == lane]  # The comparison route+lane DataFrame
                df_route_lane = df_route.loc[df_route[rni_lane_code] == lane]  # The input DataFrame route+lane

                if len(df_comp_lane) == 0:
                    # If the lane does not exist in the comparison table
                    result = 'Tidak terdapat lane {0} pada rute {1} di tahun {2}'.format(lane, route, year_comp)
                    self.insert_route_message(route, 'error', result)
                    continue

                comp_surftype_len = RNIRouteDetails(df_comp_lane, comp_route_col, comp_from_col, comp_to_col, comp_surftype_col)
                input_surftype_len = RNIRouteDetails(df_route_lane, rni_route_col, rni_from_col, rni_to_col, rni_surftype_col)

                input_percent = input_surftype_len.details_percentage(route)  # The input DataFrame surftype group
                comp_percent = comp_surftype_len.details_percentage(route)  # The Comparison surftype group

                # Join the surface type DataFrame from the input and the comparison table.
                merge = pd.merge(input_percent, comp_percent, how='outer', on='index', indicator=True)
                merge_input_only = merge.loc[merge['_merge'] == 'left_only']  # Surface type only on the input
                merge_comp_only = merge.loc[merge['_merge'] == 'right_only']  # Surface type only on the comparison table
                both = merge.loc[merge['_merge'] == 'both']

                if len(merge_input_only) != 0:
                    input_only_surftype = merge_input_only['index'].tolist()
                    result = 'Rute {0} pada lane {4} memiliki {1} yang tidak terdapat pada data tahun {2}. {1}-{3}'.\
                        format(route, rni_surftype_col, year_comp, input_only_surftype, lane)
                    self.insert_route_message(route, 'ToBeReviewed', result)

                if len(merge_comp_only) != 0:
                    comp_only_surftype = merge_comp_only['index'].tolist()
                    result = 'Rute {0} pada lane {4} tidak memiliki tipe {1} yang terdapat pada data tahun {2}. {1}-{3}'.\
                        format(route, rni_surftype_col, year_comp, comp_only_surftype, lane)
                    self.insert_route_message(route, 'ToBeReviewed', result)

                if ((len(merge_input_only) != 0) or (len(merge_comp_only) != 0)) and (len(both) != 0):
                    pass

    def rni_compare_surfwidth(self, comp_fc, comp_route_col, comp_from_col, comp_to_col, comp_lane_width, year_comp,
                              rni_route_col='LINKID', rni_from_col='STA_FR', rni_to_col='STA_TO',
                              rni_lane_width='LANE_WIDTH', routes='ALL'):
        """
        This class method will compare the road width to a comparison feature class, if there is a difference in the
        road width percentage in the requested routes, then an error message will be written.
        :param comp_fc: Feature Class used for comparison.
        :param comp_route_col: RouteID field in the comparison feature class.
        :param comp_from_col: From Measure field in the comparison feature class.
        :param comp_to_col: To Measure field in the ocmparison feature class.
        :param comp_lane_width: Lane Width in the comparison feature class.
        :param year_comp: The year of comparison feature class.
        :param rni_route_col: The input RNI RouteID field.
        :param rni_from_col: The input RNI From Measure field.
        :param rni_to_col: The input RNI To Measure field.
        :param rni_lane_width: The input RNI Lane Width field.
        :param routes: The requested routes.
        :return:
        """
        df = self.selected_route_df(self.copy_valid_df(), routes)  # Create a valid DataFrame copy
        route_list = self.route_lane_tuple(df, rni_route_col, None, True)

        for route in route_list:  # Iterate over all available route in the input table.
            df_comp = event_fc_to_df(comp_fc, [comp_route_col, comp_from_col, comp_to_col, comp_lane_width], route,
                                     comp_route_col, self.sde_connection, is_table=False, include_all=True, orderby=None)
            df_route = self.selected_route_df(df, route)

            if len(df_comp) == 0:
                result = 'Rute {0} tidak terdapat pada data tahun {1}.'.format(route, year_comp)
                self.insert_route_message(route, 'error', result)
                continue

            input_surfwidth = RNIRouteDetails(df_route, rni_route_col, rni_from_col, rni_to_col, rni_lane_width,
                                              agg_type='sum')
            comp_surfwidth = RNIRouteDetails(df_comp, comp_route_col, comp_from_col, comp_to_col, comp_lane_width,
                                             agg_type='sum')

            input_details = input_surfwidth.details_percentage(route)
            comp_details = comp_surfwidth.details_percentage(route)

            merge = pd.merge(input_details, comp_details, how='outer', on='index', indicator=True)
            both = merge.loc[merge['_merge'] == 'both']

            # Analyze the 'both'
            if (not(np.allclose(both['percentage_x'], both['percentage_y']))) or (len(both) == 0):  # if percentage at 'both' does not match or there is no 'both'
                result = "Rute {0} memiliki perbedaan lebar jalan dengan data tahun {1}.".format(route, year_comp)
                lanew_input = merge.loc[merge['percentage_x'].notnull(), ['index', 'percentage_x']].to_dict('split')  # Create the dictionary
                lanew_comp = merge.loc[merge['percentage_y'].notnull(), ['index', 'percentage_y']].to_dict('split')  # Create the dictionary

                for lanew_combination in [lanew_input, lanew_comp]:
                    lanews = lanew_combination['data']
                    if lanew_combination == lanew_input:
                        msg = ' Data input memiliki lebar jalan '
                        for lanew_details in lanews:  # Iterate over all available lane width in both input and comp
                            lanew = lanew_details[0]  # The lane width in meters
                            lanew_percentage = round(lanew_details[1])  # The lane width percentage
                            detail = "{0} meter sebanyak {1}%, ".format(lanew, lanew_percentage)  # Create the message details
                            msg += detail

                        result += msg[:-2]
                        result += '.'

                    else:
                        msg = ' Data {0} memiliki lebar jalan '.format(year_comp)
                        for lanew_details in lanews:
                            lanew = lanew_details[0]
                            lanew_percentage = round(lanew_details[1])
                            detail = "{0} meter sebanyak {1}%, ".format(lanew, lanew_percentage)
                            msg += detail

                        result += msg[:-2]
                        result += '.'

                self.insert_route_message(route, 'ToBeReviewed', result)

    def compare_kemantapan(self, rni_table, surftype_col, grading_col, comp_fc, comp_from_col, comp_to_col,
                           comp_route_col, comp_lane_code, comp_grading_col, routes='ALL', routeid_col='LINKID',
                           lane_codes='LANE_CODE', from_m_col='STA_FR', to_m_col='STA_TO', rni_route_col='LINKID',
                           rni_from_col='FROMMEASURE', rni_to_col='TOMEASURE', rni_lane_code='LANE_CODE',
                           threshold=0.05, lane_based = True):
        """
        This class method will compare the Kemantapan between the inputted data and previous year data, if the
        difference exceed the 5% absolute tolerance then the data will undergo further inspection.
        :param rni_table: The RNI Feature Class Table
        :param surftype_col: The surface type column in the RNI Feature Class
        :param grading_col: The column in the input event DataFrame used for grading.
        :param comp_fc: The feature class used for kemantapan status comparison
        :param comp_from_col: The column in the comparison feature class which store the from measure
        :param comp_to_col: The column in the comparison feature class which store the to measure
        :param comp_route_col: The column in the comparison feature class which store the Route ID
        :param comp_grading_col: The column in the comparison feature class which store the grading column
        :param routes: The route selections
        :param routeid_col: The RouteID column in the event DataFrame
        :param lane_codes: The lane code column in the event DataFrame
        :param from_m_col: The from measure column in the event DataFrame
        :param to_m_col: The to measure column in the event DataFrame
        :param rni_route_col: The RouteID column in the RNI Table
        :param rni_from_col: The from measure column in the RNI Table
        :param rni_to_col: The to measure column in the RNI Table
        :param threshold: The threshold for Kemantapan changes.
        :return:
        """
        df = self.copy_valid_df()  # Create the valid DataFrame copy

        if routes == 'ALL':  # Check for route request
            pass
        else:
            df = self.selected_route_df(df, routes)  # Create a DataFrame with selected route

        route_list = self.route_lane_tuple(df, routeid_col, lane_codes, route_only=True)  # Create a route list
        for route in route_list:  # Iterate over all available route
            df_route = df.loc[df[routeid_col] == route]  # The DataFrame with only selected route
            # Create the comparison DataFrame
            comp_search_field = [comp_route_col, comp_from_col, comp_to_col, comp_grading_col, comp_lane_code]
            df_comp = event_fc_to_df(comp_fc, comp_search_field, route,
                                     comp_route_col, self.sde_connection, include_all=True, orderby=None)
            df_comp[[comp_from_col, comp_to_col]] = df_comp[[comp_from_col, comp_to_col]].apply(lambda x: x*100).astype(int)

            # Create the RNI Table DataFrame
            rni_search_field = [rni_route_col, rni_from_col, rni_to_col, surftype_col, rni_lane_code]  # The column included in RNI
            df_rni = event_fc_to_df(rni_table, rni_search_field, route, rni_route_col, self.sde_connection,
                                    is_table=True, orderby=None)

            if len(df_comp) != 0:  # Check if the specified route exist in the comparison table.

                # Create Kemantapan instance for both input data and comparison data.
                kemantapan = Kemantapan(df_rni, df_route, grading_col, routeid_col, from_m_col, to_m_col, lane_codes,
                                        rni_route_col, rni_from_col, rni_to_col, rni_lane_code, surftype_col=surftype_col,
                                        lane_based=lane_based)
                kemantapan_compare = Kemantapan(df_rni, df_comp, comp_grading_col, comp_route_col, comp_from_col,
                                                comp_to_col, comp_lane_code, rni_route_col, rni_from_col, rni_to_col,
                                                rni_lane_code, surftype_col=surftype_col, lane_based=lane_based)

                if not lane_based:  # If the comparison is not lane based
                    current = kemantapan.mantap_percent.at['mantap', '_len']
                    compare = kemantapan_compare.at['mantap', '_len']

                    # Compare the kemantapan percentage between current data and previous data
                    if np.isclose(compare, current, atol=(compare*threshold)):
                        pass  # If true then pass
                    else:
                        # Create the error message
                        error_message = "{0} memiliki perbedaan persen kemantapan yang melebihi batas ({1}%) dari data Roughness sebelumnya.".\
                            format(route, (100*threshold))
                        self.error_list.append(error_message)
                        self.insert_route_message(route, 'ToBeReviewed', error_message)

                elif lane_based:  # If the comparison is lane based
                    current = kemantapan.graded_df
                    compare = kemantapan_compare.graded_df

                    # Merge the current input data and comparison table
                    current_key = [routeid_col, from_m_col, to_m_col, lane_codes]
                    compare_key = [comp_route_col, comp_from_col, comp_to_col, comp_lane_code]
                    _merge = pd.merge(current, compare, how='inner', left_on=current_key,
                                      right_on=compare_key)

                    # Create the new column for difference in grade level
                    diff_col = '_level_diff'
                    grade_diff = 2
                    _merge[diff_col] = abs(_merge['_grade_level_x'].astype(float) - _merge['_grade_level_y'].astype(float))
                    _error_rows = _merge.loc[_merge[diff_col] >= grade_diff]
                    error_rows_len = len(_error_rows)

                    # If the lane code between the input table and comparison table is same
                    if lane_codes in list(compare):
                        lane_codes = lane_codes+'_x'  # Use the lane code from the input

                    # Iterate over all error rows
                    if error_rows_len >= 10:
                        for index, row in _error_rows.iterrows():
                            sta_fr = row[from_m_col]
                            sta_to = row[to_m_col]
                            lane = row[lane_codes]

                            error_message = "{0} pada segmen {1}-{2} {3} memiliki perbedaan {4} tingkat kemantapan dengan data tahun sebelumnya.".\
                                format(route, sta_fr, sta_to, lane, grade_diff)
                            self.insert_route_message(route, 'ToBeReviewed', error_message)

            else:  # If the route does not exist in the comparison table
                error_message = "Data rute {0} pada tahun sebelumnya tidak tersedia, sehingga perbandingan kemantapan tidak dapat dilakukan.".\
                    format(route)
                self.error_list.append(error_message)
                self.insert_route_message(route, 'ToBeReviewed', error_message)

        return self

    def copy_valid_df(self, dropna=True):
        """
        This function create a valid DataFrame from the dtype check class method, which ensures every column match the
        required DataType
        :return:
        """
        # If there is a problem with the data type check then return the df_string
        if self.dtype_check(write_error=False) is None:
            df = self.df_valid
            return df.copy(deep=True)
        elif dropna and self.df_valid is not None:
            df = self.df_valid.dropna()
            return df.copy(deep=True)
        elif not dropna:
            df = self.df_string
            return df.copy(deep=True)
        else:
            return None

    @staticmethod
    def selected_route_df(df, routes, routeid_col="LINKID"):
        """
        This static method selects only route which is defined in the routes parameter
        :param df: Input table DataFrame
        :param routes: The requested routes
        :param routeid_col: RouteId column of the input table
        :return:
        """
        route_list = []  # List for storing all requested routes
        if type(routes) != list:
            route_list.append(routes)  # Append the requested routes to the list
        else:
            route_list = routes  # If the requested routes is in list format

        df = df.loc[df[routeid_col].isin(route_list)]
        return df  # Return the DataFrame with dropped invalid route

    @staticmethod
    def route_lane_tuple(df, routeid_col, lane_code, route_only=False):
        """
        This static method return a list containing tuple (route, lane) to be iterated to df_route_lane
        :param df: Input table DataFrame
        :param routeid_col: RouteID column of the input table
        :param lane_code: Lane Code column of the input table
        :param route_only: If route only is False then the function returns (route, lane), if True then the function
        returns a list containing route.
        :return: List containing a tuple (route, lane)
        """
        return_list = []  # Empty list for storing route, lane tuple

        for route in df[routeid_col].unique().tolist():  # Iterate for every available route
            if route_only:  # if route only
                return_list.append(str(route))
            if not route_only:
                for lane in df.loc[df[routeid_col] == route, lane_code].unique().tolist():  # Iterate for every lane
                    route_lane_tuple = (str(route), lane)  # Create tuple object
                    return_list.append(route_lane_tuple)  # Append the tuple object

        return return_list  # Return a list containing the (route, lane) tuple

    @staticmethod
    def route_geometry(route, lrs_network, lrs_routeid):
        """
        This static method return a Polyline object geometry from the LRS Network.
        :param route: The requested route.
        :param lrs_network: LRS Network feature class.
        :param lrs_routeid: The LRS Network feature class RouteID column.
        :return: Arcpy Polyline geometry object if the requested route exist in the LRS Network, if the requested route
        does not exist in the LRS Network then the function will return None.
        """
        where_statement = "{0}='{1}'".format(lrs_routeid, route)  # The where statement for SearchCursor
        route_exist = False  # Variable for determining if the requested route exist in LRS Network

        with da.SearchCursor(lrs_network, "SHAPE@", where_clause=where_statement) as cursor:
            for row in cursor:
                route_exist = True
                route_geom = row[0]  # The Polyline geometry object

        if route_exist:
            return route_geom
        if not route_exist:
            return None

    @staticmethod
    def rtc_time_stamp(date_time_stamp, hours, minutes):
        """
        This static method return a timestamp created from survey date TimeStamp + delta(hours and minutes), to create
        the survey TimeStamp which contain hours and minutes.
        :param date_time_stamp: Survey date TimeStamp.
        :param hours: Row hours value, the survey hours.
        :param minutes: Row minutes value, the sruvey minutes.
        :return: TimeStamp
        """
        if (type(hours) == np.int64) or (type(minutes) == np.int64):
            hours = hours.item()
            minutes = minutes.item()
        date = date_time_stamp  # The survey date
        hours_minutes = timedelta(hours=hours, minutes=minutes)  # Timedelta from survey hours and minutes
        timestamp = date + hours_minutes  # The new timestamp with hours and minutes

        return timestamp  # Return the new timestamp

    def insert_route_message(self, route, message_type, message):
        """
        This static method will insert a result message for specified route
        :param route:
        :param message_type:
        :param message:
        :return:
        """
        if route not in self.route_results:
            self.route_results[route] = {
                "error": [],
                "ToBeReviewed": []
            }

        self.route_results[route][message_type].append(message)

    def altered_route_result(self, routeid_col='LINKID', message_type='error', dict_output=True,
                             include_valid_routes=True):
        """
        This method will alter route_result dictionary from {'route':['msg', 'msg',...]} to [{'route: 'msg'},
        {'route':'msg'},...]
        :param routeid_col: The Route ID column of the input table.
        :param message_type: The message type will be passed to dictionary object
        :param dict_output: If True then this function will return [{'route: 'msg'}, {'route':'msg'},...] if false
        :param include_valid_routes: Include the valid routes, these valid route will only be included if
        dict_output = True
        :return:
        """
        result_list = []  # The list object to store the dictionary
        failed_routes = self.route_results.keys()
        passed_routes_row = self.df_valid.loc[~self.df_valid[routeid_col].isin(failed_routes), routeid_col].unique().tolist()

        for route in failed_routes:
            messages = self.route_results[route][message_type]

            for msg in messages:
                if dict_output:
                    # The dictionary object
                    dict_object = {
                        "linkid": route,
                        "status": message_type,
                        "msg": msg
                    }
                    result_list.append(dict_object)  # Append the dictionary object
                else:
                    result_list.append(msg)  # Append the message directly to list object

        if include_valid_routes and dict_output:  # If the output is dictionary and also include the valid route
            for route in passed_routes_row:
                dict_object = {
                    "linkid": route,  # The valid route
                    "status": "verified",  # The status is "verified"
                    "msg": ""  # The message is just an empty string
                }
                result_list.append(dict_object)  # Append the dictionary object

        return result_list
