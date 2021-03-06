from arcpy import env, da
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from SMD_Package.FCtoDataFrame import event_fc_to_df
from SMD_Package.event_table.kemantapan.kemantapan import Kemantapan
from SMD_Package.event_table.RNITable import RNIRouteDetails, add_rni_data
from SMD_Package.load_config import SMDConfigs
from SMD_Package.event_table.rni.summary import RNISummary
import coordinate


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
        self.df_valid = None  # df_valid is pandas DataFrame which has the correct data type and value for all columns
        self.header_check_result = self.header_check()  # The header checking result
        self.dtype_check_result = self.dtype_check(write_error=True)  # The data type checking result
        self.missing_route = []  # List for storing all route which is not in the balai route domain
        self.valid_route = []  # List for storing all route which is in the balai route domain
        self.config = SMDConfigs()
        self.rni_mfactor = 1
        self._coordinate_status = dict()  # For storing route's coordinate status

        if self.header_check_result is None:
            self.capitalize_string()  # Capitalize every column with string data type

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
                allow_null = False  # If True then Null value from input will not raise an error.
                raise_error = True  # If False then no error message will be written.
                null_input = df[col_name].isnull()  # Row with null input

                if 'allow_null' in self.column_details[col].keys():  # If there is 'allow_null' key in the JSON
                    allow_null = self.column_details[col]['allow_null']

                if 'raise' in self.column_details[col].keys():  # If there is 'write_error' key in the JSON
                    raise_error = self.column_details[col]['raise']

                if col_dtype in ["integer", "double"]:  # Check for numeric column

                    # Convert the column to numeric
                    # If the column contain non numerical value, then change that value to Null
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                    error_null = df[col_name].isnull()  # Null value from the coerce

                    if 'decimals' in self.column_details[col].keys():  # If there is 'decimal' key in the JSON
                        decimal_places = int(self.column_details[col]['decimals'])
                        df[col_name] = df[col_name].round(decimals=decimal_places)

                    if allow_null:
                        error_row = df.loc[~null_input & error_null, [routeid_col, col_name]]  # Null value from the coerce not from the input
                    else:
                        error_row = df.loc[error_null, [routeid_col, col_name]]  # Find the row with Null value

                    # If there is an error
                    if len(error_row) != 0 and raise_error:
                        excel_i = [x + 2 for x in error_row.index.tolist()]
                        error_message = '{0} memiliki nilai bukan angka (integer/decimal) pada baris {1}.'\
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
                    error_null = df[col_name].isnull()

                    if allow_null:
                        error_row = df.loc[~null_input & error_null, [routeid_col, col_name]]  # Null value from the coerce not from the input
                    else:
                        error_row = df.loc[error_null, [routeid_col, col_name]]  # Find the row with Null value

                    error_i = error_row.index.tolist()  # Find the index of the null

                    # If there is an error
                    if len(error_i) != 0 and raise_error:
                        excel_i = [x + 2 for x in error_i]
                        error_message = '{0} memiliki tanggal yang tidak sesuai dengan format (dd/mm/yyyy) pada baris{1}.'\
                            .format(col_name, str(excel_i))
                        error_list.append(error_message)

                        if write_error:
                            for index, row in error_row.iterrows():
                                result = 'Rute {0} pada kolom {1} memiliki tanggal yang tidak sesuai dengan format baris{2}.'. \
                                    format(row[routeid_col], col_name, index + 2)
                                self.insert_route_message(row[routeid_col], "error", result)

                            self.error_list.append(error_message)

                elif col_dtype == 'string':

                    if allow_null:
                        continue
                    elif col_name == routeid_col:
                        error_row = df.loc[null_input, [col_name]]
                    else:
                        error_row = df.loc[null_input, [routeid_col, col_name]]  # Find the row with Null value

                    error_i = error_row.index.tolist()  # Find the index of the null

                    # If there is an error
                    if len(error_i) != 0 and raise_error:
                        excel_i = [x + 2 for x in error_i]
                        error_message = '{0} memiliki baris yang tidak diisi (Null/kosong) pada baris {1}.'\
                            .format(col_name, str(excel_i))
                        error_list.append(error_message)

                        if write_error:
                            for index, row in error_row.iterrows():
                                result = 'Rute {0} pada kolom {1} memiliki baris yang tidak diisi pada baris {2}.'. \
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

    def capitalize_string(self, string_type='string'):
        """
        This class method will capitalize all value in any text column.
        :return:
        """
        string_cols = []
        for col, details in self.column_details.items():
            dtype = details['dtype']

            if dtype == string_type:
                all_null = np.all(self.df_valid[col].isnull())
                if not all_null:
                    string_cols.append(col)

        self.df_valid[string_cols] = self.df_valid[string_cols].apply(lambda x: x.str.upper())
        return self

    def year_and_semester_check(self, year_input, semester_input, year_col='SURVEY_YEAR', sem_col='SURVEY_SMS',
                                routeid_col='LINKID', from_m_col='STA_FROM', to_m_col='STA_TO', lane_code='LANE_CODE',
                                year_check_only=False, **kwargs):
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
        :param year_check_only: If True then the method will only check the survey_year value.
        :return: self
        """
        df = self.copy_valid_df(ignore=True)

        if df is None:  # This means no rows passed the data type check
            return None  # Return None

        # Get the current year
        cur_year = datetime.now().year

        # Create the selection mask
        year_mask = df[year_col] != year_input
        cur_year_mask = df[year_col] > cur_year  # The inputted year should not exceed the current year

        # the index of row with bad val
        if year_check_only:
            error_row = df.loc[year_mask | cur_year_mask]
        else:
            semester_mask = df[sem_col] != semester_input
            error_row = df.loc[cur_year_mask | semester_mask | year_mask]

        # If  there is an error
        if len(error_row) != 0:
            excel_i = [x + 2 for x in error_row.index.tolist()]
            error_message = '{0} atau {1} tidak sesuai dengan input ({3}/{4}) pada baris{2}.'.\
                format(year_col, sem_col, excel_i, year_input, semester_input)

            for index, row in error_row.iterrows():

                if year_check_only:
                    if (lane_code is not None) and (row.get(lane_code) is not None):
                        result = "Rute {0} memiliki {1} yang tidak sesuai dengan input {2} pada segmen {3}-{4} {5}.".\
                            format(row[routeid_col], year_col, year_input, row[from_m_col], row[to_m_col], row[lane_code])
                        self.insert_route_message(row[routeid_col], 'error', result)
                    else:
                        result = "Rute {0} memiliki {1} yang tidak sesuai dengan input {2} pada segmen {3}-{4}.".\
                            format(row[routeid_col], year_col, year_input, row[from_m_col], row[to_m_col])
                        self.insert_route_message(row[routeid_col], 'error', result)

                else:
                    if (lane_code is not None) and (row.get(lane_code) is not None):
                        result = "Rute {0} memiliki {1} atau {2} yang tidak sesuai dengan input {3}/{4} pada segmen {5}-{6} {7}.".\
                            format(row[routeid_col], year_col, sem_col, year_input, semester_input, row[from_m_col],
                                   row[to_m_col], row[lane_code])
                        self.insert_route_message(row[routeid_col], 'error', result)
                    else:  # If the data does not contain lane code column
                        result = "Rute {0} memiliki {1} atau {2} yang tidak sesuai dengan input {3}/{4} pada segmen {5}-{6}".\
                            format(row[routeid_col], year_col, sem_col, year_input, semester_input, row[from_m_col],
                                   row[to_m_col])
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
                result = "Rute {0} bukan kewenangan balai {1}.".format(missing_route, balai_code)
                self.insert_route_message(missing_route, 'error', result)

        return self

    def route_selection(self, selection='ALL'):
        """
        This class method will modify valid_route class attribute based on the selection parameter. If the selection
        parameter is 'ALL' then the valid_route will not be modified.
        :param selection: The route selection.
        :return:
        """
        if selection == 'ALL':
            return self
        else:
            if type(selection) == str or type(selection) == unicode:
                route_exist = np.any(np.in1d(self.valid_route, selection))
                if route_exist:  # Check if the requested route exist in the valid route list
                    self.valid_route = [selection]
                else:
                    error_message = '{0} tidak termasuk di dalam kode ruas valid yang terdapat di dalam tabel input.'.\
                        format(selection)
                    self.insert_route_message(selection, 'error', error_message)
                    self.valid_route = list()  # Return an empty list

            elif type(selection) == list:
                route_exist = np.any(np.in1d(self.valid_route, selection))
                if route_exist:  # Check in any of the requested routes exist in the valid route list
                    route_intersect = np.intersect1d(self.valid_route, selection).tolist()
                    self.valid_route = route_intersect
                else:  # If all the requested routes does not exist in the valid route list
                    for missing_route in selection:
                        error_message = '{0} tidak termasuk di dalam kode ruas valid yang terdapat di dalam tabel input.'.\
                            format(missing_route)
                        self.insert_route_message(missing_route, 'error', error_message)
                        self.valid_route = list()  # Return an empty list

        return self

    def range_domain_check(self, routes='ALL', routeid_col='LINKID', from_m_col='STA_FROM', to_m_col='STA_TO',
                           lane_code='LANE_CODE', **kwargs):
        """
        This function checks every value in a specified data column, to match the specified range value defined by
        parameter upper and lower (lower < [value] < upper).
        :param routes: Route selection.
        :param routeid_col: The Route ID column in the input table.
        :param from_m_col: The From Measure column in the input table.
        :param to_m_col: The To Measure column in the input table.
        :param lane_code: The lane code column in the input table.
        :return: self
        """
        df = self.copy_valid_df()

        if routes == 'ALL':
            pass
        else:
            df = self.selected_route_df(df, routes, routeid_col=routeid_col)

        for column in self.column_details.keys():
            allow_null = self.column_details[column].get('allow_null')

            if 'range' in self.column_details[column].keys():
                range_details = self.column_details[column]['range']
                upper_bound = range_details['upper']  # The range upper bound
                lower_bound = range_details['lower']  # The range lower bound
                eq_upper = range_details['eq_upper']  # Equal with the upper bound
                eq_lower = range_details['eq_lower']  # Equal with the lower bound
                review = range_details['review']  # As To Be Reviewed message or as an Error Message
                status_col = '_status'
                df[status_col] = pd.Series(None)  # Create an empty column for storing row status

                # The upper value mask
                if upper_bound is None:
                    upper_mask = False
                else:
                    if eq_upper:
                        upper_mask = df[column] > upper_bound
                    else:
                        upper_mask = df[column] >= upper_bound

                # The lower value mask
                if lower_bound is None:
                    lower_mask = False
                else:
                    if eq_lower:
                        lower_mask = df[column] < lower_bound
                    else:
                        lower_mask = df[column] <= lower_bound

                # Give the error status for the lower and upper mask
                df.loc[upper_mask | lower_mask, [status_col]] = 'error'

                # Check the review condition
                if review is True:
                    df.loc[upper_mask | lower_mask, [status_col]] = 'ToBeReviewed'
                elif review == 'upper':
                    df.loc[upper_mask, [status_col]] = 'ToBeReviewed'
                elif review == 'lower':
                    df.loc[lower_mask, [status_col]] = 'ToBeReviewed'
                elif type(review) == dict:
                    rev_upper = review['upper']
                    rev_lower = review['lower']
                    rev_eq_upper = review['eq_upper']
                    rev_eq_lower = review['eq_lower']
                    direction = review['direction']

                    # Give the ToBeReviewed Status
                    # Does not override any 'error' status
                    if direction == 'inward':
                        # The upper value mask
                        if rev_eq_upper:
                            rev_upper_mask = df[column] <= rev_upper
                        else:
                            rev_upper_mask = df[column] < rev_upper

                        # The lower value mask
                        if rev_eq_lower:
                            rev_lower_mask = df[column] >= rev_lower
                        else:
                            rev_lower_mask = df[column] > rev_lower

                        df.loc[rev_upper_mask & rev_lower_mask & df[status_col].isnull(), [status_col]] = 'ToBeReviewed'

                    if direction == 'outward':
                        # The upper value mask
                        if rev_eq_upper:
                            rev_upper_mask = df[column] >= rev_upper
                        else:
                            rev_upper_mask = df[column] > rev_upper

                        # The lower value mask
                        if rev_eq_lower:
                            rev_lower_mask = df[column] <= rev_lower
                        else:
                            rev_lower_mask = df[column] < rev_lower

                        df.loc[(rev_upper_mask | rev_lower_mask) & df[status_col].isnull(), [status_col]] = 'ToBeReviewed'

                if allow_null:
                    error_row = df.loc[(df[status_col].notnull()) & (df[column].notnull())]
                else:
                    error_row = df.loc[df[status_col].notnull()]  # Find the faulty row

                if len(error_row) != 0:
                    # Create error message
                    excel_i = [x + 2 for x in error_row.index.tolist()]  # Create row for excel file index
                    error_message = '{0} memiliki nilai yang berada di luar rentang ({1}<{0}<{2}), pada baris {3}'. \
                        format(column, lower_bound, upper_bound, excel_i)
                    self.error_list.append(error_message)  # Append to the error message

                    for index, row in error_row.iterrows():
                        msg_status = row[status_col]

                        if (msg_status == 'error') or ((msg_status == 'ToBeReviewed') and (review is True)):
                            if from_m_col is None or to_m_col is None:
                                result = "Rute {0} memiliki nilai {1} yang berada di luar rentang ({2}<{1}<{3}), pada baris {4}.".\
                                    format(row[routeid_col], column, lower_bound, upper_bound, index+2)
                            else:
                                result = "Rute {0} memiliki nilai {1} yang berada di luar rentang ({2}<{1}<{3}), pada segmen {4}-{5} lane {6} yaitu {7}". \
                                    format(row[routeid_col], column, lower_bound, upper_bound, row[from_m_col],
                                           row[to_m_col], row[lane_code], row[column])

                        elif (msg_status == 'ToBeReviewed') and review == 'upper':
                            if from_m_col is None or to_m_col is None:
                                result = "Rute {0} memiliki nilai {1} yang berada di luar rentang ({1}>{3}), pada baris {4}.".\
                                    format(row[routeid_col], column, lower_bound, upper_bound, index+2)
                            else:
                                result = "Rute {0} memiliki nilai {1} yang berada di luar rentang ({1}>{3}), pada segmen {4}-{5} {6} yaitu {7}". \
                                    format(row[routeid_col], column, lower_bound, upper_bound, row[from_m_col],
                                           row[to_m_col], row[lane_code], row[column])

                        elif (msg_status == 'ToBeReviewed') and review == 'lower':
                            if from_m_col is None or to_m_col is None:
                                result = "Rute {0} memiliki nilai {1} yang berada di luar rentang ({1}<{2}), pada baris {4}.".\
                                    format(row[routeid_col], column, lower_bound, upper_bound, index+2)
                            else:
                                result = "Rute {0} memiliki nilai {1} yang berada di luar rentang ({1}<{2}), pada segmen {4}-{5} lane {6} yaitu {7}". \
                                    format(row[routeid_col], column, lower_bound, upper_bound, row[from_m_col],
                                           row[to_m_col], row[lane_code], row[column])

                        elif (msg_status == 'ToBeReviewed') and type(review) == dict:
                            if direction == 'outward':
                                if from_m_col is None or to_m_col is None:
                                    result = "Rute {0} memiliki nilai {1} yang berada di luar rentang ({1}<{2} atau {1}>{3}), pada baris {4}.". \
                                        format(row[routeid_col], column, rev_lower, rev_upper, index + 2)
                                else:
                                    result = "Rute {0} memiliki nilai {1} yang berada di luar rentang ({1}<{2} atau {1}>{3}), pada segmen {4}-{5} lane {6} yaitu {7}". \
                                        format(row[routeid_col], column, rev_lower, rev_upper, row[from_m_col],
                                               row[to_m_col], row[lane_code], row[column])

                            if direction == 'inward':
                                if from_m_col is None or to_m_col is None:
                                    result = "Rute {0} memiliki nilai {1} yang berada di luar rentang ({2}<{1}<{3}), pada baris {4}.". \
                                        format(row[routeid_col], column, rev_lower, rev_upper, index + 2)
                                else:
                                    result = "Rute {0} memiliki nilai {1} yang berada di luar rentang ({2}<{1}<{3}), pada segmen {4}-{5} lane {6} yaitu {7}". \
                                        format(row[routeid_col], column, rev_lower, rev_upper, row[from_m_col],
                                               row[to_m_col], row[lane_code], row[column])

                        # Insert the error message depend on the message status (as an Error or Review)
                        self.insert_route_message(row[routeid_col], msg_status, result)

            if 'domain' in self.column_details[column].keys():
                val_domain = self.column_details[column]['domain']  # The domain list

                if allow_null:
                    error_row = df.loc[(~df[column].isin(val_domain)) & (df[column].notnull())]
                else:
                    error_row = df.loc[~df[column].isin(val_domain)]  # Find the faulty row

                if len(error_row) != 0:
                    for index, row in error_row.iterrows():

                        if (from_m_col is None) or (to_m_col is None):
                            result = "Rute {0} memiliki nilai {1} yang tidak termasuk di dalam domain, pada baris {2}.".\
                                format(row[routeid_col], column, index + 2)
                        else:
                            result = "Rute {0} memiliki nilai {1} yang tidak termasuk di dalam domain, pada segmen {2}-{3} {4} yaitu {5}.".\
                                format(row[routeid_col], column, row[from_m_col], row[to_m_col], row[lane_code],
                                       row[column])

                        self.insert_route_message(row[routeid_col], 'error', result)

        return self

    def survey_year_check(self, data_year, survey_date_col='SURVEY_DATE', routeid_col='LINKID', from_m_col='STA_FROM',
                          to_m_col='STA_TO', lane_code='LANE_CODE', **kwargs):
        """
        This function checks for consistency between the stated data year and survey date year.
        :param survey_date_col:
        :param data_year:
        :param routeid_col:
        :param from_m_col:
        :param to_m_col:
        :param lane_code:
        :return:
        """
        df = self.copy_valid_df()
        df['_year'] = pd.DatetimeIndex(df[survey_date_col]).year  # Create a year column
        error_rows = df.loc[df['_year'] != data_year]

        for index, row in error_rows.iterrows():
            route = row[routeid_col]
            from_m = row[from_m_col]
            to_m = row[to_m_col]
            lane = row[lane_code]

            error_msg = 'Rute {0} pada segmen {1}-{2} {3} memiliki tanggal survey dengan tahun yang berbeda dengan tahun data.'.\
                format(route, from_m, to_m, lane)
            self.insert_route_message(route, 'error', error_msg)

        return self

    def segment_duplicate_check(self, routes='ALL', routeid_col='LINKID', from_m_col='STA_FROM', to_m_col='STA_TO',
                                lane_code='LANE_CODE', drop=True, **kwargs):
        """
        This function checks for any duplicate segment determined by the keys defined in the parameter.
        :param routes: Route selection.
        :param routeid_col: The Route ID column
        :param from_m_col: The From Measure column
        :param to_m_col: The To Measure column
        :param lane_code: The Lane Code column
        :param drop: If true then the duplicated rows will be dropped from the DataFrame.
        :return:
        """
        keys = [routeid_col, from_m_col, to_m_col, lane_code]

        df_route = self.selected_route_df(self.df_valid, routes, routeid_col)
        duplicate_rows = df_route[df_route.duplicated(keys, keep='first')]
        duplicate_index = duplicate_rows.index

        for index, row in duplicate_rows.iterrows():
            from_m = row[from_m_col]
            to_m = row[to_m_col]
            route = row[routeid_col]
            lane = row[lane_code]

            error_message = 'Segmen {0}-{1} {2} pada rute {3} memiliki duplikat.'.format(from_m, to_m, lane, route)
            self.insert_route_message(route, 'error', error_message)

        if drop:
            self.df_valid.drop(duplicate_index, inplace=True)  # Drop duplicated rows

        return self

    def segment_len_check(self, routes='ALL', segment_len=0.1, routeid_col='LINKID', from_m_col='STA_FROM',
                          to_m_col='STA_TO', lane_code='LANE_CODE', length_col='SEGMENT_LENGTH', **kwargs):
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
        orig_df = self.copy_valid_df()  # The unmodified DataFrame.

        df[from_m_col] = pd.Series(df[from_m_col] / 100)  # Convert the from measure to Km
        df[to_m_col] = pd.Series(df[to_m_col] / 100)  # Convert the to measure to Km
        df['diff'] = pd.Series(df[to_m_col] - df[from_m_col])  # Create a diff column for storing from-to difference

        if routes == 'ALL':
            pass
        else:
            df = self.selected_route_df(df, routes, routeid_col=routeid_col)

        for route, lane in self.route_lane_tuple(df, routeid_col, lane_code):  # Iterate over all route and lane

                df_route_lane = df.loc[(df[lane_code] == lane) & (df[routeid_col] == route)]
                max_to_ind = df_route_lane[from_m_col].idxmax()

                # Rounded
                last_segment_len = np.round(df_route_lane.at[max_to_ind, 'diff'], 2)  # The last segment real length
                last_segment_statedlen = df_route_lane.at[max_to_ind, length_col]  # The last segment stated len
                last_from = df_route_lane.at[max_to_ind, from_m_col]*100  # Last segment from measure in Decameters
                last_to = df_route_lane.at[max_to_ind, to_m_col]*100  # Last segment to measure in Decameters
                last_interval = '{0}-{1}'.format(last_from, last_to)  # The interval value in string

                # Find the row with segment len error, find the index
                # Exclude the last segment (segment with the largest from_m).
                error_i = df_route_lane.loc[(~np.isclose(df_route_lane['diff'], df_route_lane[length_col], atol=0.02) |
                                            (~np.isclose(df_route_lane[length_col], segment_len, atol=0.02))) &
                                            ~(df_route_lane[from_m_col] == df_route_lane.at[max_to_ind, from_m_col])].index

                # Pop the last segment from the list of invalid segment
                # error_i_pop_last = np.setdiff1d(error_i, max_to_ind)

                if len(error_i) != 0:
                    for error_index in error_i:
                        excel_i = error_index + 2  # Create the index for excel table
                        from_m = df_route_lane.at[error_index, from_m_col]  # The from m value
                        to_m = df_route_lane.at[error_index, to_m_col]  # The to m value
                        segment_real_len = df_route_lane.at[error_index, length_col]
                        # Create error message
                        error_message = 'Segmen pada baris {2} memiliki {0}-{1} ({4}-{5}) dan panjang segmen ({3}) yang tidak konsisten atau memiliki panjang segmen yang tidak sama dengan {6}.'.\
                            format(from_m_col, to_m_col, excel_i, segment_real_len, from_m, to_m, segment_len)
                        self.error_list.append(error_message)  # Append the error message
                        self.insert_route_message(route, 'error', error_message)

                # Check whether the stated length for the last segment match the actual length
                len_statedlen_diff = abs(last_segment_len - last_segment_statedlen)

                if np.isclose(last_segment_len, 0):  # Prevent last segment from having same from-to value.
                    error_message = 'Segmen akhir {0} di rute {1} pada lane {2} memililki nilai {3}-{4} yang sama (panjang = 0).'.\
                        format(last_interval, route, lane, from_m_col, to_m_col)
                    self.insert_route_message(route, 'error', error_message)
                else:
                    if len_statedlen_diff > 0.01 and (last_segment_statedlen > last_segment_len):
                        # Create error message
                        error_message = 'Segmen akhir {0} di rute {1} pada lane {2} memiliki panjang yang berbeda dengan yang tertera pada kolom {3} yaitu ({4}).'.\
                            format(last_interval, route, lane, length_col, last_segment_statedlen)
                        self.error_list.append(error_message)
                        self.insert_route_message(route, 'error', error_message)

                    if last_segment_len > segment_len and (not np.isclose(last_segment_len, segment_len, atol=0.001)):
                        # Create error message
                        error_message = 'Segmen akhir {0} di rute {1} pada lane {2} memiliki panjang segmen ({3}) yang melebihi {4}km.'.\
                            format(last_interval, route, lane, last_segment_len, segment_len)
                        self.insert_route_message(route, 'error', error_message)

                # Check if the last TO_M is integer OR has .0 decimal.
                if not last_to.is_integer():
                    error_message = 'Segmen akhir {0} di rute {1} pada lane {2} memiliki nilai {3}={4} yang bukan merupakan bilangan bulat(integer).'.\
                        format(last_interval, route, lane, to_m_col, last_to)
                    self.insert_route_message(route, 'error', error_message)

        return self

    def measurement_check(self, routes='ALL', from_m_col='STA_FROM', to_m_col='STA_TO',
                          routeid_col='LINKID', lane_code='LANE_CODE', compare_to='RNI', length_col='SEGMENT_LENGTH',
                          ignore_end_gap=False, ignore_exceed_ref=False, start_at_zero=True, tolerance=0.01,
                          end_only=False, max_m='to_m', **kwargs):
        """
        This function checks all event segment measurement value (from and to) for gaps, uneven increment, and final
        measurement should match the route M-value where the event is assigned to.
        :param routes: Route selection.
        :param from_m_col: From Measurement column.
        :param to_m_col: To Measurment column.
        :param routeid_col: Route ID column.
        :param lane_code: Lane code column.
        :param compare_to: Comparison used for total survey data length.
        :param ignore_end_gap: If True then any gap located at the end of inputted data is ignored.
        :param ignore_exceed_ref: If False then the input data max To Measure should not exceed the reference.
        :param tolerance: Tolerance used for detecting error (in Meters).
        :param end_only: If True then the function only checks for error located at the end of inputted data (Data
        measurement should start at 0 and the final measurement are within the comparison max value tolerance).
        :param kwargs:
        :return:
        """
        env.workspace = self.sde_connection  # Setting up the env.workspace
        df = self.copy_valid_df()  # Create a valid DataFrame with matching DataType with requirement
        groupby_cols = [routeid_col, from_m_col, to_m_col]

        rni_table = self.config.table_names['rni']
        rni_routeid = self.config.table_fields['rni']['route_id']
        rni_to_m = self.config.table_fields['rni']['to_measure']

        if routes == 'ALL':  # Only process selected routes, if 'ALL' then process all routes in input table
            pass
        else:
            df = self.selected_route_df(df, routes)

        # Iterate over valid row in the input table
        for route in df[routeid_col].unique().tolist():
            # Create a route DataFrame
            df_route = df.loc[df[routeid_col] == route, [routeid_col, from_m_col, to_m_col, lane_code, length_col]]

            if lane_code is not None:
                df_groupped = df_route.groupby(by=groupby_cols)[lane_code].unique().\
                    reset_index()  # Group the route df
            else:
                df_groupped = df_route

            # Sort the DataFrame based on the RouteId and FromMeasure
            df_groupped.sort_values(by=[routeid_col, from_m_col, to_m_col], inplace=True)
            df_groupped.reset_index(drop=True)

            max_to_ind = df_groupped[to_m_col].idxmax()  # The index of segment with largest To Measure
            if max_m == 'to_m':
                max_to_meas = float(df_groupped.at[max_to_ind, to_m_col]) / 100  # The largest To Measure value
            elif max_m == 'segment_len':
                pivot = df_route.pivot_table(length_col, None, lane_code, 'sum')
                max_to_meas = pivot.max()
            else:
                raise TypeError('max_m parameter "{0}" is not valid.'.format(max_m))

            min_from_ind = df_groupped[from_m_col].idxmin()  # The index of segment with smallest From Measure
            min_from_meas = float(df_groupped.at[min_from_ind, from_m_col]) / 100  # The smallest From Measure value

            # Comparison based on the 'compare_to' parameter
            if compare_to == 'RNI':
                # Get the RNI Max Measurement
                rni_df = event_fc_to_df(rni_table, [rni_routeid, rni_to_m], route, rni_routeid, self.sde_connection,
                                        is_table=False, include_all=True)  # The RNI DataFrame

                if len(rni_df) == 0:  # If the RNI Table does not exist for a route
                    comparison = None  # The comparison value will be None
                else:
                    rni_max_m = rni_df.at[rni_df[rni_to_m].argmax(), rni_to_m]  # The Route RNI maximum measurement
                    comparison = float(rni_max_m)/float(100/self.rni_mfactor)

            elif compare_to == 'LRS':
                # Get the LRS Network route length
                lrs_route_len = self.route_geometry(route, self.lrs_network, self.lrs_routeid).lastPoint.M
                comparison = lrs_route_len

            else:
                raise ValueError("{0} is not a correct M-Value comparison parameter (LRS or RNI only)".
                                 format(compare_to))

            # If the comparison value is not available.
            if comparison is None:
                pass
            else:
                if compare_to == 'LRS':
                    round_comp = np.round(comparison, decimals=2)
                    less_than_reference = max_to_meas < round_comp
                    more_than_reference = max_to_meas > round_comp
                    close_to_reference = np.isclose(max_to_meas, round_comp, atol=tolerance)
                else:
                    less_than_reference = max_to_meas < comparison
                    more_than_reference = max_to_meas > comparison
                    close_to_reference = np.isclose(max_to_meas, comparison)

                if not close_to_reference:
                    if less_than_reference and (not ignore_end_gap):
                        if compare_to == 'LRS':
                            # Create an error message
                            msg = 'Data survey pada rute {0} adalah {1}, masih lebih pendek daripada panjang datar'.\
                                format(route, max_to_meas)
                        else:
                            msg = "Tidak ditemukan data survey pada rute {0} dari km {1} hingga {2}. (Terdapat gap di akhir ruas).".\
                                format(route, max_to_meas, comparison)
                        self.error_list.append(msg)
                        self.insert_route_message(route, 'error', msg)

                    elif more_than_reference and (not ignore_exceed_ref):
                        msg = 'Rute {0} memiliki panjang yang melebihi data referensi yaitu {1} Km.'.\
                            format(route, max_to_meas)
                        self.insert_route_message(route, 'error', msg)

            if not np.isclose(min_from_meas, 0) and (start_at_zero):
                error_message = 'Data survey pada rute {0} tidak dimulai dari 0.'.format(route)
                self.error_list.append(error_message)
                self.insert_route_message(route, 'error', error_message)

            if not end_only:  # If end_only is False then checks for all error at the middle of inputted data.
                if (kwargs.get('first_lane_only_gap') is None) or (not kwargs.get('first_lane_only_gap')):

                    if lane_code is not None:
                        lane_group = df_route.groupby(by=[str(lane_code)])
                    else:
                        lane_group = df_route.groupby(by=[str(routeid_col)])

                    error_rows = None
                    for name, group_df in lane_group:
                        g_sorted = group_df.sort_values(by=from_m_col).reset_index()
                        g_sorted['SHIFTED_FROM_M'] = g_sorted[from_m_col].shift(-1)
                        g_sorted['TO-FROM_GAP'] = g_sorted[to_m_col] - g_sorted['SHIFTED_FROM_M']  # Detect gap/overlap

                        g_sorted.loc[g_sorted['TO-FROM_GAP'] < 0, 'MIDDLE_GAP'] = True
                        g_sorted.loc[g_sorted['TO-FROM_GAP'] > 0, 'OVERLAP'] = True
                        g_sorted.loc[g_sorted[from_m_col] > g_sorted[to_m_col], 'FROM>TO'] = True  # From M > To M

                        if error_rows is None:
                            error_rows = g_sorted.loc[g_sorted['MIDDLE_GAP'] |
                                                      g_sorted['OVERLAP'] |
                                                      g_sorted['FROM>TO']]
                        else:
                            error_rows = error_rows.append(g_sorted.loc[g_sorted['MIDDLE_GAP'] |
                                                           g_sorted['OVERLAP'] |
                                                           g_sorted['FROM>TO']])

                else:
                    segment_group = df_route.loc[(df_route[lane_code] == 'L1') | (df_route[lane_code] == 'R1')].\
                                    groupby(by=[from_m_col, to_m_col])[lane_code].unique().reset_index()

                    if segment_group.empty:
                        error_msg = "Rute {0} tidak memiliki lane L1 atau R1.".format(route)
                        self.insert_route_message(route, "error", error_msg)
                        continue

                    g_sorted = segment_group.sort_values(by=from_m_col).reset_index()
                    g_sorted['SHIFTED_FROM_M'] = g_sorted[from_m_col].shift(-1)
                    g_sorted['TO-FROM_GAP'] = g_sorted[to_m_col] - g_sorted['SHIFTED_FROM_M']  # Detect gap/overlap

                    g_sorted.loc[g_sorted['TO-FROM_GAP'] < 0, 'MIDDLE_GAP'] = True
                    g_sorted.loc[g_sorted['TO-FROM_GAP'] > 0, 'OVERLAP'] = True
                    g_sorted.loc[g_sorted[from_m_col] > g_sorted[to_m_col], 'FROM>TO'] = True  # From M > To M

                    error_rows = g_sorted.loc[g_sorted['MIDDLE_GAP'] |
                                              g_sorted['OVERLAP'] |
                                              g_sorted['FROM>TO']]

                for index, row in error_rows.iterrows():
                    from_m = row[from_m_col]
                    to_m = row[to_m_col]

                    if lane_code is None:
                        lane = None
                    else:
                        lane = row[lane_code]

                    next_from_m = row['SHIFTED_FROM_M']

                    if row['MIDDLE_GAP'] is True:
                        if lane is None:
                            error_message = 'Tidak ditemukan data survey pada rute {0} dari Km {1} hingga {2}. (Terdapat gap di tengah ruas)'. \
                                format(route, to_m, next_from_m)
                        else:
                            error_message = 'Tidak ditemukan data survey pada rute {0} dari Km {1} hingga {2} pada lane {3}. (Terdapat gap di tengah ruas)'. \
                                format(route, to_m, next_from_m, lane)

                        self.error_list.append(error_message)
                        self.insert_route_message(route, 'error', error_message)

                    if row['OVERLAP'] is True:
                        next_to_m = g_sorted.at[index + 1, to_m_col]
                        if lane is None:
                            error_message = 'Terdapat tumpang tindih antara segmen {0}-{1} dengan {2}-{3} pada rute {4}.'. \
                                format(next_from_m, next_to_m, from_m, to_m, route)
                        else:
                            error_message = 'Terdapat tumpang tindih antara segmen {0}-{1} dengan {2}-{3} pada rute {4} di jalur {5}'. \
                                format(next_from_m, next_to_m, from_m, to_m, route, lane)

                        self.error_list.append(error_message)
                        self.insert_route_message(route, 'error', error_message)

                    if row['FROM>TO'] is True:
                        if lane is None:
                            error_message = 'Segmen {0}-{1} pada rute {2} memiliki arah segmen yang terbalik, {3} > {4} lane.'. \
                                format(to_m, from_m, route, from_m_col, to_m_col)
                        else:
                            error_message = 'Segmen {0}-{1} pada rute {2} memiliki arah segmen yang terbalik, {3} > {4} lane {5}.'.\
                                format(to_m, from_m, route, from_m_col, to_m_col, lane)

                        self.error_list.append(error_message)
                        self.insert_route_message(route, 'error', error_message)

        return self

    def coordinate_check(self, routes='ALL', routeid_col="LINKID", long_col="STATO_LONG", lat_col="STATO_LAT",
                         from_m_col='STA_FROM', to_m_col='STA_TO', lane_code='LANE_CODE', spatial_ref='4326',
                         length_col='SEGMENT_LENGTH', threshold=30, at_start=False, segment_data=True, comparison='LRS',
                         window=5, write_error=True, previous_year_table=None, previous_data_mfactor=100, radius=100,
                         kwargs_comparison={}, **kwargs):
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
        :param spatial_ref: The coordinate system used to project the lat and long value from the input table
        :param threshold: The maximum tolerated distance for a submitted coordinate (in meters)
        :param at_start: If True then the inputted coordinate is assumed to be generated at the beginning of a segment.
        :param segment_data: If True then the check will measure the distance from the input point to the segment's end.
        :param comparison: Coordinate data used to check for error. ('LRS' or 'RNI-LRS')
        :param write_error: If True then error messages will be written into route message class attribute.
        :param previous_year_table: Previous year table used to check coordinate similarity.
        :param previous_data_mfactor: Conversion factor to convert previous data from-to measurement to match the input.
        :param radius: The valid radius distance in meters.
        :return:
        """
        env.workspace = self.sde_connection  # Setting up the env.workspace
        df = self.copy_valid_df()
        rni_table = self.config.table_names['rni']
        rni_routeid = self.config.table_fields['rni']['route_id']
        rni_from_m = self.config.table_fields['rni']['from_measure']
        rni_to_m = self.config.table_fields['rni']['to_measure']
        rni_lane = self.config.table_fields['rni']['lane_code']
        rni_long = self.config.table_fields['rni']['longitude']
        rni_lat = self.config.table_fields['rni']['latitude']
        rni_lane_width = self.config.table_fields['rni']['lane_width']
        initial_comparison = comparison

        if routes == 'ALL':  # Only process selected routes, if 'ALL' then process all routes in input table
            pass
        else:
            df = self.selected_route_df(df, routes)

        # Iterate for every requested routes
        for route in self.route_lane_tuple(df, routeid_col, lane_code, route_only=True):
            # Create a selected route DF
            column_selection = [routeid_col, long_col, lat_col, from_m_col, to_m_col, lane_code, length_col]
            added_cols = ['rniSegDistance', 'rniDistance', 'lrsDistance', 'measureOnLine', 'previousYear']
            df_route = df.loc[df[routeid_col] == route, (column_selection+added_cols)]

            # Get LRS route geometry
            route_geom = self.route_geometry(route, self.lrs_network, self.lrs_routeid)

            if comparison != 'LRS':
                # Get the RNI table
                rni_df = event_fc_to_df(rni_table, [rni_from_m, rni_to_m, rni_lane, rni_long, rni_lat, rni_lane_width],
                                        route, rni_routeid, self.sde_connection, True)
                rni_df[rni_from_m] = pd.Series(rni_df[rni_from_m] * self.rni_mfactor, index=rni_df.index).astype(int)
                rni_df[rni_to_m] = pd.Series(rni_df[rni_to_m] * self.rni_mfactor, index=rni_df.index).astype(int)
            else:
                rni_df = df.loc[df[routeid_col] == route, (column_selection + [rni_lane_width])]

            rni_invalid = rni_df[[rni_lat, rni_long]].isnull().any().any()  # If True then there is Null in RNI coords
            rni_segment_count = len(rni_df.groupby([rni_from_m, rni_to_m]).groups)  # The count of RNI data segment/s

            # Get the previous year data if available
            if previous_year_table is not None:

                prev_routeid = kwargs_comparison.get('route_id')
                prev_from_m = kwargs_comparison.get('from_measure')
                prev_to_m = kwargs_comparison.get('to_measure')
                prev_lane_code = kwargs_comparison.get('lane_code')
                prev_long_col = kwargs_comparison.get('long_col')
                prev_lat_col = kwargs_comparison.get('lat_col')

                prev_cols = [prev_routeid, prev_from_m, prev_to_m, prev_lane_code, prev_long_col, prev_lat_col]
                prev_df = event_fc_to_df(previous_year_table, prev_cols, route, routeid_col, self.sde_connection, True)

                if prev_df.empty is False:  # If the data available is not empty then do the conversion
                    prev_df[[prev_from_m, prev_to_m]] = prev_df[[prev_from_m, prev_to_m]].\
                        apply(lambda x: pd.Series(x*previous_data_mfactor).astype(int))
                else:
                    prev_df = None  # The previous data is empty.
            else:
                prev_df = None  # The previous data table is not defined.

            # Check if the coordinate is valid
            # TODO: In case if these coordinate range needs further adjustment
            long_condition = (df_route[long_col] > 95) & (df_route[long_col] < 142)
            lat_condition = (df_route[lat_col] > -12) & (df_route[lat_col] < 7)
            valid_coords = np.all(long_condition & lat_condition)
            self._coordinate_status[route] = [0]  # Initiate the route's status value.

            if not valid_coords:
                msg = "Rute {0} memiliki koordinat yang tidak valid.".format(route)
                self.insert_route_message(route, 'error', msg)
                continue

            if rni_df.empty and (comparison != 'LRS'):
                return self

            if rni_segment_count < 2 and (comparison != 'LRS'):
                comparison = 'RNIPoint-LRS'
                segment_data = False
            else:
                comparison = initial_comparison

            if rni_invalid:
                comparison = 'LRS'

            # Add distance column based on the comparison request
            if segment_data and (comparison == 'LRS'):
                df_route[added_cols] = df_route.apply(lambda _x: coordinate.distance_series(_x[lat_col],
                                                                                            _x[long_col],
                                                                                            route_geom,
                                                                                            from_m=_x[from_m_col],
                                                                                            to_m=_x[to_m_col],
                                                                                            lane=_x[lane_code],
                                                                                            projections=spatial_ref,
                                                                                            at_start=at_start,
                                                                                            previous_df=prev_df,
                                                                                            kwargs_comparison=kwargs_comparison)
                                                      , axis=1)
            if segment_data and (comparison == 'RNIseg-LRS'):
                df_route[added_cols] = df_route.apply(lambda _x: coordinate.distance_series(_x[lat_col],
                                                                                            _x[long_col],
                                                                                            route_geom,
                                                                                            from_m=_x[from_m_col],
                                                                                            to_m=_x[to_m_col],
                                                                                            lane=_x[lane_code],
                                                                                            projections=spatial_ref,
                                                                                            at_start=at_start,
                                                                                            rni_df=rni_df,
                                                                                            rni_from_m=rni_from_m,
                                                                                            rni_to_m=rni_to_m,
                                                                                            rni_lane_code=rni_lane,
                                                                                            rni_lat=rni_lat,
                                                                                            rni_long=rni_long,
                                                                                            previous_df=prev_df,
                                                                                            kwargs_comparison=kwargs_comparison)
                                                      , axis=1)
            if segment_data and (comparison == 'RNIline-LRS'):
                rni_line = coordinate.to_polyline(rni_df, rni_from_m, rni_long, rni_lat, rni_to_m, projections=spatial_ref)
                df_route[added_cols] = df_route.apply(lambda _x: coordinate.distance_series(_x[lat_col],
                                                                                            _x[long_col],
                                                                                            route_geom,
                                                                                            from_m=_x[from_m_col],
                                                                                            to_m=_x[to_m_col],
                                                                                            lane=_x[lane_code],
                                                                                            projections=spatial_ref,
                                                                                            at_start=at_start,
                                                                                            rni_df=rni_df,
                                                                                            rni_from_m=rni_from_m,
                                                                                            rni_to_m=rni_to_m,
                                                                                            rni_lane_code=rni_lane,
                                                                                            rni_lat=rni_lat,
                                                                                            rni_long=rni_long,
                                                                                            rni_polyline=rni_line,
                                                                                            previous_df=prev_df,
                                                                                            kwargs_comparison=kwargs_comparison)
                                                      , axis=1)

            if not segment_data and (comparison == 'LRS'):
                df_route[added_cols] = df_route.apply(lambda _x: coordinate.distance_series(_x[lat_col],
                                                                                            _x[long_col],
                                                                                            route_geom,
                                                                                            projections=spatial_ref,
                                                                                            ), axis=1)
            if not segment_data and (comparison == 'RNIline_LRS'):
                rni_line = coordinate.to_polyline(rni_df, rni_from_m, rni_long, rni_lat, rni_to_m, projections=spatial_ref)
                df_route[added_cols] = df_route.apply(lambda _x: coordinate.distance_series(_x[lat_col],
                                                                                            _x[long_col],
                                                                                            route_geom,
                                                                                            projections=spatial_ref,
                                                                                            rni_polyline=rni_line
                                                                                            ), axis=1)

            if comparison == 'RNIPoint-LRS':
                df_route[added_cols] = df_route.apply(lambda _x: coordinate.distance_series(_x[lat_col],
                                                                                            _x[long_col],
                                                                                            route_geom,
                                                                                            rni_df=rni_df,
                                                                                            rni_lat=rni_lat,
                                                                                            rni_long=rni_long), axis=1)

            elif comparison not in ['LRS', 'RNIline-LRS', 'RNIseg-LRS', 'RNIPoint-LRS']:
                raise TypeError("Comparison is invalid.")

            coordinate_error = coordinate.FindCoordinateError(df_route, route, from_m_col, to_m_col, lane_code,
                                                              comparison=comparison, long_col=long_col, lat_col=lat_col)
            if not segment_data:
                errors = df_route.loc[df_route['lrsDistance'] > threshold, [from_m_col, to_m_col, lane_code]]

                if (comparison == 'RNIline-LRS') or (comparison == 'RNIPoint-LRS'):
                    errors = df_route.loc[(df_route['rniDistance'] > threshold) & (df_route['lrsDistance'] > threshold),
                                          [from_m_col, to_m_col, lane_code]]

                if (from_m_col is None) or (to_m_col is None) or (lane_code is None):
                    if len(errors) != 0:
                        error_message = "Rute {0} memiliki koordinat yang berjarak lebih dari {1}m dari geometri ruas.".\
                            format(route, threshold)
                        if write_error:
                            self.insert_route_message(route, 'error', error_message)
                else:
                    for index, row in errors.iterrows():
                        from_m = row[from_m_col]
                        to_m = row[to_m_col]
                        lane = row[lane_code]
                        error_message = "Rute {0} pada segmen {1}-{2} {3} memiliki koordinat yang berjarak lebih dari {4}m dari geometri rute.".\
                            format(route, from_m, to_m, lane, threshold)
                        if write_error:
                            self.insert_route_message(route, 'error', error_message)

            if segment_data and (comparison == 'LRS'):
                coordinate_error.find_distance_error('lrsDistance', window=window, threshold=threshold)
                coordinate_error.find_end_error(route_geom, 'start', radius=radius)
                coordinate_error.find_end_error(route_geom, 'end', radius=radius)

                if lane_code is not None:
                    coordinate_error.find_lane_error(rni_df=rni_df)

                coordinate_error.close_to_zero('lrsDistance')

                if previous_year_table is not None:
                    coordinate_error.close_to_zero('previousYear')

                coordinate_error.find_non_monotonic('measureOnLine')
                coordinate_error.find_segment_len_error('measureOnLine')

            if segment_data and ((comparison == 'RNIseg-LRS') or (comparison == 'RNIline-LRS')):
                coordinate_error.distance_double_check('rniDistance', 'lrsDistance', window=window, threshold=threshold)
                coordinate_error.find_end_error(rni_line, 'start', 'rniDistance', radius=radius)
                coordinate_error.find_end_error(rni_line, 'end', 'rniDistance', radius=radius)

                if lane_code is not None:
                    coordinate_error.find_lane_error(rni_df=rni_df)

                coordinate_error.close_to_zero('rniDistance')

                if previous_year_table is not None:
                    coordinate_error.close_to_zero('previousYear')

                coordinate_error.find_non_monotonic('measureOnLine')
                coordinate_error.find_segment_len_error('measureOnLine')

            for error_message in coordinate_error.error_msg:
                self.insert_route_message(route, 'error', error_message)
            for warning_msg in coordinate_error.warning_msg:
                self.insert_route_message(route, 'VerifiedWithWarning', warning_msg)

        return self

    def lane_sequence_check(self, routes='ALL', routeid_col='LINKID', from_m_col='STA_FROM',
                            to_m_col='STA_TO', lane_code='LANE_CODE', **kwargs):
        """
        This class method check for lane code sequence, every segment lane should start with L1/R1 and increment by one
        eg. L1, L2, L3 not L1, L3, L4 or L2, L3, L4.
        :param lane_code: The lane code column.
        :return:
        """
        df = self.copy_valid_df()
        df = self.selected_route_df(df, routes)

        # Check if all lane code is correct within the specified domain
        lane_domains = self.column_details.get('LANE_CODE').get('domain')
        if lane_domains is not None:
            all_correct = np.all(df[lane_code].isin(lane_domains))

            if not all_correct:  # If there is invalid value then don't proceed.
                return self

        df['SIDE'] = df[lane_code].apply(lambda x: str(x[0]))
        segment_g = df.groupby([routeid_col, from_m_col, to_m_col, 'SIDE'])[lane_code].unique().reset_index()

        segment_g['FIRST_LANE_EXIST'] = segment_g[lane_code].\
            apply(lambda x: np.any((np.array(x) == 'R1') | (np.array(x) == 'L1')))

        segment_g['LANE_SEQ'] = segment_g[lane_code].\
            apply(lambda x: np.sort([int(_[1:]) for _ in x]))

        segment_g['CORRECT_SEQ'] = segment_g['LANE_SEQ'].\
            apply(lambda x: np.all(np.abs(np.diff(x)) == 1))

        error_rows = segment_g.loc[~(segment_g['FIRST_LANE_EXIST'] | segment_g['CORRECT_SEQ'])]

        for index, row in error_rows.iterrows():
            first_lane_exist = row['FIRST_LANE_EXIST']
            correct_seq = row['CORRECT_SEQ']
            route = row[routeid_col]
            from_m = row[from_m_col]
            to_m = row[to_m_col]
            side = row['SIDE']
            lanes = row[lane_code]

            if not first_lane_exist:
                if side == 'L':
                    first_lane = 'L1'
                else:
                    first_lane = 'R1'

                error_msg = "Rute {0} pada segmen {1}-{2} di sisi {3} tidak memiliki lajur {4}.".\
                    format(route, from_m, to_m, side, first_lane)
                self.insert_route_message(route, 'error', error_msg)

            if not correct_seq:
                error_msg = 'Rute {0} pada segmen {1}-{2} di sisi {3} memiliki kode lajur yang tidak berurutan yaitu {4}.'.\
                    format(route, from_m, to_m, side, lanes)
                self.insert_route_message(route, 'error', error_msg)

        return self

    def lane_code_check(self, routes='ALL', routeid_col='LINKID', lane_code='LANE_CODE', from_m_col='STA_FROM',
                        to_m_col='STA_TO', find_no_match=False, **kwargs):
        """
        This function checks the lane code combination for all segment in the input table, the segment interval value
        has to be the same with interval value in the RNI Table.
        :param rni_table: RNI event table
        :param routes: requested routes, if 'ALL' then all routes in the input table will be processed
        :param lane_code: lane code column in the input table
        :param routeid_col: The Route ID column in the input table.
        :param from_m_col: Column in the input table which contain the From Measurement value.
        :param to_m_col: Column in the input table which  contain the To Measurement value.
        :param rni_routeid: The Route ID column in the RNI Table.
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

        rni_table = self.config.table_names['rni']
        rni_routeid = self.config.table_fields['rni']['route_id']
        rni_from_col = self.config.table_fields['rni']['from_measure']
        rni_to_col = self.config.table_fields['rni']['to_measure']
        rni_lane_col = self.config.table_fields['rni']['lane_code']

        if routes == 'ALL':  # If 'ALL' then process all available route in input table
            pass
        else:
            # Else then only process the selected routes
            df = self.selected_route_df(df, routes)

        # Iterate over all requested routes
        for route in self.route_lane_tuple(df, routeid_col, lane_code, route_only=True):
            df_route = df.loc[df[routeid_col] == route]  # Create a DataFrame containing only selected routes

            # The RNI DataFrame
            search_field = [rni_routeid, rni_from_col, rni_to_col, rni_lane_col]
            df_rni = event_fc_to_df(rni_table, search_field, route, rni_routeid, self.sde_connection, is_table=True)
            df_rni[rni_from_col] = pd.Series(df_rni[rni_from_col]*self.rni_mfactor).round(2).astype(int)
            df_rni[rni_to_col] = pd.Series(df_rni[rni_to_col]*self.rni_mfactor).round(2).astype(int)

            if len(df_rni) == 0:  # Check if the route exist in the RNI Table
                error_message = "Ruas {0} tidak terdapat pada table RNI.".format(route)  # Create an error message
                self.error_list.append(error_message)
                self.insert_route_message(route, 'error', error_message)
            else:
                # Create the join key for both DataFrame
                input_merge_key = [routeid_col, from_m_col, to_m_col]
                rni_merge_key = [rni_routeid, rni_from_col, rni_to_col]

                # Create a groupby DataFrame
                input_groupped = df_route.groupby(by=input_merge_key)[lane_code].unique().reset_index()
                rni_groupped = df_rni.groupby(by=rni_merge_key)[rni_lane_col].unique().reset_index()

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

                if len(df_both) == 0:
                    error_message = "Rute {0} tidak memiliki interval segmen yang cocok dengan data RNI".format(route)
                    self.insert_route_message(route, 'error', error_message)
                    continue

                # Modify the lane_code variable
                if lane_code == rni_lane_col:
                    input_lane_code = lane_code+'_INPUT'
                    merge_rni_lane_col = rni_lane_col+'_RNI'
                else:
                    input_lane_code = lane_code
                    merge_rni_lane_col = rni_lane_col

                # Create a column containing intersection count of lane code combination
                # between the input table and RNI Table
                df_both.loc[:, 'lane_intersect_count'] = pd.Series([len(set(a).intersection(b)) for a, b in
                                                                    zip(df_both[input_lane_code], df_both[merge_rni_lane_col])],
                                                                   index=df_both.index)

                # Create a column containing the lane diff of Input - RNI
                df_both.loc[:, 'input-RNI'] = pd.Series([np.setdiff1d(a, b) for a, b in
                                                         zip(df_both[input_lane_code], df_both[merge_rni_lane_col])],
                                                        index=df_both.index)

                # Create a column containing the lane diff of RNI - Input
                df_both.loc[:, 'RNI-input'] = pd.Series([np.setdiff1d(b, a) for a, b in
                                                         zip(df_both[input_lane_code], df_both[merge_rni_lane_col])],
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
                    error_message = 'Segmen {0} pada rute {1} tidak memiliki lane {2} dan memiliki lane {3} yang tidak terdapat pada tabel RNI.'.\
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

    def lane_direction_check(self, routes='ALL', routeid_col='LINKID', lane_code='LANE_CODE', from_m_col='STA_FROM',
                             to_m_col='STA_TO', direction_col='SURVEY_DIREC', **kwargs):
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

            message = "Rute {0} pada segmen {1}-{2} {3} memiliki arah yang tidak konsisten dengan kode lajur.".\
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
                                is_table=True)  # The input routes in RNI Table DF in np.array format
        rni_routes = np.array(rni_df[rni_route_col])  # The available routes in the RNI Table in np.array
        missing_route = np.setdiff1d(input_routes, rni_routes)  # The unavailable route in the RNI Table

        for route in self.missing_route:
            message = "Rute {0} belum memiliki data RNI.".format(route)  # Create an error message
            self.insert_route_message(route, 'error', message)  # Insert the error message

        return missing_route.tolist()

    def rni_roadtype_check(self, road_type_details, routes='ALL', routeid_col='LINKID', from_m_col='STA_FROM', to_m_col='STA_TO', lane_codes='LANE_CODE',
                           median_col='MED_WIDTH', road_type_col='ROAD_TYPE', **kwargs):
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
            agg({lane_codes: ['nunique', 'unique'], median_col: ['sum', 'nunique'],
                 road_type_col: ['unique', 'nunique']}).reset_index()

        for index, row in df_groupped.iterrows():
            route = str(row[routeid_col][0])
            from_m = str(row[from_m_col][0])
            to_m = str(row[to_m_col][0])
            road_type_code_list = row[road_type_col]['unique']  # Get the road type list
            road_type_count = int(row[road_type_col]['nunique'])

            # If the road type is more than one in a single segment then give error message
            if road_type_count == 1:

                road_type_code = str(road_type_code_list[0])  # The road type in string
                if road_type_code not in road_type_details.keys():
                    continue

                lane_count = road_type_details[road_type_code]['lane_count']  # The required lane count for specified type
                direction = road_type_details[road_type_code]['direction']  # The direction required
                median_exist = road_type_details[road_type_code]['median']  # The median existence requirement

                input_lane_count = row[lane_codes]['nunique']  # The lane count from the input
                input_direction = len(set([x[0] for x in row[lane_codes]['unique']]))  # The direction from input 1 or 2 dir
                input_median = row[median_col]['sum']  # The total median from the input
                input_median_count = row[median_col]['nunique']

                if input_lane_count != lane_count:
                    result = "Rute {0} pada segmen {1}-{2} memiliki jumlah lane ({3} lane) yang tidak sesuai dengan road type {4} ({5} lane).".\
                        format(route, from_m, to_m, input_lane_count, road_type_code, lane_count)
                    self.insert_route_message(route, 'error', result)

                if input_direction != direction:
                    result = "Rute {0} pada segmen {1}-{2} memiliki arah ({3} arah) yang tidak sesuai dengan road type {4} ({5} arah).".\
                        format(route, from_m, to_m, input_direction, road_type_code, direction)
                    self.insert_route_message(route, 'error', result)

                if (median_exist and (input_median == 0)) or (not median_exist and (input_median != 0)):
                    result = "Rute {0} pada segmen {1}-{2} memiliki median yang tidak sesuai dengan road type {3}.".\
                        format(route, from_m, to_m, road_type_code)
                    self.insert_route_message(route, 'error', result)

                if input_median_count > 1:
                    msg = "Rute {0} pada segmen {1}-{2} memiliki nilai median yang tidak konsisten.".\
                        format(route, from_m, to_m)
                    self.insert_route_message(route, 'error', msg)
            else:
                result = "Rute {0} pada segmen {1}-{2} memiliki road type yang tidak konsisten {3}.".\
                    format(route, from_m, to_m, road_type_code_list)
                self.insert_route_message(route, 'error', result)

        return self

    def rtc_duration_check(self, duration=3, routes='ALL', routeid_col='LINKID', surv_date_col='SURVEY_DATE',
                           hours_col='SURVEY_HOURS', minute_col='SURVEY_MINUTE', direction_col='SURVEY_DIREC',
                           interval=15, **kwargs):
        """
        This class method will check the RTC survey direction for every available route in the input table.
        :param duration: The survey duration (in days), the default is 3 days.
        :param routes: The selected routes to be processed.
        :param routeid_col: The RouteID column in the event DataFrame.
        :param surv_date_col: The survey date column in the event DataFrame.
        :param hours_col: The hours column in the event DataFrame.
        :param minute_col: The minute column in the event DataFrame.
        :param direction_col: The survey direction column of the input table.
        :param interval: The interval duration in minutes.
        :return:
        """
        def timestamp_col(date, hour, minute, col='_timestamp'):
            """
            This function will populate a column with a timestamp data for every row.
            :param date: Date column of the input table.
            :param hour: Hour column of the input table.
            :param minute: Minute column of the input table.
            :param col: The timestamp column.
            :return:
            """
            for index, row in df_route_dir.iterrows():
                row_date = row[date]
                row_hour = row[hour]
                row_minute = row[minute]
                df_route_dir.loc[index, [col]] = self.rtc_time_stamp(row_date, row_hour, row_minute)

            df_route_dir[col] = pd.to_datetime(df_route_dir[col])

        df = self.copy_valid_df()  # Create a copy of input table DataFrame
        df['_timestamp'] = pd.Series(None)

        if routes == 'ALL':
            pass
        else:
            df = self.selected_route_df(df, routes)  # If there is a route request then only process the selected route

        for route, direction in self.route_lane_tuple(df, routeid_col, direction_col):  # Iterate over all available route
            df_route_dir = df.loc[(df[routeid_col] == route) & (df[direction_col] == direction)].reset_index()  # Create a route and lane DataFrame
            timestamp_col(surv_date_col, hours_col, minute_col)

            survey_start_index = df_route_dir['_timestamp'].idxmin()  # The survey start row index
            survey_start_date = df_route_dir.at[survey_start_index, surv_date_col]  # The survey start date
            survey_start_hour = df_route_dir.at[survey_start_index, hours_col]  # The survey start hours
            survey_start_minutes = df_route_dir.at[survey_start_index, minute_col] - interval  # The survey start minutes
            start_timestamp = self.rtc_time_stamp(survey_start_date, survey_start_hour, survey_start_minutes)

            survey_end_index = df_route_dir['_timestamp'].idxmax()  # The survey end row index
            survey_end_date = df_route_dir.at[survey_end_index, surv_date_col]  # The survey end date
            survey_end_hour = df_route_dir.at[survey_end_index, hours_col]  # The survey end hours
            survey_end_minutes = df_route_dir.at[survey_end_index, minute_col]  # The survey end minutes
            end_timestamp = self.rtc_time_stamp(survey_end_date, survey_end_hour, survey_end_minutes)

            required_delta = timedelta(minutes=duration*24*60)  # The required survey duration

            if end_timestamp < (required_delta+start_timestamp):
                actual_delta = abs((end_timestamp-(required_delta+start_timestamp)).total_seconds()/60)  # The actual survey duration in minutes
                duration_in_h = duration*24  # The required survey duration in hours
                result = "Rute {0} pada arah {1} memiliki kekurangan durasi survey RTC sebanyak {2} menit dari total {3} jam yang harus dilakukan.".\
                    format(route, direction, actual_delta, duration_in_h)
                self.insert_route_message(route, 'error', result)

        return self

    def rtc_time_interval_check(self, interval=15, routes='ALL', routeid_col='LINKID', surv_date_col='SURVEY_DATE',
                                hours_col='SURVEY_HOURS', minute_col='SURVEY_MINUTE', direction_col='SURVEY_DIREC',
                                **kwargs):
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
            df_route_dir.sort_values([surv_date_col, hours_col, minute_col], inplace=True)
            df_route_dir.reset_index(inplace=True)  # Reset the index

            for index, row in df_route_dir.iterrows():
                row_date = row[surv_date_col]
                row_hour = row[hours_col]
                row_minute = row[minute_col]

                row_timestamp = self.rtc_time_stamp(row_date, row_hour, row_minute)
                row_timestamp_start = self.rtc_time_stamp(row_date, row_hour, (row_minute-interval))
                date_timestamp_isof = row[surv_date_col].date().isoformat()
                row_timestamp_isof = row_timestamp_start.date().isoformat()

                if index == 0:
                    pass
                else:
                    delta_start_end = (row_timestamp - start_timestamp).total_seconds()/60  # Interval in minutes

                    if delta_start_end != interval:  # If the delta does not match the requirement
                        end_time_str = row_timestamp.strftime('%d/%m/%Y %H:%M')  # Start time in string format
                        start_time_str = start_timestamp.strftime('%d/%m/%Y %H:%M')  # End time in string format
                        result = "Survey RTC di rute {0} pada arah {1} di interval survey {2} - {3} tidak berjarak {4} menit.".\
                            format(route, direction, start_time_str, end_time_str, interval)
                        self.insert_route_message(route, 'error', result)

                if date_timestamp_isof != row_timestamp_isof:  # Find the date which does not match with the hours
                    result = "Waktu survey RTC di rute {0} {1} pada tanggal {2} jam {3} menit {4} seharusnya memiliki tanggal {5}.".\
                        format(route, direction, date_timestamp_isof, row_hour, row_minute, row_timestamp_isof)
                    self.insert_route_message(route, 'error', result)

                start_timestamp = row_timestamp

        return self

    def rtc_lane_vehicle(self, routes="ALL", min_width=2.5, not_allowed='NUM_VEH7C'):
        """
        This class method checks if column in 'not_allowed' column parameter has a value which is not zero when the
        lane width is/or less than the minimal width (in meters).
        :param routes: Route selection parameter.
        :param min_width: Mini
        :param not_allowed:
        :return:
        """
        df = self.copy_valid_df()
        if routes == "ALL":
            pass
        else:
            df = self.selected_route_df(df, routes)

        rni_table = self.config.table_names['rni']
        rni_routeid = self.config.table_fields['route_id']
        rni_from_m = self.config.table_fields['from_measure']
        rni_to_m = self.config.table_fields['to_measure']
        rni_lane_code = self.config.table_fields['lane_code']
        rni_lane_w = self.config.table_fields['lane_width']
        rni_search_field = [rni_routeid, rni_from_m, rni_to_m, rni_lane_code, rni_lane_w]
        rni_df = event_fc_to_df(rni_table, rni_search_field, routes, rni_routeid, self.sde_connection, is_table=True)

    def rni_compare_surftype(self, routeid_col='LINKID', from_m_col='STA_FROM', to_m_col='STA_TO',
                             surftype_col='SURF_TYPE', lane_code='LANE_CODE', routes='ALL',
                             kwargs_comparison=None, **kwargs):
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
        route_list = self.route_lane_tuple(df, routeid_col, None, True)  # List of all route in the input DataFrame.

        for route in route_list:
            df_route = self.selected_route_df(df, route)  # The input selected route DataFrame
            df_route = df_route[[routeid_col, from_m_col, to_m_col, surftype_col, lane_code]]
            merged = add_rni_data(df_route, routeid_col, from_m_col, to_m_col, lane_code, self.sde_connection,
                                  surftype_col, 'outer', rni_kwargs=kwargs_comparison)
            merged.dropna(inplace=True)

            if merged.empty:  # If the merged resulting empty DataFrame then continue.
                continue

            input_surftype = surftype_col + '_x'  # The surface type column for input and ref after merge
            ref_surftype = surftype_col + '_y'

            # Add surface group and category column
            merged = Kemantapan.static_grading(merged, input_surftype, None, Kemantapan.group_details(), None,
                                               surftype_group='input_group', surftype_cat='input_cat')

            if merged is None:
                return self

            merged = Kemantapan.static_grading(merged, ref_surftype, None, Kemantapan.group_details(), None,
                                               surftype_group='ref_group', surftype_cat='ref_cat')

            if merged is None:
                return self

            merged['status'] = pd.Series(np.nan)
            merged.loc[(merged['ref_cat'] == 'p') & (merged['input_cat'] == 'up'), ['status']] = 'error'
            # merged.loc[(merged['ref_cat'] == 'unpaved') & (merged['input_cat'] == 'paved'), ['status']] = 'ToBeReviewed'

            for index, row in merged.loc[merged['status'].notnull()].iterrows():
                from_m = row[from_m_col]
                to_m = row[to_m_col]
                lane = row[lane_code]
                input_cat = row['input_cat']
                ref_cat = row['ref_cat']
                msg_status = row['status']

                msg = "Rute {0} pada segmen {1}-{2} lajur {3} memiliki tipe perkerasan yang berbeda dengan data tahun sebelumnya, yaitu (input = {4}, tahun sebelumnya = {5}).".\
                    format(route, from_m, to_m, lane, input_cat, ref_cat)
                self.insert_route_message(route, msg_status, msg)

        return self

    def rni_compare_surfwidth(self, comp_fc, comp_route_col, comp_from_col, comp_to_col, comp_lane_width, year_comp,
                              routeid_col='LINKID', from_m_col='STA_FROM', to_m_col='STA_TO',
                              rni_lane_width='LANE_WIDTH', routes='ALL', **kwargs):
        """
        This class method will compare the road width to a comparison feature class, if there is a difference in the
        road width percentage in the requested routes, then an error message will be written.
        :param comp_fc: Feature Class used for comparison.
        :param comp_route_col: RouteID field in the comparison feature class.
        :param comp_from_col: From Measure field in the comparison feature class.
        :param comp_to_col: To Measure field in the ocmparison feature class.
        :param comp_lane_width: Lane Width in the comparison feature class.
        :param year_comp: The year of comparison feature class.
        :param routeid_col: The input RNI RouteID field.
        :param from_m_col: The input RNI From Measure field.
        :param to_m_col: The input RNI To Measure field.
        :param rni_lane_width: The input RNI Lane Width field.
        :param routes: The requested routes.
        :return:
        """
        df = self.selected_route_df(self.copy_valid_df(), routes)  # Create a valid DataFrame copy
        route_list = self.route_lane_tuple(df, routeid_col, None, True)

        for route in route_list:  # Iterate over all available route in the input table.
            df_comp = event_fc_to_df(comp_fc, [comp_route_col, comp_from_col, comp_to_col, comp_lane_width], route,
                                     comp_route_col, self.sde_connection, is_table=False, include_all=True)
            df_route = self.selected_route_df(df, route)

            if len(df_comp) == 0:
                result = 'Rute {0} tidak terdapat pada data tahun {1}.'.format(route, year_comp)
                self.insert_route_message(route, 'ToBeReviewed', result)
                continue

            input_surfwidth = RNIRouteDetails(df_route, routeid_col, from_m_col, to_m_col, rni_lane_width,
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

    def rni_surftype_check(self, routes='ALL', routeid_col='LINKID', from_m_col='STA_FROM', to_m_col='STA_TO',
                           lane_code='LANE_CODE', surftype_col='SURF_TYPE', **kwargs):
        """
        This class method check for consistency in surface type for every segment in the input table. A single segment
        should have same surface type for every available lane in that segment, otherwise that segment will be reviewed.
        :param routes: Route selection.
        :param routeid_col: Route ID column.
        :param from_m_col: From Measure column.
        :param to_m_col: To Measure column.
        :param lane_code: Lane code column.
        :param surftype_col: Surface type column.
        :return:
        """
        df = self.selected_route_df(self.copy_valid_df(), routes)  # Route selection
        grouped = df.groupby([routeid_col, from_m_col, lane_code])
        group_surface_count = grouped[surftype_col].nunique().reset_index()  # Count of surface type unique value
        error_row = group_surface_count.loc[group_surface_count[surftype_col] > 1]

        for index, row in error_row.iterrows():
            route = row[routeid_col]
            from_m = row[from_m_col]
            to_m = row[to_m_col]

            msg = "Rute {0} pada segmen {1}-{2} memiliki jalur dengan {3} yang tidak konsisten antara satu sama lain.".\
                format(route, from_m, to_m, surftype_col)
            self.insert_route_message(route, "ToBeReviewed", msg)

        return self

    def rni_median_inn_shwidth(self, r_shwidth, l_shwidth, routes='ALL', routeid_col='LINKID',
                               from_m_col='STA_FROM', to_m_col='STA_TO', lane_code='LANE_CODE', median_col='MED_WIDTH',
                               empty_as_null=False, **kwargs):
        """
        This class method check for consistency between the value of median width and inner shoulder width columns,
        inner shoulder width should only available when the median is also available, otherwise an error message will be
        raised.
        :param r_shwidth: Right inner shoulder width column.
        :param l_shwidth: Left inner shoulder witdth column.
        :param routes: Route selection.
        :param routeid_col: Route ID column.
        :param from_m_col: From measure column.
        :param to_m_col: To measure column.
        :param lane_code: Lane code column.
        :param median_col: Median width column.
        :param empty_as_null: If True then empty value is recognized as Null, otherwise recognized as 0.
        :return:
        """
        df = self.selected_route_df(self.copy_valid_df(), routes)  # Route selection
        filtered = df.groupby([routeid_col, to_m_col, from_m_col]).filter(lambda x: np.any(x[r_shwidth].notnull()) &
                                                                                    np.any(x[l_shwidth].notnull()) &
                                                                                    np.any(x[median_col].notnull()))
        grouped = filtered.groupby([routeid_col, to_m_col, from_m_col])

        agg_func = {
            median_col: (lambda x: x.dropna().values[0]),  # Get the first non N/A value.
            lane_code: (lambda x: x.str.get(0).nunique()),  # The direction count 1(L or R) or 2 (L and R).
            r_shwidth: (lambda x: x.dropna().values[0]),
            l_shwidth: (lambda x: x.dropna().values[0])
        }

        grouped_val = grouped.aggregate(agg_func).reset_index()
        no_median = np.isclose(grouped_val[median_col], 0)  # Median equal to zero
        no_right_sh = np.isclose(grouped_val[r_shwidth], 0)  # Shoulder width equal to zero
        no_left_sh = np.isclose(grouped_val[l_shwidth], 0)

        # Error type
        # With median but has no right shoulder OR left shoulder value.
        # Without median but has right shoulder OR left shoulder value.
        error_rows = grouped_val.loc[(no_median & (~no_right_sh | ~no_left_sh)) |
                                     (~no_median & (no_right_sh | no_left_sh))]

        # if type(inn_shwidth_cols) == list:
        #     not_zero = np.any(~np.isclose(df[inn_shwidth_cols], 0), axis=1)
        #     not_null = np.any(df[inn_shwidth_cols].notnull(), axis=1)
        # else:
        #     not_zero = ~np.isclose(df[inn_shwidth_cols], 0)
        #     not_null = df[inn_shwidth_cols].notnull()
        #
        # with_shwidth = not_null & not_zero
        # error_rows = df.loc[no_median & with_shwidth]

        for index, row in error_rows.iterrows():
            route = row[routeid_col]
            from_m = row[from_m_col]
            to_m = row[to_m_col]
            lane = row[lane_code]
            median_val = row[median_col]
            zero_median = np.isclose(median_val, 0)

            if zero_median:
                msg = "Rute {0} pada segmen {1}-{2} tidak memiliki median namun memiliki nilai {3} atau {4}.".\
                    format(route, from_m, to_m, r_shwidth, l_shwidth)
            else:
                msg = "Rute {0} pada segmen {1}-{2} memiliki median namun tidak memiliki nilai {3} atau {4}.".\
                    format(route, from_m, to_m, r_shwidth, l_shwidth)

            self.insert_route_message(route, 'error', msg)

        return self

    def pci_asp_check(self, routes='ALL', asp_pref='AS_', routeid_col='LINKID', from_m_col='STA_FROM',
                      to_m_col='STA_TO', lane_code='LANE_CODE', segment_len='SEGMENT_LENGTH', **kwargs):
        """
        This method will check the consistency in as_ column value compared to the maximum calculated value
        (lane width*segment length). The value of as_ column should not exceed the calculated value, otherwise an error
        message will be written.
        :param asp_pref: Asphalt column of PCI table prefix(as_alg_crack, as_edge_crack, etc)
        :param routes: The route selections.
        :return:
        """
        df = self.copy_valid_df()  # Create a valid DataFrame copy

        col_list = df.columns.tolist()
        col_mask2 = np.char.startswith(col_list, asp_pref)
        rni_lane_width = self.config.table_fields['rni']['lane_width']

        if not col_mask2.any():
            return self  # The specified prefix does not match any column.

        if routes == 'ALL':  # Check for route request
            pass
        else:
            df = self.selected_route_df(df, routes)

        route_list = self.route_lane_tuple(df, routeid_col, lane_code, route_only=True)
        for route in route_list:
            df_route = df.loc[df[routeid_col] == route]
            merge = add_rni_data(df_route, routeid_col, from_m_col, to_m_col, lane_code, self.sde_connection,
                                 added_column=rni_lane_width, mfactor=self.rni_mfactor)

            if merge is None:  # If the RNI DataFrame is empty.
                error_message = "Data RNI rute {0} tidak tersedia".format(route)
                self.insert_route_message(route, 'error', error_message)
                continue

            calc_asp_col = '_calc'
            merge[calc_asp_col] = pd.Series(None)
            merge.loc[:, [calc_asp_col]] = merge[segment_len]*merge[rni_lane_width]*1000

            for index, row in merge.iterrows():
                calc_val = row[calc_asp_col]
                from_m = row[from_m_col]
                to_m = row[to_m_col]
                lane = row[lane_code]
                asp_series = row[col_mask2].mask(row[col_mask2] <= calc_val)
                asp_series.dropna(inplace=True)

                for col, value in asp_series.iteritems():
                    error_message = 'Rute {0} pada segmen {1}-{2} {3} memiliki nilai {4} ({5}) yang melebihi nilai luas segmen ({6}).'.\
                        format(route, from_m, to_m, lane, col, value, calc_val)
                    self.insert_route_message(route, 'error', error_message)

        return self

    def pci_val_check(self, rg_pref='RG_', asp_pref='AS_', pci_col='PCI', routeid_col='LINKID', from_m_col='STA_FROM',
                      to_m_col='STA_TO', lane_code='LANE_CODE', routes='ALL', min_value=0, max_value=100,
                      check_null=True, **kwargs):
        """
        This class method will check for consistency between the value of PCI and the RG_x and AS_x columns.
        :param rg_pref: The prefix of Rigid column.
        :param asp_pref: The prefix of Asphalt column.
        :param pci_col: The PCI column.
        :param routeid_col: The Route ID column.
        :param from_m_col: The From Measure column.
        :param to_m_col: The To Measure column.
        :param lane_code: The Lane Code.
        :param routes: The route selections.
        :param min_value: The minimum value of pci_col.
        :param max_value: The maximum value of pci_col.
        :param check_null: Check for consistency between asp_ and rg_ column if the pci_col value is Null.
        :return:
        """
        df = self.copy_valid_df()  # Create the valid DataFrame copy

        if routes == 'ALL':  # Check for route request
            pass
        else:
            df = self.selected_route_df(df, routes)  # Create a DataFrame with selected route

        col_list = df.columns.tolist()
        rg_mask = np.char.startswith(col_list, rg_pref)
        asp_mask = np.char.startswith(col_list, asp_pref)

        route_list = self.route_lane_tuple(df, routeid_col, lane_code, route_only=True)
        for route in route_list:
            df_route = df.loc[df[routeid_col] == route]
            pci_all_null = np.all(df_route[pci_col].isnull())
            df_pci = pd.DataFrame()

            if not pci_all_null:
                # Check for the consistency for min and max value.
                if (min_value is not None) and (max_value is not None):
                    df_pci = df_route.loc[(df_route[pci_col] == min_value) |
                                          (df_route[pci_col] == max_value)]
                elif (min_value is None) and (max_value is not None):
                    df_pci = df_route.loc[df_route[pci_col] == max_value]
                elif (max_value is None) and (min_value is not None):
                    df_pci = df_route.loc[df_route[pci_col] == min_value]
                else:
                    raise TypeError("max_value and min_value could not be None at the same time.")

            if check_null:
                null_pci = df_route.loc[df_route[pci_col].isnull()]

                if pci_all_null:
                    df_pci = df_route
                else:
                    if df_pci.empty:
                        df_pci = null_pci
                    else:
                        df_pci = df_pci.append(null_pci)

            for index, row in df_pci.iterrows():
                from_m = row[from_m_col]
                to_m = row[to_m_col]
                lane = row[lane_code]
                pci_val = row[pci_col]

                if (type(pci_val) == str) or (type(pci_val) == unicode):
                    pci_val = str(pci_val)
                elif np.isnan(pci_val):  # Convert from numpy nan to python None.
                    pci_val = None

                asp_rg = row[rg_mask | asp_mask]
                asp_rg_cond = asp_rg.mask(asp_rg == 0)
                asp_rg_allzero = np.all(asp_rg_cond.isnull())  # True if all value in asp_rg_cond is zero.
                asp_rg_allnull = np.all(asp_rg.isnull())  # True if all value in asp_rg_cond is Null.

                if (pci_val == min_value) and asp_rg_allzero and (min_value is not None):
                    error_message = 'Rute {0} pada segmen {1}-{2} lane {3} memiliki nilai {4}={5} namun nilai kerusakan perkerasan aspal ataupun rigid yang sepenuhnya bernilai 0.'.\
                        format(route, from_m, to_m, lane, pci_col, min_value)
                    self.insert_route_message(route, 'error', error_message)
                if (pci_val == max_value) and (not asp_rg_allzero) and (max_value is not None):
                    error_message = 'Rute {0} pada segmen {1}-{2} lane {3} memiliki nilai {4}={5} namun nilai kerusakan perkerasan aspal ataupun rigid yang tidak sepenuhnya bernilai 0.'.\
                        format(route, from_m, to_m, lane, pci_col, max_value)
                    self.insert_route_message(route, 'error', error_message)

                if check_null:
                    if (pci_val == max_value) and (not asp_rg_allzero and not asp_rg_allnull) and (
                            max_value is not None):
                        error_message = 'Rute {0} pada segmen {1}-{2} lane {3} memiliki nilai {4}={5} namun nilai kerusakan perkerasan aspal ataupun rigid yang tidak sepenuhnya bernilai 0.'. \
                            format(route, from_m, to_m, lane, pci_col, max_value)
                        self.insert_route_message(route, 'error', error_message)

                    if (pci_val is None) and (not asp_rg_allnull):
                        error_message = 'Rute {0} pada segmen {1}-{2} lane {3} tidak memiliki nilai {4} namun memiliki nilai kerusakan aspal atau rigid.'.\
                            format(route, from_m, to_m, lane, pci_col)
                        self.insert_route_message(route, 'error', error_message)
                    elif (pci_val is not None and (pci_val != max_value)) and asp_rg_allnull:
                        error_message = 'Rute {0} pada segmen {1}-{2} lane {3} memiliki nilai {4} yang bukan {5} atau Null namun tidak memiliki nilai kerusakan aspal atau rigid.'.\
                            format(route, from_m, to_m, lane, pci_col, max_value)
                        self.insert_route_message(route, 'error', error_message)

        return self

    def pci_surftype_check(self, routes='ALL', asp_pref='VOL_AS', rg_pref='VOL_RG', routeid_col='LINKID',
                           from_m_col='STA_FROM', to_m_col='STA_TO', lane_code='LANE_CODE', pci_col='PCI', **kwargs):
        """
        This class method check the consistency between segment's surface type and its AS_x and RG_x value.
        :param routes: Route selection
        :param asp_pref: AS_ column prefix
        :param rg_pref: RG_ column prefix
        :param routeid_col:  The routeid column
        :param from_m_col: The from measure column
        :param to_m_col: The to measure column
        :param lane_code: Lane code column
        :param pci_col: PCI column
        :return:
        """

        def col_allnull(row_series, mask):
            """
            This function will determine whether a row contain only Null value.
            :param row_series: The row series from a DataFrame.
            :param mask: Column masking.
            :return:
            """
            row = row_series[mask]
            result = np.all(row.isnull())  # Check if all column contain only Null value

            return result

        df = self.copy_valid_df()  # Create a valid DataFrame copy
        surf_col = '_surface'
        df_surf = self.surftype_df(surf_col)  # DataFrame containing surface group
        rni_surf_type = self.config.table_fields['rni']['surface_type']

        col_list = df.columns.tolist()
        asp_mask = np.char.startswith(col_list, asp_pref)
        rg_mask = np.char.startswith(col_list, rg_pref)

        if (not rg_mask.any()) or (not asp_mask.any()):  # Check if no column was found
            return self  # The specified prefix does not match any column.

        if routes == 'ALL':  # Check for route request
            pass
        else:
            df = self.selected_route_df(df, routes)

        route_list = self.route_lane_tuple(df, routeid_col, lane_code, route_only=True)
        for route in route_list:
            df_route = df.loc[df[routeid_col] == route]
            merge = add_rni_data(df_route, routeid_col, from_m_col, to_m_col, lane_code, self.sde_connection,
                                 added_column=rni_surf_type, mfactor=self.rni_mfactor)

            if merge is None:  # If the RNI DataFrame is empty.
                error_message = "Data RNI rute {0} tidak tersedia".format(route)
                self.insert_route_message(route, 'error', error_message)
                continue

            merge_surf = merge.join(df_surf, on=rni_surf_type, how='inner')  # Add surface type to merge result.

            for index, row in merge_surf.iterrows():
                from_m = row[from_m_col]
                to_m = row[to_m_col]
                lane = row[lane_code]

                asp_allnull = col_allnull(row, asp_mask)  # Check if all ASP_ value is null
                rg_allnull = col_allnull(row, rg_mask)  # Check if all RG_ value is null
                pci_null = pd.isnull(row[pci_col])  # Check whether PCI value is null
                surface = row[surf_col]  # The surface type

                if surface == 'asphalt' and (not rg_allnull):
                    error_message = 'Rute {0} pada segmen {1}-{2} lane {3} memiliki tipe perkerasan aspal namun memiliki nilai kerusakan rigid.'.\
                        format(route, from_m, to_m, lane)
                    self.insert_route_message(route, 'error', error_message)
                if surface == 'rigid' and (not asp_allnull):
                    error_message = 'Rute {0} pada segmen {1}-{2} lane {3} memiliki tipe perkerasan rigid namun memiliki nilai kerusakan aspal.'. \
                        format(route, from_m, to_m, lane)
                    self.insert_route_message(route, 'error', error_message)
                if surface == 'unpaved' and ((not asp_allnull) or (not rg_allnull)):
                    error_message = 'Rute {0} pada segmen {1}-{2} lane {3} memiliki tipe perkerasan unpaved namun memiliki nilai kerusakan rigid atau aspal.'. \
                        format(route, from_m, to_m, lane)
                    self.insert_route_message(route, 'error', error_message)

                # Compare the surface type and pci value
                if (surface in ['asphalt', 'rigid']) and pci_null:
                    error_message = 'Rute {0} pada segmen {1}-{2} lane {3} memiliki tipe perkerasan {4} namun tidak memiliki nilai PCI.'.\
                        format(route, from_m, to_m, lane, surface)
                    self.insert_route_message(route, 'error', error_message)
                if surface == 'unpaved' and (not pci_null):
                    error_message = 'Rute {0} pada segmen {1}-{2} lane {3} memiliki tipe perkerasan {4} namun memiliki nilai PCI.'.\
                        format(route, from_m, to_m, lane, surface)
                    self.insert_route_message(route, 'error', error_message)

        return self

    def fwd_dropid_check(self, dropid_col='DROP_ID', routeid_col='LINKID', from_m_col='STA_FROM', to_m_col='STA_TO',
                         survey_dir='SURVEY_DIREC', id_count=3, starts_at=1, routes='ALL', **kwargs):
        """
        This class method will check for drop ID repetition pattern. Single segment should has a drop id pattern starting
        from 0 and has an increment pattern of 1.
        :param dropid_col: The Drop ID column in the event DataFrame.
        :param routeid_col: The Route ID column in the event DataFrame.
        :param from_m_col: The From Measure column in the event DataFrame.
        :param to_m_col: The To Measure column in the event DataFrame.
        :param id_count: The Drop ID count for every segment.
        :param starts_at: The value that the Drop ID sequence starts.
        :param routes: The Routes selection.
        :return:
        """
        df = self.copy_valid_df()  # Create a copy of valid DataFrame.

        if routes == 'ALL':  # Check for route request
            pass
        else:
            df = self.selected_route_df(df, routes)

        route_list = self.route_lane_tuple(df, routeid_col, None, route_only=True)
        for route in route_list:
            df_route = df.loc[df[routeid_col] == route]  # The DataFrame with only selected route
            df_group = df_route.groupby(by=[routeid_col, from_m_col, to_m_col, survey_dir])

            for name, group in df_group:
                drop_ids = group[dropid_col]

                from_m = name[1]
                to_m = name[2]
                direction = name[3]

                seq_list = drop_ids.tolist()  # The sequence as list object
                seq_start = drop_ids.min()  # The start value of the drop id sequence
                seq_len = len(drop_ids)  # The len of the drop id sequence

                seq_diff = np.diff(seq_list)  # The difference between value in drop id sequence
                all_1_diff = np.all(seq_diff == 1)  # Check if all the difference is 1

                if seq_start != starts_at:
                    error_message = "Rute {0} pada segmen {1}-{2} di arah survey {3} memiliki pola drop id yang tidak diawali oleh nilai {4}".\
                        format(route, from_m, to_m, direction, starts_at)
                    self.insert_route_message(route, 'error', error_message)

                if seq_len != id_count:
                    error_message = "Rute {0} pada segmen {1}-{2} di arah survey {3} memiliki jumlah drop id yang tidak sama dengan {4}".\
                        format(route, from_m, to_m, direction, id_count)
                    self.insert_route_message(route, 'error', error_message)

                if not all_1_diff:
                    error_message = "Rute {0} pada segmen {1}-{2} di arah survey {3} memiliki pola yang tidak berurutan (memiliki interval lebih dari 1)".\
                        format(route, from_m, to_m, direction)
                    self.insert_route_message(route, 'error', error_message)

        return self

    def surf_thickness_check(self, routes='ALL', routeid_col='LINKID', from_m_col='STA_FROM', to_m_col='STA_TO',
                             survey_dir_col='SURVEY_DIREC', thickness_col='SURF_THICKNESS', **kwargs):
        """
        This class method compare the surface thickness value from the input table with the valid range which will be
        determined based on the segment's surface type in the RNI data.
        :param routes: The route selection.
        :param routeid_col: The route id column.
        :param from_m_col:  The from measure column.
        :param to_m_col: The to measure column.
        :param survey_dir_col: The survey direction column.
        :param thickness_col: The surface thickness column of the input table.
        :return:
        """
        rni_surface_type = self.config.table_fields['rni']['surface_type']

        df = self.selected_route_df(self.copy_valid_df(), routes)
        df = df.drop(df.loc[df[thickness_col].isnull()].index)  # Drop row with Null Thickness.
        surf_df = self.surftype_df('_surface')  # The surface type DataFrame.

        if df.empty:
            return self

        self.expand_segment(df, from_m_col=from_m_col, to_m_col=to_m_col)  # Expand the segment to 100meter segment.

        merged = add_rni_data(df, routeid_col, "_"+from_m_col, "_"+to_m_col, None, self.sde_connection,
                              rni_surface_type, agg_func={rni_surface_type: lambda x: x.value_counts().index[0]})

        grouped = merged.groupby([routeid_col, "_"+from_m_col, "_"+to_m_col, survey_dir_col]).\
            agg({rni_surface_type: lambda x: x.value_counts().index[0],
                 thickness_col: lambda x: x.value_counts().index[0]}).reset_index()

        grouped_surf = grouped.join(surf_df, on=rni_surface_type, how='inner')

        asphalt_error = ((grouped_surf['_surface'] == 'asphalt') &
                         ((grouped_surf[thickness_col] < 70) | (grouped_surf[thickness_col] > 350)))
        rigid_error = ((grouped_surf['_surface'] == 'rigid') &
                       ((grouped_surf[thickness_col] < 150) | (grouped_surf[thickness_col] > 320)))

        error_rows = grouped_surf.loc[asphalt_error | rigid_error]

        for index, row in error_rows.iterrows():
            route = row[routeid_col]
            from_m = row["_"+from_m_col]
            to_m = row["_"+to_m_col]
            direction = row[survey_dir_col]
            surface = row['_surface']
            surface_thickness = row[thickness_col]

            msg = 'Rute {0} pada segmen {1}-{2} di arah survey {3} memiliki nilai {4} yang berada diluar rentang untuk tipe perkerasan {5}, yaitu {6}.'.\
                format(route, from_m, to_m, direction, thickness_col, surface, surface_thickness)
            self.insert_route_message(route, 'ToBeReviewed', msg)

        return self

    def compare_kemantapan(self, grading_col, comp_fc, comp_from_col, comp_to_col, comp_route_col, comp_lane_code,
                           comp_grading_col, routes='ALL', routeid_col='LINKID', lane_codes='LANE_CODE',
                           from_m_col='STA_FROM', to_m_col='STA_TO', threshold=0.05, percentage=True,
                           rni_comp_kwargs=None, **kwargs):
        """
        This class method will compare the Kemantapan between the inputted data and previous year data, if the
        difference exceed the 5% absolute tolerance then the data will undergo further inspection.
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
        :param threshold: The threshold for Kemantapan changes.
        :return:
        """
        from SMD_Package.event_table.checks.error_runs import find_runs

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
                                     comp_route_col, self.sde_connection, include_all=True)

            if len(df_comp) == 0:  # Check if the specified route exist in the comparison table.
                error_message = "Data rute {0} pada tahun sebelumnya tidak tersedia, sehingga perbandingan kemantapan tidak dapat dilakukan.". \
                    format(route)
                self.error_list.append(error_message)
                self.insert_route_message(route, 'ToBeReviewed', error_message)

                return self

            # Create Kemantapan instance for both input data and comparison data.
            kemantapan = Kemantapan(df_route, grading_col, routeid_col, from_m_col, to_m_col, lane_codes,
                                    lane_based=True)

            if rni_comp_kwargs is None:
                kemantapan_compare = Kemantapan(df_comp, comp_grading_col, comp_route_col, comp_from_col,
                                                comp_to_col, comp_lane_code, lane_based=True, to_km_factor=1)
            else:
                kemantapan_compare = Kemantapan(df_comp, comp_grading_col, comp_route_col, comp_from_col,
                                                comp_to_col, comp_lane_code, lane_based=True, to_km_factor=1,
                                                **rni_comp_kwargs)

            if percentage:  # If the comparison is not lane based
                current = kemantapan.summary()['mantap_psn'].values[0]
                compare = kemantapan_compare.summary()['mantap_psn'].values[0]

                # Compare the kemantapan percentage between current data and previous data
                if np.isclose(compare, current, atol=(threshold*100)):
                    pass  # If true then pass
                else:
                    # Create the error message
                    error_message = "{0} memiliki perbedaan persen kemantapan yang melebihi batas ({1}%) dari data Roughness sebelumnya.".\
                        format(route, (100*threshold))
                    self.error_list.append(error_message)
                    self.insert_route_message(route, 'ToBeReviewed', error_message)

            current = kemantapan.graded_df
            compare = kemantapan_compare.graded_df

            # Merge the current input data and comparison table
            current_key = [routeid_col, from_m_col, to_m_col, lane_codes]
            compare_key = [comp_route_col, comp_from_col, comp_to_col, comp_lane_code]
            merge = pd.merge(current, compare, how='inner', left_on=current_key, right_on=compare_key)
            lanes = merge[lane_codes].unique().tolist()

            for lane in lanes:
                # Create the new column for difference in grade level
                merge_lane = merge.loc[merge[lane_codes] == lane]
                merge_lane.sort_values(from_m_col, inplace=True)  # Sort the lane rows
                diff_col = '_level_diff'
                grade_diff = 2
                merge_lane[diff_col] = merge_lane['_grade_level_x'].astype(int) - \
                                       merge_lane['_grade_level_y'].astype(int)
                error_rows = merge_lane.loc[merge_lane[diff_col] >= grade_diff]
                runs = find_runs(error_rows, 5)

                # Iterate over all error rows
                for index in runs:
                    sta_fr = merge_lane.at[index[0], from_m_col]
                    sta_to = merge_lane.at[index[1], to_m_col]

                    error_message = "{0} pada segmen {1}-{2} {3} memiliki perbedaan {4} tingkat kemantapan dengan data tahun sebelumnya.".\
                        format(route, sta_fr, sta_to, lane, grade_diff)
                    self.insert_route_message(route, 'ToBeReviewed', error_message)

        return self

    def side_consistency_check(self, columns, routes='ALL', routeid_col='LINKID', from_m_col='STA_FROM',
                               to_m_col='STA_TO', lane_code='LANE_CODE', empty_as_null=True, wipe=True, fill=True,
                               **kwargs):
        """
        This class method check for consistency in the specified check column for a single segment, the value of the
        check column for every lane in each "L" and "R" side should be the same.
        :param columns: Value in the column which the value will be checked.
        :param routes: Route selection.
        :param routeid_col: The Route ID column.
        :param from_m_col: The From Measure column.
        :param to_m_col: The To Measure column.
        :param lane_code: The lane code column.
        :param empty_as_null: The empty value is Null not 0.
        :param wipe: If True then fill column from other side with empty value (Null or zero, defined from empty_as_null
                     parameter)
        :param fill: If True then the function will fill all null row with the any value available in the other lane.
        :return:
        """
        def group_function(series):
            """
            This function is used to create new column for group by aggregate result.
            :param series: Series passed from group by object
            :return: Pandas Series.
            """
            d = dict()
            side = series['side'].values[0]
            dir_count = series['DIR_COUNT'].max()  # Just take the maximum value of a segment.

            if side == 'L':
                other_side = 'R'
            else:
                other_side = 'L'

            # In the production version, the side should be a prefix not suffix.
            check_side_col = side + "_" + column
            other_side_col = other_side + "_" + column

            if dir_count > 1:
                d['single_dir'] = False

                if empty_as_null:
                    d['all_empty'] = np.all(series[check_side_col].isnull())  # If the value is all Null
                    empty_value = np.nan
                else:
                    d['all_empty'] = np.all(series[check_side_col] == 0)
                    empty_value = 0

                # Check if there is more than 1 value
                # Null is not counted in
                value_count = series[check_side_col].nunique(True)
                d['check_value_count'] = value_count
                d['inconsistent'] = value_count > 1
                d['column'] = check_side_col

                if wipe:
                    self.df_valid.loc[series.index, [other_side_col]] = empty_value
                if fill and (value_count == 1):
                    self.df_valid.loc[series.index, [check_side_col]] = series[check_side_col].values[0]

            else:
                d['single_dir'] = True
                d['column'] = check_side_col

                if empty_as_null:
                    check_side_null = np.all(series[check_side_col].isnull())  # If the value in the check col is Null
                    other_side_null = np.all(series[other_side_col].isnull())  # If the value in the other side col is Null
                else:
                    check_side_null = np.all(series[check_side_col] == 0)  # If the value in the check col is Null
                    other_side_null = np.all(series[other_side_col] == 0)  # If the value in the other side col is Null

                check_side_inconsistent = series[check_side_col].nunique(True) > 1
                other_side_inconsistent = series[other_side_col].nunique(True) > 1

                if check_side_null:
                    d['all_empty'] = check_side_col
                if other_side_null:
                    d['all_empty'] = other_side_col
                if check_side_null and other_side_null:
                    d['all_empty'] = [check_side_col, other_side_col]

                if check_side_inconsistent:
                    d['incosistent'] = check_side_col
                if other_side_inconsistent:
                    d['inconsistent'] = other_side_col
                if check_side_inconsistent and other_side_inconsistent:
                    d['inconsistent'] = [check_side_col, other_side_col]

                if fill and not(check_side_inconsistent and other_side_inconsistent):
                    self.df_valid.loc[series.index, [check_side_col]] = series[check_side_col].values[0]
                    self.df_valid.loc[series.index, [other_side_col]] = series[other_side_col].values[0]

            return pd.Series(d, index=['all_empty', 'check_value_count', 'inconsistent', 'column', 'single_dir'])

        df = self.selected_route_df(self.copy_valid_df(), routes)

        if df.empty:
            return self

        side_column = 'side'
        df[side_column] = df[[lane_code]].apply(lambda x: x[0][0], axis=1)  # Adding the side column L or R

        segment_group = df.groupby([routeid_col, from_m_col, to_m_col])[side_column]. \
            agg({"DIR_COUNT": lambda x: x.nunique()})
        dir_count_df = segment_group.reset_index()

        df = df.merge(dir_count_df, on=[routeid_col, from_m_col, to_m_col], right_index=True)  # Add direction count for every segment.

        if type(columns) != list:  # Force columns variable as list type.
            columns = [columns]

        for column in columns:
            side_group = df.groupby([routeid_col, from_m_col, to_m_col, side_column]).apply(group_function)
            side_group.reset_index(inplace=True)  # Reset the group by index

            # Start check for any error
            all_empty = ~side_group['all_empty'].isnull()
            inconsistent = ~side_group['inconsistent'].isnull()
            error_rows = side_group.loc[all_empty | inconsistent]

            for index, row in error_rows.iterrows():
                route = row[routeid_col]
                from_m = row[from_m_col]
                to_m = row[to_m_col]
                side = row[side_column]
                empty = row['all_empty']
                val_count_error = row['inconsistent']
                error_col = row['column']
                single_dir = row['single_dir']

                if not single_dir:
                    if empty:
                        msg = "Rute {0} pada segmen {1}-{2} di sisi {3} tidak memiliki nilai {4}.".\
                            format(route, from_m, to_m, side, error_col)
                        self.insert_route_message(route, 'error', msg)

                    if val_count_error:
                        msg = "Rute {0} pada segmen {1}-{2} di sisi {3} memiliki nilai {4} yang tidak konsisten di setiap jalur.".\
                            format(route, from_m, to_m, side, error_col)
                        self.insert_route_message(route, 'error', msg)
                else:
                    if empty is not np.nan:
                        if type(empty) == list:
                            msg = "Rute {0} pada segmen {1}-{2} di sisi L dan R tidak memiliki nilai {3}.".\
                                format(route, from_m, to_m, empty)
                            self.insert_route_message(route, 'error', msg)
                        else:
                            side = str(empty).split('_')[0]
                            msg = "Rute {0} pada segmen {1}-{2} di sisi {3} tidak memiliki nilai {4}".\
                                format(route, from_m, to_m, side, empty)
                            self.insert_route_message(route, 'error', msg)

                    if val_count_error is not np.nan:
                        if type(val_count_error) == list:
                            msg = "Rute {0} pada segmen {1}-{2} di sisi L dan R memiliki nilai {3} yang tidak konsisten ditiap sisi.".\
                                format(route, from_m, to_m, empty)
                            self.insert_route_message(route, 'error', msg)
                        else:
                            side = str(val_count_error).split('_')[0]
                            msg = "Rute {0} pada segmen {1}-{2} di sisi {3} memiliki nilai {4} yang tidak konsisten di setiap jalur.".\
                                format(route, from_m, to_m, side, val_count_error)
                            self.insert_route_message(route, 'error', msg)

        return self

    def side_pattern_check(self, columns, routes='ALL', routeid_col='LINKID', from_m_col='STA_FROM', to_m_col='STA_TO',
                           lane_code='LANE_CODE', in_type_cols='TYPE', type_as_suffix=True, empty_value_type=0,
                           **kwargs):
        """
        This function check for filling pattern in the requested columns for every available side (L or R).
        :param columns: Requested columns.
        :param routes: Requested routes.
        :param routeid_col: Route ID column.
        :param from_m_col: From Measure column.
        :param to_m_col: To Measure column.
        :param lane_code: Lane code column.
        :param in_type_cols: Type columns specifier.
        :param type_as_suffix: If True then the string in the in_type_cols will be used as suffix for type columns.
        :param empty_value_type: The type which means other value should be Null/empty.
        :return:
        """
        def group_function(g_df):
            d = dict()
            side = g_df['side'].values[0]
            dir_count = g_df['DIR_COUNT'].max()
            single_dir = dir_count == 1

            if dir_count == 1:
                if side == 'L':
                    other_side = 'R'
                else:
                    other_side = 'L'
                check_col_side = [side + "_" + column for column in columns]
                check_col_side = check_col_side + [other_side + "_" + column for column in columns]
            else:
                # In the production version, the side should be a prefix not suffix.
                check_col_side = [side + "_" + column for column in columns]

            # Find the type columns
            if type_as_suffix:  # "TYPE" as column suffix
                type_cols = np.array(check_col_side)[np.char.endswith(check_col_side, in_type_cols)].tolist()
            else:  # "TYPE" as column prefix
                type_cols = np.array(check_col_side)[np.char.startswith(check_col_side, in_type_cols)].tolist()

            # Type vs value consistency check.
            type_value = dict()  # For storing  each type column result.
            type_value_results = list()  # For storing row's type value check result.
            for type_col in type_cols:
                if type_as_suffix:
                    type_domain = type_col.split(in_type_cols)[0]
                    value_col = np.array(check_col_side)[np.char.startswith(check_col_side, type_domain)].\
                        tolist()
                else:
                    type_domain = type_col.split(in_type_cols)[1]
                    value_col = np.array(check_col_side)[np.char.endswith(check_col_side, type_domain)].\
                        tolist()

                value_col.remove(type_col)
                type_value_error = g_df.loc[((g_df[type_col] == empty_value_type) &
                                            (np.any(g_df[value_col] != 0, axis=1)) &
                                            (~g_df[type_col].isnull())) |

                                            ((g_df[type_col] != empty_value_type) &
                                            (np.any(g_df[value_col] == 0, axis=1)) &
                                            (~g_df[type_col].isnull()))]

                type_value[type_col] = type_value_error.empty  # If there is no error then True
                type_value_results.append(type_value_error.empty)  # Append result without type column name

            # The filling pattern check
            if len(g_df) > 1:
                null_mask = g_df[check_col_side].apply(lambda x: x.isnull(), axis=1)
                all_result = [null_mask[check_col_side[0]].equals(null_mask[col]) for col in check_col_side]
                result = np.all(all_result)  # If all is True.
            else:
                result = True

            d['correct_pattern'] = result
            d['columns'] = check_col_side
            d['type_value'] = type_value
            d['type_value_results'] = np.all(type_value_results)  # True, if all is correct.
            d['single_dir'] = single_dir

            return pd.Series(d, index=['correct_pattern', 'columns',
                                       'type_value', 'type_value_results', 'single_dir'])  # Returned row

        df = self.selected_route_df(self.copy_valid_df(), routes)

        if df.empty:
            return self

        side_column = 'side'
        df[side_column] = df[[lane_code]].apply(lambda x: x[0][0], axis=1)  # Adding the side column L or R

        if type(columns) != list:  # The input columns should be list, otherwise function will not be proceeded.
            return self

        segment_group = df.groupby([routeid_col, from_m_col, to_m_col])[side_column]. \
            agg({"DIR_COUNT": lambda x: x.nunique()})
        dir_count_df = segment_group.reset_index()

        df = df.merge(dir_count_df, on=[routeid_col, from_m_col, to_m_col])  # Add direction count for every segment.
        side_group = df.groupby([routeid_col, from_m_col, to_m_col, side_column]).apply(group_function)
        side_group.reset_index(inplace=True)
        error_rows = side_group.loc[(~side_group['correct_pattern']) | (~side_group['type_value_results'])]

        for index, row in error_rows.iterrows():
            route = row[routeid_col]
            from_m = row[from_m_col]
            to_m = row[to_m_col]
            side = row[side_column]
            columns = row['columns']
            correct_pattern = row['correct_pattern']
            correct_type_value = row['type_value_results']
            type_value = row['type_value']
            single_dir = row['single_dir']

            if not single_dir:
                if not correct_pattern:
                    msg = "Rute {0} pada segmen {1}-{2} di sisi {3} memiliki pola pengisian kolom {4} yang tidak konsisten.".\
                        format(route, from_m, to_m, side, columns)
                    self.insert_route_message(route, 'error', msg)

                if not correct_type_value:
                    msg = "Rute {0} pada segmen {1}-{2} di sisi {3} memiliki nilai tipe yang tidak konsisten yaitu {4}.".\
                        format(route, from_m, to_m, side, type_value)
                    self.insert_route_message(route, 'error', msg)
            else:
                pass

        return self

    def median_direction_check(self, routes='ALL', routeid_col='LINKID', from_m_col='STA_FROM', to_m_col='STA_TO',
                               direction_col='SURVEY_DIREC', **kwargs):
        """
        This class method merge the input table with RNI data to get the median width and compare the median data with
        the amount of available survey direction. A segment with median should have both of direction, otherwise an
        error message will be raised.
        :param routes: Route selection.
        :param routeid_col: Route ID column.
        :param from_m_col: From measure column.
        :param to_m_col: To measure column.
        :param direction_col: The survey direction column.
        :return:
        """
        smd_config = SMDConfigs()
        rni_medwidth = smd_config.table_fields['rni']['median']

        df = self.selected_route_df(self.copy_valid_df(), routes)

        if df.empty:  # If the query result returns empty DataFrame.
            return self

        self.expand_segment(df, from_m_col=from_m_col, to_m_col=to_m_col, segment_len_col=kwargs.get('length_col'))

        merged = add_rni_data(df, routeid_col, "_"+from_m_col, "_"+to_m_col, None, self.sde_connection, rni_medwidth,
                              agg_func={rni_medwidth: lambda x: x.max()})
        grouped = merged.groupby([routeid_col, "_"+from_m_col, "_"+to_m_col]).agg({direction_col: lambda x: x.nunique(),
                                                                              rni_medwidth: lambda x: x.max()}).reset_index()
        error_rows = grouped.loc[(grouped[rni_medwidth] > 0) & (grouped[direction_col] < 2)]

        for index, row in error_rows.iterrows():
            from_m = row["_"+from_m_col]
            to_m = row["_"+to_m_col]
            route = row[routeid_col]

            msg = "Rute {0} pada segmen {1}-{2} memiliki median namum hanya memiliki data pada di satu arah saja.".\
                format(route, from_m, to_m)
            self.insert_route_message(route, 'error', msg)

    def deflection_null_row_check(self, deflection_cols=None, routes='ALL', routeid_col='LINKID', from_m_col='FROM_STA',
                                  to_m_col='TO_STA', allow_rigid=True, roughness_table='SMD.ROUGHNESS_1_2020',
                                  second_roughness_table='SMD.ROUGHNESS_2019_2_RERUN_2', **kwargs):
        """
        Checks for deflection value for paved segment, unpaved segment should not have any deflection value (all Null).
        :param deflection_cols: The deflection columns to be checked.
        :param routes: Route selection.
        :param routeid_col: Route ID column.
        :param from_m_col: From Measure column.
        :param to_m_col: To Measure column.
        :param allow_rigid: Allow deflection data to be filled in the rigid segment.
        :param roughness_table: The roughness table used to get the IRI data.
        :param second_roughness_table: The second roughness table used if the data is not available in the first table.
        :return:
        """
        import copy

        df = self.selected_route_df(self.copy_valid_df(), routes)

        if df.empty:
            return self

        input_routes = df[routeid_col].unique().tolist()
        rni_surf_type = self.config.table_fields['rni']['surface_type']
        rni_hor_align = self.config.table_fields['rni']['hor_align']
        rni_ver_align = self.config.table_fields['rni']['ver_align']
        rni_added_col = [rni_surf_type, rni_hor_align, rni_ver_align]

        roughness_config = copy.deepcopy(self.config.table_fields['rni'])
        roughness_config['table_name'] = roughness_table

        surface_group = RNISummary.surface_group_df().reset_index()
        self.expand_segment(df, from_m_col=from_m_col, to_m_col=to_m_col)

        new_from_col = '_'+from_m_col  # The new from and to measurement column from the expand_segment function.
        new_to_col = '_'+to_m_col

        if deflection_cols is None:
            return self

        if type(deflection_cols) != list:
            deflection_cols = list(deflection_cols)

        # Get the first available surface type from each RNI segment.
        merged = add_rni_data(df, routeid_col, new_from_col, new_to_col, None, self.sde_connection, rni_added_col,
                              agg_func={rni_surf_type: lambda x: x.values[0],
                                        rni_hor_align: lambda x: x.values[0],
                                        rni_ver_align: lambda x: x.values[0]})

        # Add the IRI data from the Roughness table.
        merged_iri = add_rni_data(merged, routeid_col, new_from_col, new_to_col, None, self.sde_connection, 'IRI',
                                  agg_func={'IRI': lambda x: x.max()}, mfactor=100, rni_kwargs=roughness_config)
        if merged_iri is None:
            roughness_config['table_name'] = second_roughness_table  # Replace the IRI table
            roughness_config['from_measure'] = 'STA_FROM'
            roughness_config['to_measure'] = 'STA_TO'
            merged_iri = add_rni_data(merged, routeid_col, new_from_col, new_to_col, None, self.sde_connection, 'IRI',
                                      agg_func={'IRI': lambda x: x.max()}, mfactor=100, rni_kwargs=roughness_config)

        if merged_iri is not None:
            merged = merged_iri
        else:
            for route in input_routes:
                error_msg = 'Rute {route} tidak memiliki data IRI pada tabel {table1} dan {table2}'.format(
                    route=input_routes, table1=roughness_table, table2=second_roughness_table)
                self.insert_route_message(route, 'error', error_msg)

        # Merge with surface group details.
        merged_surf = merged.merge(surface_group, left_on=rni_surf_type, right_on='group')

        all_null = np.all(merged_surf[deflection_cols].isnull(), axis=1)
        any_null = np.any(merged_surf[deflection_cols].isnull(), axis=1)

        # The conditions.
        not_aspal = merged_surf['index'] != 'aspal'
        hor_align_allow = merged_surf[rni_hor_align].isin([2, 3])
        ver_align_allow = merged_surf[rni_ver_align].isin([2, 3])
        iri_allow = merged_surf['IRI'] >= 12

        if allow_rigid:
            error_rows = merged_surf.loc[(~not_aspal & any_null) &
                                         (~hor_align_allow & any_null) &
                                         (~ver_align_allow & any_null) &
                                         (~iri_allow & any_null)]  # Only check for error in the asphalt segment.
        else:
            error_rows = merged_surf.loc[(not_aspal & ~all_null) | (~not_aspal & any_null)]  # Check in all surface.

        grouped_error = error_rows.groupby([routeid_col, from_m_col+'_x', to_m_col+'_x'])['index'].max().reset_index()

        for index, row in grouped_error.iterrows():
            route = row[routeid_col]
            from_m = row[from_m_col+'_x']
            to_m = row[to_m_col+'_x']
            category = str(row['index'])

            if category != 'aspal':
                error_msg = "Rute {0} pada segmen {1}-{2} merupakan segmen rigid/tanah namun memiliki nilai dari salah satu atau semua kolom berikut {3}.".\
                    format(route, from_m, to_m, deflection_cols)
                self.insert_route_message(route, 'error', error_msg)
            else:
                error_msg = "Rute {0} pada segmen {1}-{2} merupakan segmen aspal namun tidak memiliki nilai dari salah satu atau semua kolom berikut {3}.".\
                    format(route, from_m, to_m, deflection_cols)
                self.insert_route_message(route, 'error', error_msg)

        return self

    def col_unique_check(self, column, routes='ALL', routeid_col='LINKID', from_m_col='STA_FROM', to_m_col='STA_TO'
                         , **kwargs):
        """
        Checks for column N unique value, if the value exceed one then an error message will be raised.
        :param column: Column to be checked.
        :param routes: Route selection.
        :param routeid_col: Route ID column.
        :param from_m_col: From Measure column.
        :param to_m_col: To Measure column.
        :return:
        """
        df = self.selected_route_df(self.copy_valid_df(), routes)

        if df.empty:
            return self

        grouped = df.groupby([routeid_col, from_m_col, to_m_col])
        nunique = grouped.aggregate({column: 'nunique'}).reset_index()
        error_row = nunique.loc[(nunique[column] > 1) | (nunique[column] == 0)]

        for index, row in error_row.iterrows():
            route = row[routeid_col]
            from_m = row[from_m_col]
            to_m = row[to_m_col]
            val_count = row[column]

            if val_count > 1:  # Unique value count > 1
                msg = "Rute {0} pada segmen {1}-{2} memiliki nilai unik yang lebih dari satu pada kolom {3}".\
                    format(route, from_m, to_m, column)
            else:  # No unique value detected, all Null.
                msg = "Rute {0} pada segmen {1}-{2} tidak memiliki nilai {3}".\
                    format(route, from_m, to_m, column)
            self.insert_route_message(route, 'error', msg)

        return self

    def copy_valid_df(self, dropna=False, ignore=False):
        """
        This function create a valid DataFrame from the dtype check class method, which ensures every column match the
        required DataType
        :return:
        """
        # If there is a problem with the data type check then return the df_string
        if self.df_valid is None:
            return None
        elif self.dtype_check_result is None or ignore:
            df = self.df_valid
            return df.copy(deep=True)
        elif dropna:
            df = self.df_valid.dropna()
            return df.copy(deep=True)
        elif not dropna:
            df = self.df_string
            return df.copy(deep=True)

    @staticmethod
    def surftype_df(surface_column):
        """
        This function create a surface group DataFrame for PCI usage.
        :param surface_column: The surface column in the output DataFrame.
        :return:
        """
        df_surf = pd.DataFrame(range(1, 22), columns=['code'])
        df_surf[surface_column] = pd.Series(None)

        # Define the surface group
        surf_dict = {
            "unpaved": range(1, 3),
            "asphalt": range(3, 21),
            "rigid": range(21, 22)
        }

        for surf in surf_dict:
            group = surf_dict[surf]
            df_surf.loc[df_surf['code'].isin(group), [surface_column]] = surf

        return df_surf.set_index('code')

    @staticmethod
    def expand_segment(input_df, segment_len=0.1, from_m_col='FROM_STA', to_m_col='TO_STA',
                       segment_len_col='SEGMENT_LENGTH', len_to_m=100, max_length_allowed=0.5):
        """
        This static method expand the input DataFrame segment with segment length greater than the defined segment
        length in the parameter.
        :param input_df: The input DataFrame.
        :param segment_len: Determined segment length.
        :param from_m_col: From Measure column.
        :param to_m_col: To Measure column.
        :param segment_len_col: Segment length from the input table.
        :param len_to_m: The conversion factor to convert the segment length to from-to measurement value.
        :return:
        """
        n_from_m_col = '_' + from_m_col  # The new from and to measure column
        n_to_m_col = '_' + to_m_col

        # Create the new empty column
        input_df[[n_from_m_col, n_to_m_col]] = pd.DataFrame(index=input_df.index, columns=[n_from_m_col, n_to_m_col])

        for index, row in input_df.iterrows():
            input_seg_len = row[segment_len_col]
            input_from = row[from_m_col]
            new_row_count = int(float(input_seg_len)/float(segment_len))-1  # Count of to be inserted rows.

            # Fill the value for the existing segment row
            if input_seg_len > segment_len:
                if input_seg_len > max_length_allowed:
                    continue
                first_to_m = input_from + int(float(segment_len)*len_to_m)  # The first corrected to measure value.
            else:
                first_to_m = input_from + int(float(input_seg_len)*len_to_m)

            row[n_from_m_col] = input_from
            row[n_to_m_col] = first_to_m
            input_df.loc[index] = row

            for n_row in range(new_row_count):  # Insert the new row
                if n_row == 0:
                    n_from = first_to_m
                    n_to = first_to_m + int(float(segment_len) * len_to_m)
                else:
                    n_from = n_to
                    n_to = n_to + int(float(segment_len) * len_to_m)

                df_max_i = input_df.index.max()
                row[n_from_m_col] = n_from
                row[n_to_m_col] = n_to
                input_df.loc[df_max_i+1] = row

        input_df.sort_values([from_m_col, to_m_col], inplace=True)
        return

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

        if routes == 'ALL':
            return df

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
    def route_geometry(route, lrs_network, lrs_routeid, from_date_col='FROMDATE', to_date_col='TODATE'):
        """
        This static method return a Polyline object geometry from the LRS Network.
        :param route: The requested route.
        :param lrs_network: LRS Network feature class.
        :param lrs_routeid: The LRS Network feature class RouteID column.
        :return: Arcpy Polyline geometry object if the requested route exist in the LRS Network, if the requested route
        does not exist in the LRS Network then the function will return None.
        """
        where_statement = "{0}='{1}'".format(lrs_routeid, route)  # The where statement for SearchCursor
        date_query = "({0} is null or {0}<=CURRENT_TIMESTAMP) and ({1} is null or {1}>CURRENT_TIMESTAMP)".\
            format(from_date_col, to_date_col)
        route_exist = False  # Variable for determining if the requested route exist in LRS Network

        with da.SearchCursor(lrs_network, "SHAPE@", where_clause=where_statement+" and "+date_query) as cursor:
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
                "ToBeReviewed": [],
                "VerifiedWithWarning": []
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
        route_with_msg = self.route_results.keys()

        select_failed = ~self.df_valid[routeid_col].isin(route_with_msg)
        is_valid_route = self.df_valid[routeid_col].isin(self.valid_route)
        passed_routes_row = self.df_valid.loc[select_failed & is_valid_route, routeid_col].unique().tolist()

        for route in route_with_msg:
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

    @property
    def failed_routes(self):
        """
        This property contain a list of all routes that did not passed the verification (with error message not
        ToBeReviewed message). The route is extracted from the route_result class attribute.
        :return:
        """
        df = pd.DataFrame(self.altered_route_result(include_valid_routes=False))
        if len(df) != 0:
            routes = df.loc[df['status'] == 'error']['linkid'].unique().tolist()
            return routes
        else:
            return list()

    @property
    def passed_routes(self):
        """
        This property contain a list of all routes that passed all the verification and does not require any review.
        The route is extracted from the route_result class attribute.
        :return:
        """
        df = pd.DataFrame(self.altered_route_result(include_valid_routes=True, message_type='VerifiedWithWarning'))

        if len(df) != 0:
            routes = df['linkid'].unique().tolist()
            routes_intersect = np.intersect1d(routes, self.valid_route).tolist()
            return routes_intersect
        else:
            return list()

    @property
    def no_error_route(self):
        """
        This property contain a list of all routes without any error message (verfied and ToBeReviewed)
        :return:
        """
        error_routes = self.failed_routes
        if len(error_routes) != 0:
            intersect = np.setdiff1d(self.valid_route, error_routes).tolist()
            return intersect
        elif len(error_routes) == 0:
            return self.valid_route
        else:
            return list()
