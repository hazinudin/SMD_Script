import pandas as pd
import json
from arcpy import AddMessage


class EventTableCheck(object):
    """
    This class will be used for event table review, consist of table columns review and row by row review.
    """
    def __init__(self, event_table_path, column_details):
        """
        Initialize EventTable class
        :param event_table_path:
        """
        self.file_format = str(event_table_path).split('.')[1]  # Get the table file format
        if self.file_format in ['xls', 'xlsx']:

            df_self_dtype = pd.read_excel(event_table_path)
            s_converter = {col: str for col in list(df_self_dtype)}
            del df_self_dtype

            df_string = pd.read_excel(event_table_path, converters=s_converter)
            df_string.columns = df_string.columns.str.upper()
            self.df_string = df_string
        else:
            self.df_string = None

        self.ev_table_path = str(event_table_path)
        self.column_details = column_details

        self.header_check_result = self.header_check()  # The header checking result
        self.dtype_check_result = self.dtype_check()  # The data type checing result

    def header_check(self):
        """
        This function check for the input table header name and any redundant column in the input table.
        :param columns_details:
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
        if self.header_check_result is None:  # If there is no problem with the header then continue

            # Start checking the data type for every numeric columns
            df = self.df_string

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
                        error_list.append('{0} memiliki nilai non-numeric pada baris{1}.'\
                            .format(col_name, str(excel_i)))

                elif col_dtype == 'date':  # Check for date column

                    # Convert the column to a date data type
                    # If the column contain an invalid date format, then change that value to Null
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
                    error_i = df.loc[df[col_name].isnull()].index.tolist()  # Find the index of the null

                    # If there is an error
                    if len(error_i) != 0:
                        excel_i = [x + 2 for x in error_i]
                        error_list.append('{0} memiliki tanggal yang tidak sesuai dengan format pada baris{1}.' \
                                          .format(col_name, str(excel_i)))

            return reject_message(error_list)

        else:
            return self.header_check_result


def reject_message(err_message):
    message = {
        'status': 'rejected',
        'msg': err_message
    }

    return json.dumps(message)
