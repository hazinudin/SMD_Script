import pandas as pd


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
        self.header_check_result = None

    def header_check(self):
        """
        This function check for the input table header name and any redundant column in the input table
        :param columns_details:
        :return:
        """

        error_message = ''

        # Check the file format
        if self.df_string is not None:
            df = self.df_string  # Get the string data frame

            table_header = list(df)
            missing_column = []

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
            pass
        else:
            self.header_check_result = reject_message(error_message)

        return self

    def dtype_check(self):
        """
        This function check the input table column data type
        :return:
        """
        # Run the header check method
        self.header_check()
        if self.header_check_result is None: # If there is no problem with the header then continue
            pass


def reject_message(err_message):
    message = {
        'status': 'rejected',
        'msg': err_message
    }

    return message
