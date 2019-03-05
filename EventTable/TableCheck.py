import pandas as pd


class EventTableCheck(object):
    """
    This class will be used for event table review, consist of table columns review and row by row review.
    """
    def __init__(self, event_table_path):
        """
        Initialize EventTable class
        :param event_table_path:
        """
        self.file_format = str(event_table_path).split('.')[1]  # Get the table file format
        self.ev_table_path = str(event_table_path)

    def header_checker(self, columns_details):
        """
        This function check for the input table header name and any redundant column in the input table
        :param columns_details:
        :return:
        """

        error_message = ''

        # Check the file format
        if self.file_format in ['xls', 'xlsx']:
            df_table = pd.read_excel(self.ev_table_path)  # Get the table header names
            df_table.columns = df_table.columns.str.upper()
            table_header = list(df_table)
            missing_column = []

            # Check if the required header is not in the input header
            for req_header in columns_details:
                if req_header not in table_header:
                    missing_column.append(str(req_header))
            if len(missing_column) != 0:
                error_message += 'Table input tidak memiliki kolom {0}.'.format(missing_column)

            # Check if the amount of header is the same as the requirement
            if len(table_header) != len(columns_details.keys()):
                error_message += 'Table input memiliki jumlah kolom yang berlebih.'

        else:
            error_message += 'Tabel input tidak berformat .xls atau .xlsx.'

        if error_message == '':
            return None
        else:
            return reject_message(error_message)


def reject_message(err_message):
    message = {
        'status': 'rejected',
        'msg': err_message
    }

    return message
