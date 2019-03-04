import pandas as pd


class RowChecker(object):
    """
    This class will be used for event table review, consist of table columns review and row by row review.
    """
    def __init__(self, event_table_data, columns_details):
        """
        Initialize EventTableCheck class
        :param event_table_data:
        """
        file_format = str(event_table_data).split('.')[1]  # Get the table file format


def header_checker(event_table_data, columns_details):
    """
    Check for input table header
    """
    file_format = str(event_table_data).split('.')[1]  # Get the table file format

    error_message = ''

    # Check the file format
    if file_format in ['xls', 'xlsx']:
        df_table = pd.read_excel(event_table_data)  # Get the table header names
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
