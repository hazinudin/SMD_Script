import pandas as pd
from zipfile import BadZipfile
from arcpy import SetParameterAsText
from SMD_Package.OutputMessage import output_message
import sys


def read_input_excel(event_table_path, parameter_index=2):
    """
    This function will read the submitted excel file by the SMD user, the file format has to be '.xls' or '.xlsx', if
    any other file format is submitted then this function will return None.
    :param event_table_path: The excel file path.
    :param parameter_index: Parameter index for arcpy output message.
    :return: Pandas DataFrame or NoneType.
    """
    file_format = str(event_table_path)[-4:]
    if file_format in ['xls', 'xlsx']:

        try:
            df_self_dtype = pd.read_excel(event_table_path)
            s_converter = {col: str for col in list(df_self_dtype)}  # Create a string converters for read_excel
            del df_self_dtype
        except IOError:  # Handle error if the file path is invalid
            SetParameterAsText(parameter_index, output_message("Failed", "File tidak ditemukan."))
            sys.exit(0)
        except BadZipfile:  # Handle corrupt file.
            SetParameterAsText(parameter_index, output_message("Failed", "File tidak dapat dibaca."))
            sys.exit(0)

        try:
            df_string = pd.read_excel(event_table_path, converters=s_converter, keep_default_na=False)  # Convert all column to 'str' type.
        except UnicodeEncodeError:  # Handle if there is a non ascii character.
            SetParameterAsText(parameter_index, output_message("Failed", "Terdapat karakter yang tidak bisa diconvert."))
            sys.exit(0)

        df_string.columns = df_string.columns.str.upper()  # Uppercase all the column name
        return df_string  # df_string is DataFrame which contain all data in string format
    else:
        SetParameterAsText(2, output_message("Failed", "Jenis file harus dalam .xlsx atau .xls"))
        sys.exit(0)
