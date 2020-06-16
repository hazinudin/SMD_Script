from arcpy import da, env, CreateTable_management, AddField_management, Exists, ListFields
import datetime

env.overwriteOutput = True


def gdb_table_writer(workspace, dataframe, table_name, cols_dtype, new_table=False, input_routeid='LINKID',
                     target_routeid='LINKID', write_date=True, replace_key=None):
    """
    This function writes input DataFrame as geodatabase event table
    :param workspace: The workspace for target database table
    :param dataframe: The input DataFrame
    :param new_table: If true then a new table will be created
    :param table_name: The target table name
    :param cols_dtype: The colums of target table name, if new 'new_table' is True then the column will be used to
    create new column in the newly created table.
    :param input_routeid: The RouteID column in the input table.
    :param target_routeid: The RouteID column in the target table.
    :param write_date: If True then a date column will be created and filled with current time. Default is True.
    :return:
    """
    env.workspace = workspace  # Environment workspace
    env.overwriteOutput = True
    table_exist = Exists(table_name)  # Check if the table already exist in the workspace.
    date_column = 'UPDATE_DATE'

    if new_table or (not table_exist):  # If the table does not exist or new_table is True, then create new table.
        CreateTable_management(workspace, table_name)
        for col in cols_dtype.keys():
            AddField_management(table_name, col, cols_dtype[col]['dtype'])

    existing_col = [str(f.name) for f in ListFields(table_name)]  # List all available table column

    if write_date and (date_column not in existing_col):  # If the date column does not exist
        AddField_management(table_name, date_column, "DATE")

    input_routes = dataframe[input_routeid].unique().tolist()  # List of every route in the input DataFrame

    for route in input_routes:  # Iterate for every available route in the input
        df_route = dataframe.loc[dataframe[input_routeid] == route]  # The route DataFrame

        if replace_key is None:
            replace_clause = "{0}='{1}'".format(target_routeid, route)
        else:
            if type(replace_key) != list:
                raise TypeError
            else:
                replace_clause = ''
                for key in replace_key:
                    value = df_route[key].tolist()[0]
                    key_type = cols_dtype[key]['dtype']

                    if key_type != 'string':
                        statement = "({0} = {1})".format(key, value)
                    else:
                        statement = "({0} = '{1}')".format(key, str(value))

                    if replace_clause == '':
                        replace_clause = statement
                    else:
                        replace_clause = replace_clause + ' AND ' + statement

        if write_date:
            table_column = cols_dtype.keys()+[date_column]
        else:
            table_column = cols_dtype.keys()

        with da.UpdateCursor(table_name, target_routeid, where_clause=replace_clause)\
                as del_cursor:
            for _ in del_cursor:
                del_cursor.deleteRow()  # If the route already exist in the table then delete the whole route row

        with da.InsertCursor(table_name, table_column) as insert_cursor:  # Create an insert cursor
            for index, row in df_route.iterrows():  # Iterate over available rows
                row_object = []

                for col_name in cols_dtype.keys():  # Iterate only column which has been specified in config
                    row_object.append(row[col_name])

                row_object.append(datetime.datetime.now())  # Add date value which at the last index
                insert_cursor.insertRow(row_object)  # Insert the new row
