from arcpy import da, env, CreateTable_management, AddField_management, Exists

env.overwriteOutput = True


def gdb_table_writer(workspace, dataframe, table_name, cols_dtype, new_table=False, input_routeid='LINKID',
                     target_routeid='LINKID'):
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
    :return:
    """
    env.workspace = workspace  # Environment workspace
    env.overwriteOutput = True
    table_exist = Exists(table_name)  # Check if the table already exist in the workspace.

    if new_table or (not table_exist):  # If the table does not exist or new_table is True, then create new table.
        CreateTable_management(workspace, table_name)
        for col in cols_dtype.keys():
            AddField_management(table_name, col, cols_dtype[col]['dtype'])

    input_routes = dataframe[input_routeid].unique().tolist()  # List of every route in the input DataFrame

    for route in input_routes:  # Iterate for every available route in the input
        df_route = dataframe.loc[dataframe[input_routeid] == route]  # The route DataFrame

        with da.UpdateCursor(table_name, target_routeid, where_clause="{0}='{1}'".format(target_routeid, route))\
                as del_cursor:
            for _ in del_cursor:
                del_cursor.deleteRow()  # If the route already exist in the table then delete the whole route row

        cursor = da.InsertCursor(table_name, cols_dtype.keys())  # Create an insert cursor
        for index, row in df_route.iterrows():  # Iterate over available rows
            row_object = []  # Create the new row object
            for col_name in cols_dtype.keys():
                row_object.append(row[col_name])
            cursor.insertRow(row_object)  # Insert the new row


def trim_event_table(dataframe, routeid_col, to_m_col, lrs_network, lrs_routeid, workspace):
    """
    This function will trim event table to fit the LRS Network Max Measurement.
    :param dataframe: The event DataFrame
    :param routeid_col: The RouteID column of the event table
    :param to_m_col: The From Measure column of the event table
    :param lrs_network : The LRS Network Feature Class
    :param lrs_routeid : The LRS Network RouteID column
    :param workspace : The SDE Connection for LRS Network
    :return: Modified Event DataFrame
    """