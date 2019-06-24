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


def _trim_event_table(dataframe, routeid_col, to_m_col, lane_code, lrs_network, lrs_routeid, workspace):
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

    df = dataframe  # Create a DataFrame variable
    routes = df[routeid_col].unique().tolist()  # All the routes in the input DataFrame
    env.workspace = workspace

    for route in routes:  # Iterate over all available route in the input DataFrame
        df_route = df.loc[df[routeid_col] == route]  # Create a DataFrame for a single route
        lanes = df_route[lane_code].unique().tolist()  # List of lanes

        for lane in lanes:
            df_route_lane = df_route.loc[df_route[lane_code] == lane]  # Lane in route DataFrame.
            lrs_query = "{0} = '{1}'".format(lrs_routeid, route)
            with da.SearchCursor(lrs_network, 'SHAPE@', where_clause=lrs_query) as cur:
                for row in cur:
                    lrs_geom = row[0]

            lrs_max_m = lrs_geom.lastPoint.M  # The route LRS Max Measurement value
            df_route_lane['_diff'] = df_route_lane[to_m_col] - lrs_max_m  # Create a difference col

            outbound_meas = df_route_lane.loc[df_route_lane['_diff'] > 0]  # All row which lies outside the lRS max m
            closest_to = outbound_meas[to_m_col].idxmin()  # Find the index of closest to_m
            drop_ind = outbound_meas.index.tolist()  # The row which completely out of bound
            drop_ind.remove(closest_to)

            # Replace the closest value to_m with LRS Max Measurement value
            df.loc[closest_to, [to_m_col]] = lrs_max_m
            # Drop all the row which is completely out of range
            df.drop(drop_ind, inplace=True)

    return df


def _convert_measurement(dataframe, from_m_col, to_m_col, conversion=100):
    """
    This function will convert event table measurement.
    :param dataframe: The input DataFrame.
    :param from_m_col: The From Measure column in the input DataFrame.
    :param to_m_col: The To Measure column in the input DataFrame.
    :param conversion: The conversion factor which will be applied to the DataFrame.
    :return: Modified DataFrame
    """
    df = dataframe  # Create a DataFrame variable

    # Start the conversion
    df[from_m_col] = df[from_m_col].astype(float)/conversion  # Divide the from and to column with conversion
    df[to_m_col] = df[to_m_col].astype(float)/conversion

    return df
