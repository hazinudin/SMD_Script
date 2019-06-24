from arcpy import env, da

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
