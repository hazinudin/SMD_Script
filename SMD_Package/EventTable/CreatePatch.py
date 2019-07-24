"""
This script will create a patch for end gap in a Event Table
"""

import numpy as np
from arcpy import env, da
from pandas import concat


def create_patch(input_df, lrs_network, lrs_routeid, routeid_col='LINKID', from_m_col='STA_FROM', to_m_col='STA_TO',
                 lane_code='LANE_CODE', x_col='STATO_LONG', y_col='STATO_LAT', z_col='STATO_ALT', increment=0.1,
                 to_meters=1000, meanrows=3, workspace=None):
    """
    This function will create a patch for an input Event table, the patch will be created based on the measurement value
    of the LRS Network feature class
    :param input_df: The Input Event Table
    :param routeid_col: The RouteID column of the input Event Table
    :param from_m_col: The From Measure column of the input table
    :param to_m_col: The To Measure column of the input table
    :param lane_code: The Lane Code column of the input table
    :param x_col: The x/longitude column of the input table
    :param y_col: The y/latitude column of the input table
    :param z_col: The z/elevation column of the input table
    :param lrs_network: The LRS Network used as reference
    :param lrs_routeid: The RouteID column of the LRS Network Feature Class.
    :param increment: The increment between From Measure and To Measure
    :param to_meters: The increment conversion factor to Meters unit.
    :param meanrows: The amount of row used to create the summary or the attribute of  the patch rows.
    :param workspace: The SDE workspace used to access the reference feature class/table.
    :return: Modified input DataFrame.
    """

    if workspace is None:  # If there is no specified workspace then pass
        pass
    else:
        env.workspace = workspace

    df = input_df  # The input DataFrame.
    input_routes = df[routeid_col].unique().tolist()  # All the routes in the input DataFrame

    # Iterate over all route in the input DataFrame
    for route in input_routes:
        df_route = df.loc[df[routeid_col] == route]  # Create route DataFrame

        # Access the LRS Network to get LRS geometry for current route
        with da.SearchCursor(lrs_network, 'SHAPE@', where_clause="{0}='{1}'".format(lrs_routeid, route)) as cursor:
            for row in cursor:
                lrs_geom = row[0]

        # Check if route data is shorter than LRS
        route_data_max = df_route[to_m_col].max()
        lrs_max = lrs_geom.lastPoint.M
        max_diff = lrs_max - route_data_max
        if route_data_max < lrs_max:  # If the route event data is shorter than LRS max m value.

            # Start creating a patch for this route
            # The route's lane from the last interval
            last_lanes = df_route.loc[np.isclose(df_route[to_m_col], route_data_max)][lane_code].unique().tolist()

            # Iterate over last lanes
            for lane in last_lanes:
                df_lane = df_route.loc[df_route[lane_code] == lane]
                lane_max_ind = df_lane[to_m_col].idxmax()

                if max_diff > increment:
                    # Normalized the last to-m value
                    df.at[lane_max_ind, to_m_col] = df_lane.at[lane_max_ind, from_m_col] + increment  # Replace the value in df
                    df_lane.at[lane_max_ind, to_m_col] = df_lane.at[lane_max_ind, from_m_col] + increment  # Replace the value in df_lane
                    normalized_max = df_lane[to_m_col].max()

                    # New row properties
                    new_from_m = np.arange(normalized_max, lrs_max, increment)
                    new_row_count = len(new_from_m)-1

                    # Create the summary row
                    num_summary = df_lane.tail(meanrows).describe(include=[np.number]).loc[['mean']].reset_index(drop=True)
                    obj_summary = df_lane.tail(meanrows).describe(include=[object]).loc[['top']].reset_index(drop=True)
                    new_row = concat([num_summary, obj_summary], axis=1, join_axes=[num_summary.index])
                    new_rows = new_row.append([new_row]*new_row_count, ignore_index=True)

                    # Iterate over row in new_rows to replace the value of from m, to m and lane code
                    for index, row in new_rows.iterrows():
                        _from_m = new_from_m[index]  # The from measure of new row
                        _to_m = new_from_m[index]+increment  # The to measure of new row
                        _lane_code = lane  # The lane code of new row

                        _point_geom = lrs_geom.positionAlongLine(_from_m*to_meters).projectAs('4326')
                        _x_coords = _point_geom.lastPoint.X
                        _y_coords = _point_geom.lastPoint.Y
                        _z_val = _point_geom.lastPoint.Z

                        new_rows.at[index, from_m_col] = _from_m  # Assign the from measure value
                        if index == (len(new_from_m)-1):
                            new_rows.at[index, to_m_col] = lrs_max  # If the row is the last row then assign the lrs max
                        else:
                            new_rows.at[index, to_m_col] = _to_m
                        new_rows.at[index, lane_code] = lane  # Assign the lane code
                        new_rows.at[index, x_col] = _x_coords  # Assign the x coordinate
                        new_rows.at[index, y_col] = _y_coords  # Assign the y coordinate
                        new_rows.at[index, z_col] = _z_val  # Assign the z value

                    # Insert the newly created rows to DataFrame
                    df = df.append(new_rows, ignore_index=True)

                if max_diff < increment:  # If the gap/difference is less than increment, then modify the last row to_m
                    df.at[lane_max_ind, to_m_col] = lrs_max  # Stretch the to_m value to match the LRS max value.
        else:
            pass

    return df
