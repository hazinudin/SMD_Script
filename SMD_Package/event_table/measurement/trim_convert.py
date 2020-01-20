from arcpy import env, da
from SMD_Package.load_config import SMDConfigs
from SMD_Package.event_table.lrs import route_geometry
from SMD_Package.FCtoDataFrame import event_fc_to_df


def convert_and_trim(dataframe, routeid_col, from_m_col, to_m_col, lane_code, conversion=100, fit_to='LRS'):
    """
    This function will convert the input DataFrame to the specified conversion and trim the input DataFrame measurement
    column to fit the LRS Maximum measurement value.
    :param dataframe: The input DataFrame.
    :param routeid_col: The RouteID column of the evnet table.
    :param from_m_col: The From Measurement column of the event table.
    :param to_m_col: The To Measurement column of the event table.
    :param lane_code: The Lane Code column of the evne table.
    :param conversion:  Conversion factor.
    :return: Modified DataFrame.
    """
    df = dataframe
    _convert_measurement(df, from_m_col, to_m_col, conversion=conversion)  # Convert the measurement
    _trim(df, routeid_col, to_m_col, lane_code, fit_to=fit_to, rni_to_km=conversion)
    return df


def _trim(dataframe, routeid_col, to_m_col, lane_code, fit_to=None, rni_to_km=None):
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
    config = SMDConfigs()
    lrs_network = config.table_names['lrs_network']
    lrs_routeid = config.table_fields['lrs_network']['route_id']
    rni_table = config.table_names['rni']
    rni_routeid = config.table_fields['rni']['route_id']
    rni_to_col = config.table_fields['rni']['to_measure']
    rni_lane_code = config.table_fields['rni']['lane_code']
    workspace = config.smd_database['instance']

    routes = df[routeid_col].unique().tolist()  # All the routes in the input DataFrame
    env.workspace = workspace

    for route in routes:  # Iterate over all available route in the input DataFrame
        df_route = df.loc[df[routeid_col] == route]  # Create a DataFrame for a single route
        lanes = df_route[lane_code].unique().tolist()  # List of lanes

        if fit_to == 'LRS':
            lrs_geom = route_geometry(route, lrs_network, lrs_routeid)
        elif fit_to == 'RNI':
            rni_df = event_fc_to_df(rni_table, [rni_to_col, rni_lane_code], route, rni_routeid, workspace,
                                    is_table=True)

        for lane in lanes:
            if fit_to == 'LRS':
                max_m = lrs_geom.lastPoint.M
            elif fit_to == 'RNI':
                max_m = rni_df.loc[rni_df[rni_lane_code] == lane, rni_to_col].max()
                max_m = float(max_m)/rni_to_km

            df_route_lane = df_route.loc[df_route[lane_code] == lane]  # Lane in route DataFrame.
            df_route_lane['_diff'] = df_route_lane[to_m_col] - max_m  # Create a difference col

            outbound_meas = df_route_lane.loc[df_route_lane['_diff'] > 0]  # All row which lies outside the lRS max m

            if len(outbound_meas) != 0:
                closest_to = outbound_meas[to_m_col].idxmin()  # Find the index of closest to_m
                drop_ind = outbound_meas.index.tolist()  # The row which completely out of bound
                drop_ind.remove(closest_to)

                # Replace the closest value to_m with LRS Max Measurement value
                df.loc[closest_to, [to_m_col]] = max_m
                # Drop all the row which is completely out of range
                df.drop(drop_ind, inplace=True)
            else:
                pass

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
