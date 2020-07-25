import json
from pandas import Series, DataFrame, merge
from SMD_Package.load_config import SMDConfigs
from SMD_Package.FCtoDataFrame import event_fc_to_df


def add_rni_data(df, routeid_col, from_m_col, to_m_col, lane_code_col, connection, added_column=None, how='inner',
                 kwargs_comparison=None, mfactor=1, agg_func=None):
    """
    This functions perform a merge operation between the inputted DataFrame with RNI DataFrame to add requested RNI'
    column to the inputted DataFrame.
    :param df: Input DataFrame.
    :param routeid_col: Route ID column of the input.
    :param from_m_col: From Measure column of the input.
    :param to_m_col: To Measure column of the input.
    :param lane_code_col: Lane Code column of the input.
    :param connection: SDE connection.
    :param added_column: Column from the RNI table which will be added to input DataFrame.
    :param how: Merge 'how', 'inner' or 'outer'.
    :param mfactor: The factor which will be multiplied to RNI from and to measurement value to match the input from-to
    measurement unit.
    :param agg_func: Aggregate function in dictionary format to be used in the pandas GroupBy.agg() function.
    :return:
    """

    smd_config = SMDConfigs()  # Load the SMD config JSON.
    rni_table = kwargs_comparison.get('table_name')
    rni_routeid = kwargs_comparison.get('route_id')
    rni_from_m = kwargs_comparison.get('from_measure')
    rni_to_m = kwargs_comparison.get('to_measure')
    rni_lane_code = kwargs_comparison.get('lane_code')

    if type(added_column) == list:  # Make sure the added column is a list variable.
        pass
    else:
        added_column = [added_column]

    routes = df[routeid_col].unique().tolist()  # All the routes in the input DataFrame.
    if lane_code_col is not None:
        rni_key = [rni_routeid, rni_from_m, rni_to_m, rni_lane_code]  # RNI table merge key.
        input_key = [routeid_col, from_m_col, to_m_col, lane_code_col]  # Input table merge key.
        request_cols = rni_key + added_column  # Requested columns.
        df_rni = event_fc_to_df(rni_table, request_cols, routes, rni_routeid, connection, True)  # Get the RNI df.

    else:
        rni_key = [rni_routeid, rni_from_m, rni_to_m]  # RNI table merge key.
        input_key = [routeid_col, from_m_col, to_m_col]  # Input table merge key.
        request_cols = rni_key + added_column  # Requested columns.
        df_rni = event_fc_to_df(rni_table, request_cols, routes, rni_routeid, connection, True)  # Get the RNI df.
        rni_g = df_rni.groupby(rni_key).agg(agg_func)
        df_rni = rni_g.reset_index()

    if len(df_rni) == 0:  # If all of the requested route does not have RNI data then return None.
        return None

    df_rni[rni_from_m] = df_rni[rni_from_m].apply(lambda x: x*mfactor).astype(int)  # Convert the from-to value.
    df_rni[rni_to_m] = df_rni[rni_to_m].apply(lambda x: x*mfactor).astype(int)

    merged = merge(df, df_rni, how=how, left_on=input_key, right_on=rni_key)  # Merge the RNI and input df.

    return merged


class RNIRouteDetails(object):
    def __init__(self, df_rni, rni_routeid, rni_from_measure, rni_to_measure, rni_details, agg_type='unique'):
        """
        This object will calculate the length of each surface type for every route in the input DataFrame.
        :param df_rni: The input RNI DataFrame.
        :param rni_routeid: RNI RouteID column
        :param rni_from_measure:
        :param rni_to_measure:
        :param rni_details:
        """
        groupby_cols = [rni_routeid, rni_from_measure, rni_to_measure]
        self.route_surf_segment = rni_segment_dissolve(df_rni, groupby_cols, rni_details, rni_routeid,
                                                       from_m_field=rni_from_measure, to_m_field=rni_to_measure,
                                                       agg=agg_type)

    def details_percentage(self, route):
        """
        This class method will calculate surface type group length in the requested route.
        :param route:
        :return:
        """
        surface_len = {}  # The output dictionary
        for group in self.route_surf_segment:
            group_route = group[0]  # The group RouteID
            if type(group[1]) == 'str':
                group_surface = json.loads(group[1])  # The group surface type

                if len(group_surface) == 1:
                    group_surface = str(group_surface[0])  # If there is only one surface type in an interval.
                else:
                    group_surface = str(group_surface).strip('[]')  # If there are multiple surface type.
            else:
                group_surface = group[1]

            if (group_route == route) or (group_route in route):
                segments = self.route_surf_segment[group]
                # Iterate over all segments in the group
                surface_group_len = 0
                for segment in segments:
                    segment_sta = segment[0]  # Segment from measure
                    segment_end = segment[1]  # Segment to measure
                    surface_group_len += (segment_end-segment_sta)  # Length accumulation from every segments.

                surface_len[group_surface] = surface_group_len

        total_length = Series(surface_len).sum()
        length_percentage = DataFrame(Series(surface_len).
                                      apply(lambda x: (x/total_length)*100), columns=['percentage']).reset_index()

        return length_percentage


def rni_segment_dissolve(df_rni, groupby_field, agg_field, route_id_field, from_m_field='STA_FROM',
                         to_m_field='STA_TO', agg='unique'):
    """
    Dissolve the segment in RNI table if the segment has a same lane code combination with the
    next segment in single route. This function return a dictionary with a groupby group as a key=(route_id, code_lane)
    and rni_groupped index as value.

    Key and Value example = (u'6201611', "[u'L1', u'L2', u'R1', u'R2']"): [[7668L, 7681L], [7683L, 7686L]]

    The group is lane from route '6201611' with lane code combination of [u'L1', u'L2', u'R1', u'R2']. This lane code
    combination for this route start from row 7668 to 7681, ending at row 7682, and start again at 7683 to 7686.
    The pattern end at 7682 because that row contains different lane code combination.
    """

    # Groupped the RNI DataFrame based on groupby_field
    if agg == 'sum':  # If the aggregation method is 'sum' type.
        rni_groupped = df_rni.groupby(by=groupby_field)[agg_field].sum().reset_index()
    elif agg == 'unique':  # If the aggregation method is 'unique' type.
        rni_groupped = df_rni.groupby(by=groupby_field)[agg_field].unique().reset_index()
        # Sort the list value in the Lane Code column
        rni_groupped[agg_field] = rni_groupped[agg_field].apply(lambda x: sorted(x))
        rni_groupped[agg_field] = rni_groupped[agg_field].astype(str)

    # Basically do another groupby to the result of the first groupby, to get the group of segment with same lane code
    lane_code_combination_groups = rni_groupped.groupby(by=[route_id_field, agg_field]).groups

    # Dictionary for storing the result
    dissolved_segment = {}
    # Iterate over the group of segment with same lane code combination
    for group in lane_code_combination_groups:
        segment_index_list = lane_code_combination_groups[group]
        segment_index_list = sorted(segment_index_list)

        if group not in dissolved_segment:
            dissolved_segment[group] = []

        reset_sequence = True
        # Iterate over segment within group to check the continuity of the segment with the same lane code combination
        for segment_index in segment_index_list:
            if reset_sequence:
                last_index = segment_index
                from_index = segment_index
                reset_sequence = False

                if len(segment_index_list) == 1:
                    from_measure = rni_groupped.at[from_index, from_m_field]
                    to_measure = rni_groupped.at[from_index, to_m_field]
                    from_to_measurement = [from_measure, to_measure]

                    dissolved_segment[group].append(from_to_measurement)
                    reset_sequence = True

            else:

                if segment_index - last_index == 1:
                    last_index = segment_index

                    if segment_index_list.index(segment_index) == len(segment_index_list) - 1:
                        from_measure = rni_groupped.at[from_index, from_m_field]
                        to_measure = rni_groupped.at[last_index, to_m_field]
                        from_to_measurement = [from_measure, to_measure]

                        dissolved_segment[group].append(from_to_measurement)

                elif segment_index - last_index > 1:
                    from_measure = rni_groupped.at[from_index, from_m_field]
                    to_measure = rni_groupped.at[last_index, to_m_field]
                    from_to_measurement = [from_measure, to_measure]

                    dissolved_segment[group].append(from_to_measurement)
                    from_index = segment_index
                    last_index = segment_index

                    if segment_index_list.index(segment_index) == len(segment_index_list) - 1:
                        from_measure = rni_groupped.at[from_index, from_m_field]
                        to_measure = rni_groupped.at[last_index, to_m_field]
                        from_to_measurement = [from_measure, to_measure]

                        dissolved_segment[group].append(from_to_measurement)

    # Return the result
    return dissolved_segment
