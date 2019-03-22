def rni_segment_dissolve(df_rni, groupby_field, code_lane_field, route_id_field, from_m_field='FROMMEASURE',
                         to_m_field='TOMEASURE'):
    """
    Dissolve the segment in RNI table if the segment has a same lane code combination with the
    next segment in single route. This function return a dictionary with a groupby group as a key=(route_id, code_lane)
    and rni_groupped index as value.

    Key and Value example = (u'6201611', "[u'L1', u'L2', u'R1', u'R2']"): [[7668L, 7681L], [7683L, 7686L]]

    The group is lane from route '6201611' with lane code combination of [u'L1', u'L2', u'R1', u'R2']. This lane code
    combination for this route start from row 7668 to 7681, ending at row 7682, and start again at 7683 to 7686.
    The pattern end at 7682 because that row contains different lane code combination.
    """

    # Groupped the RNI dataframe based on groupby_field
    rni_groupped = df_rni.groupby(by=groupby_field)[code_lane_field].unique().reset_index()

    # Sort the list value in the Lane Code column
    rni_groupped[code_lane_field] = rni_groupped[code_lane_field].apply(lambda x: sorted(x))
    rni_groupped[code_lane_field] = rni_groupped[code_lane_field].astype(str)

    # Basically do another groupby to the result of the first groupby, to get the group of segment with same lane code
    lane_code_combination_groups = rni_groupped.groupby(by=[route_id_field, code_lane_field]).groups

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

                    dissolved_segment[group].append(from_measure)
                    dissolved_segment[group].append(to_measure)
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
