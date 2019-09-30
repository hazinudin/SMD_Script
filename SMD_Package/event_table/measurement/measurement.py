def flip_measurement(dataframe, from_m_col, to_m_col, segment_len_col, length=10):
    """
    This function will flip the measurement value to compensate wrong survey direction.
    :param dataframe: The input DataFrame.
    :param from_m_col: From Measure column.
    :param to_m_col: To Measure column.
    :param segment_len_col: Segment length column.
    :param length: Segment length default value.
    :return:
    """
    df = dataframe
    max_m = df[to_m_col].max()  # The To Measure max value.

    # Flip the measurement, From become To and vice-versa.
    df[[to_m_col, from_m_col]] = df[[from_m_col, to_m_col]].apply(lambda x: (x - max_m).abs())

    offset = length - df.at[df[to_m_col].idxmin(), to_m_col]  # The offset value
    first_segment = df[from_m_col] == 0  # First segment

    df.loc[first_segment, [to_m_col]] = df[to_m_col] + offset  # Add the offset value
    df.loc[~first_segment, [from_m_col, to_m_col]] = df[[from_m_col, to_m_col]] + offset

    df[segment_len_col] = (df[to_m_col]-df[from_m_col]).astype('float')/100

    return df
