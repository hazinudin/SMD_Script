import pandas as pd


class Kemantapan(object):
    def __init__(self, df_rni, df_event, grading_col, route_col, from_m_col, to_m_col, rni_route_col, rni_from_col,
                 rni_to_col, surftype_col=None):
        """
        Initialize the Kemantapan class for grading kemantapan value
        :param df_rni: The DataFrame for RNI table.
        :param df_event: The DataFrame for the input table.
        :param grading_col: The value used for grading
        :param surftype_col: The column which store the surface type value in the RNI Table
        """
        df_rni[rni_from_col] = pd.Series(df_rni[rni_from_col]*100).astype(int)  # Create a integer measurement column
        df_rni[rni_to_col] = pd.Series(df_rni[rni_to_col]*100).astype(int)

        # The input and RNI DataFrame merge result
        self.merge_df = self.rni_table_join(df_rni, df_event, route_col, from_m_col, to_m_col, grading_col,
                                            rni_route_col, rni_from_col, rni_to_col, surftype_col)

    @staticmethod
    def rni_table_join(df_rni, df_event, route_col, from_m_col, to_m_col, grading_col, rni_route_col, rni_from_col,
                       rni_to_col, surftype_col, match_only=True):
        """
        This static method used for joining the input event table and the RNI table
        :param df_rni: The RNI DataFrame.
        :param df_event: The input event DataFrame.
        :param route_col: The column which stores the RouteID for event table.
        :param from_m_col: The From Measure column for event table.
        :param to_m_col: The To Measure column for event table.
        :param grading_col: The value used for calculating Kemantapan.
        :param rni_route_col: The column in RNI Table which stores the RouteID
        :param rni_from_col: The column in RNI Table which stores the From Measure.
        :param rni_to_col: The column in RNI Table which stores the To Measure.
        :param surftype_col: The column in RNI Table which stores the surface type data.
        :param match_only: If True then this method only returns the 'both' merge result.
        :return: A DataFrame from the merge result between the RNI and input event table.
        """
        input_group_col = [route_col, from_m_col, to_m_col]  # The column used for input groupby
        rni_group_col = [rni_route_col, rni_from_col, rni_to_col]  # The column used for the RNI groupby
        df_rni[surftype_col] = pd.Series(df_rni[surftype_col].astype(int))  # Convert the surftype to integer type

        # GroupBy the input event DataFrame to make summarize the value used for grading from all lane.
        input_groupped = df_event.groupby(by=input_group_col)[grading_col].mean().reset_index()

        # GroupBy the RNI Table to get the summary of surface type from all lane in a segment.
        rni_groupped = df_rni.groupby(by=rni_group_col)[surftype_col].\
            agg(lambda x: x.value_counts().index[0]).reset_index()

        # Merge the RNI DataFrame and the event DataFrame
        df_merge = pd.merge(input_groupped, rni_groupped, how='outer', left_on=input_group_col, right_on=rni_group_col,
                            indicator=True, suffixes=['_INPUT', '_RNI'])

        df_match = df_merge.loc[df_merge['_merge'] == 'both']  # DataFrame for only match segment interval
        return df_match if match_only else df_merge  # If 'match_only' is true then only return the 'both'

