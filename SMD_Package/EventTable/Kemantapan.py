import pandas as pd
import numpy as np
import json
from SMD_Package.FCtoDataFrame import event_fc_to_df


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
        with open('SMD_Package/EventTable/surftype_group.json') as group_json:
            group_details = json.load(group_json)  # Load the surface type group JSON

        df_rni[rni_from_col] = pd.Series(df_rni[rni_from_col]*100).astype(int)  # Create a integer measurement column
        df_rni[rni_to_col] = pd.Series(df_rni[rni_to_col]*100).astype(int)
        self.df_rni = df_rni
        self.rni_route_col = rni_route_col
        self.rni_from_col = rni_from_col
        self.rni_to_col = rni_to_col
        self.surftype_col = surftype_col

        self.group_details = group_details

        # The input and RNI DataFrame merge result
        merge_df = self.rni_table_join(df_rni, df_event, route_col, from_m_col, to_m_col, grading_col,
                                       rni_route_col, rni_from_col, rni_to_col, surftype_col)
        self.graded_df = self.grading(merge_df, surftype_col, grading_col, group_details)
        self.mantap_percent = self.kemantapan_percentage(self.graded_df, route_col, from_m_col, to_m_col)

    def summary(self):
        # Create the pivot table
        pivot = self.create_pivot()
        required_grades = np.array(['baik', 'sedang', 'rusak ringan', 'rusak berat'])

        # Create the Column for Missing Grade in Every Surface Type.
        surftype_set = set(x for x in pivot.columns.get_level_values(0))  # All the list of surface type
        missing_surftype = np.setdiff1d(['paved', 'unpaved'], list(surftype_set))  # Check for missing surface type

        # If there is a missing surface type in the pivot table, then add the missing surface type to pivot table
        if len(missing_surftype) != 0:
            for surface in missing_surftype:
                for grade in required_grades:
                    pivot[(surface, grade)] = pd.Series(0, index=pivot.index)  # Contain 0 value

        for surface in surftype_set:
            # The existing surface grade in a surface type.
            surface_grades = np.array(pivot[surface].columns.tolist())

            # Check for missing grade.
            missing_grade = np.setdiff1d(required_grades, surface_grades)
            for grade in missing_grade:  # Iterate over all missing grade
                pivot[surface, grade] = pd.Series(0, index=pivot.index)  # Add the missing grade column

            surface_df = pivot.loc[:, [surface]]  # Create the DataFrame for a single surface
            grade_percent = surface_df.div(surface_df.sum(axis=1), axis=0)*100
            surface_grades = np.array(grade_percent[surface].columns.values)
            percent_col = pd.Index([x+'_p' for x in surface_grades])
            grade_percent.columns = percent_col

            upper_col = dict()
            upper_col[surface] = grade_percent
            grade_percent = pd.concat(upper_col, axis=1)

            pivot = pivot.join(grade_percent, how='inner')

        # Flatten the Multi Level Columns
        new_column = pd.Index([str(x[0]+'_'+x[1].replace(' ', '')) for x in pivot.columns.values])
        pivot.columns = new_column

        return pivot

    def comparison(self, compare_table, grading_col, route_col, from_m_col, to_m_col, route, sde_connection):
        """
        Compare the Kemantapan percentage from the event table and the compare table.
        :param compare_table: The Feature Class used for comparing the kemantapan status.
        :param grading_col: The column in the compare_table used for grading.
        :param route_col: The RouteID column in the compare_table
        :param from_m_col: The from measure column in the compare_table
        :param to_m_col: The to measure column in the compare_table
        :param route: Route selection for compare_table
        :param sde_connection: The SDE connection for accessing compare_table
        :return:
        """
        # Create the compare_table DataFrame
        comp_df = event_fc_to_df(compare_table, [route_col, from_m_col, to_m_col, grading_col], route, route_col,
                                 sde_connection, orderby=None)
        comp_df[from_m_col] = pd.Series(comp_df[from_m_col]*100)
        comp_df[to_m_col] = pd.Series(comp_df[to_m_col]*100)

        merge_comp = self.rni_table_join(self.df_rni, comp_df, route_col, from_m_col, to_m_col, grading_col,
                                         self.rni_route_col, self.rni_from_col, self.rni_to_col, self.surftype_col)
        graded_comp = self.grading(merge_comp, self.surftype_col, grading_col, self.group_details)
        mantap_comp = self.kemantapan_percentage(graded_comp, route_col, from_m_col, to_m_col)

        return mantap_comp

    def create_pivot(self):
        pivot = self.graded_df.pivot_table('_len', index='LINKID', columns=['_surf_group', '_grade'], aggfunc=np.sum,
                                           fill_value=0)
        return pivot

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

    @staticmethod
    def grading(df_merge, surftype_col, grading_col, group_details, grading_result='_grade'):
        """
        This static method will grade every segment in the df_merge to ("baik", "sedang", "rusak_ringan", "rusak_berat")
        based on the segment surface type group and value in the grading column.
        :param df_merge: The merge result of event DataFrame and the RNI DataFrame.
        :param surftype_col: The surface type column in the merge result.
        :param grading_col: The value used in the grading process.
        :param grading_result: The new column used to store the grading result.
        :return: The df_merge with new column which store the grading result.
        """
        # Iterate over all row in the df_merge
        for index, row in df_merge.iterrows():
            group_not_found = True

            while group_not_found:  # Iterate until a group is found

                for group in group_details:
                    if row[surftype_col] in group_details[group]['group']:  # If the group was found
                        group_not_found = False  # group not found is False
                        surface_group = str(group)  # surface group
                        paved_group = group_details[group]['category']
                        range = np.array(group_details[group]['range'])  # group's range in np.array

            lower_bound = np.amin(range)  # The lower bound
            upper_bound = np.amax(range)  # The upper bound
            mid = range[1]  # The mid value

            df_merge.loc[index, '_surf_type'] = surface_group  # Write the surface group name in '_surf_group'
            df_merge.loc[index, '_surf_group'] = paved_group

            # Start the grading process
            if row[grading_col] <= lower_bound:
                grade = 'baik'
            if (row[grading_col] > lower_bound) & (row[grading_col] <= mid):
                grade = 'sedang'
            if (row[grading_col] > mid) & (row[grading_col] <= upper_bound):
                grade = 'rusak ringan'
            if row[grading_col] > upper_bound:
                grade = 'rusak berat'

            df_merge.loc[index, grading_result] = grade

        return df_merge

    @staticmethod
    def kemantapan_percentage(df_graded, route_col, from_m_col, to_m_col, grade_result_col='_grade',
                              kemantapan_col='_kemantapan'):
        """
        This function will find the length percentage of every route with 'Mantap' dan 'Tidak Mantap' status.
        :param df_graded: The event DataFrame which already being graded.
        :param grade_result_col: The column which store the grade statue for every segment.
        :param kemantapan_col: The newly added column which store the kemantapan status.
        :return: DataFrame with '_kemantapan' column.
        """
        df_graded.loc[:, '_len'] = pd.Series(df_graded[to_m_col]-df_graded[from_m_col])
        df_graded.loc[df_graded[grade_result_col].isin(['baik', 'sedang']), kemantapan_col] = 'mantap'
        df_graded.loc[df_graded[grade_result_col].isin(['rusak ringan', 'rusak berat']), kemantapan_col] = 'tidak mantap'

        kemantapan_len = df_graded.groupby(by=[route_col, kemantapan_col]).agg({'_len': 'sum'})
        kemantapan_prcnt = kemantapan_len.groupby(level=0).apply(lambda x: 100*x/float(x.sum())).reset_index()
        kemantapan_prcnt.set_index(kemantapan_col, inplace=True)

        return kemantapan_prcnt





