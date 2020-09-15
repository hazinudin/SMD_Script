import pandas as pd
import numpy as np
import json
from SMD_Package.FCtoDataFrame import event_fc_to_df
from SMD_Package.load_config import SMDConfigs
from arcpy import env
import os


class Kemantapan(object):
    def __init__(self, df_event, grading_col, routeid_col, from_m_col, to_m_col, lane_code_col, data_type='IRI',
                 lane_based=False, rni_mfactor=1, to_km_factor=0.01, method='mean', **kwargs):
        """
        Initialize the Kemantapan class for grading kemantapan value
        :param df_event: The DataFrame for the input table.
        :param grading_col: The value used for grading
        :param routeid_col: The RouteID column of the input DataFrame.
        :param from_m_col: The From Measure column of the input DataFrame.
        :param to_m_col: The To Measure column of the input DataFrame.
        :param data_type: The type of kemantapan will be calculated. ROUGHNESS or PCI only. The selection will
        effect the amount of grading level.
        :param lane_based: Determine whether the Kemantapan will be calculated as lane based or calculated based on the
        segment interval.
        :param method: 'mean' or 'max'. This parameter determine the aggregation method used for summarizing event
        data.
        """
        # Convert the measurement value of the event dataframe to DM
        df_event[from_m_col] = df_event[from_m_col].astype(float)*to_km_factor*100
        df_event[to_m_col] = df_event[to_m_col].astype(float)*to_km_factor*100
        df_event[[from_m_col, to_m_col]] = df_event[[from_m_col, to_m_col]].round(1).astype(int)

        # make sure the kemantapan_type is between 'IRI and 'PCI'
        if data_type not in ['IRI', 'PCI']:
            raise Exception('{0} is not a valid kemantapan type.'.format(data_type))  # Raise an exception
        else:
            self.type = data_type

        if len(df_event) == 0:
            raise Exception('Input Event DataFrame is Empty')

        # Get the RNI table details.
        rni_table = SMDConfigs().table_names['rni']
        rni_route_col = SMDConfigs().table_fields['rni']['route_id']
        rni_from_col = SMDConfigs().table_fields['rni']['from_measure']
        rni_to_col = SMDConfigs().table_fields['rni']['to_measure']
        rni_lane_code = SMDConfigs().table_fields['rni']['lane_code']
        surftype_col = SMDConfigs().table_fields['rni']['surface_type']

        self.rni_table = rni_table
        self.rni_route_col = rni_route_col
        self.rni_from_col = rni_from_col
        self.rni_to_col = rni_to_col
        self.rni_lane_code = rni_lane_code
        self.surftype_col = surftype_col
        self.grading_col = grading_col
        self.route_col = routeid_col

        self.__dict__.update(kwargs)

        rni_request_cols = [self.rni_route_col, self.rni_from_col, self.rni_to_col, self.rni_lane_code, self.surftype_col]
        input_routes = df_event[routeid_col].unique().tolist()

        df_rni = event_fc_to_df(self.rni_table, rni_request_cols, input_routes, self.rni_route_col, env.workspace, True)
        df_rni[self.rni_from_col] = pd.Series(df_rni[self.rni_from_col]*rni_mfactor).round(1).astype(int)  # Convert the RNI measurement
        df_rni[self.rni_to_col] = pd.Series(df_rni[self.rni_to_col]*rni_mfactor).round(1).astype(int)

        self.df_rni = df_rni
        self.group_details = self.group_details()
        self.group_details_df = self.group_details_df()
        self.lane_based = lane_based
        self.lane_code = lane_code_col
        self.graded_df = None
        self.missing_rni = False

        if not df_rni.empty:
            # The input and RNI DataFrame merge result
            merge_df = self.rni_table_join(df_rni, df_event, routeid_col, from_m_col, to_m_col, grading_col,
                                           self.rni_route_col, self.rni_from_col, self.rni_to_col, self.surftype_col, lane_based,
                                           match_only=False, lane_code=lane_code_col, rni_lane_code=self.rni_lane_code,
                                           agg_method=method)
            self.merged_df = merge_df
            self.match_only = merge_df.loc[merge_df['_merge'] == 'both']
            self.grading(surftype_col, grading_col)
            self.mantap_percent = self.kemantapan_percentage(self.graded_df, routeid_col, from_m_col, to_m_col, 0.01)
            self.no_match_route = merge_df.loc[merge_df['_merge'] == 'left_only', routeid_col].tolist()

            if len(self.no_match_route) != 0:
                self.all_match = False
            else:
                self.all_match = True
        else:
            self.all_match = False
            self.missing_rni = True
            self.no_match_route = input_routes

    def summary(self, flatten=True, lane_km=True):
        """
        Create a summary DataFrame which contain the length for every road grade and the percentage for every road grade
        in a single route. The column with '_p' suffix contain the length percentage.
        :param flatten: If true then the returned DataFrame does not use any multi index column.
        :return:
        """
        if not self.lane_based:
            # Create the pivot table
            pivot_grade = self.create_pivot(columns=['_surf_group', '_grade'])
            pivot_mantap = self.create_pivot(columns=['_surf_group', '_kemantapan'])
            pivot_mantap_all = self.create_pivot(columns=['_kemantapan'])
            pivot_grade_all = self.create_pivot(columns=['_grade'])
        elif self.lane_based:
            # Create the pivot table
            pivot_grade = self.create_pivot(columns=['_surf_group', '_grade'], lane_code=self.lane_code, lane_km=lane_km)
            pivot_mantap = self.create_pivot(columns=['_surf_group', '_kemantapan'], lane_code=self.lane_code, lane_km=lane_km)
            pivot_mantap_all = self.create_pivot(columns=['_kemantapan'], lane_code=self.lane_code, lane_km=lane_km)
            pivot_grade_all = self.create_pivot(columns=['_grade'], lane_code=self.lane_code, lane_km=lane_km)

        required_grades = np.array(['good', 'fair', 'poor', 'bad'])

        # # All the required grades and surfaces
        # if self.type == 'IRI':
        #     required_grades = np.array(['good', 'fair', 'poor', 'bad'])
        # elif self.type == 'PCI':
        #     required_grades = np.array(['good', 'satisfactory', 'fair', 'poor', 'very poor', 'serious', 'failed'])

        required_mantap = np.array(['mantap', 'tdk_mantap'])
        required_surftype = ['p', 'up']

        # Complete all the surface type and surface grades in every pivot table.
        pivot_grade = self._complete_surftype_grade(pivot_grade, required_grades, required_surftype)
        pivot_mantap = self._complete_surftype_grade(pivot_mantap, required_mantap, required_surftype)
        pivot_mantap_all = self._complete_surftype_grade(pivot_mantap_all, required_mantap, None)
        pivot_grade_all = self._complete_surftype_grade(pivot_grade_all, required_grades, None)

        # Add suffix for non-percentage table.
        pivot_grade_s = self._add_suffix(pivot_grade, '_km', levels=1)
        pivot_mantap_s = self._add_suffix(pivot_mantap, '_km', levels=1)
        pivot_grade_all_s = self._add_suffix(pivot_grade_all, '_km', levels=0)
        pivot_mantap_all_s = self._add_suffix(pivot_mantap_all, '_km', levels=0)

        # Create all the percentage DataFrame
        pivot_grade_p = self._percentage(pivot_grade, required_surftype, modify_input=False)
        pivot_mantap_p = self._percentage(pivot_mantap, required_surftype, modify_input=False)
        pivot_grade_all_p = self._percentage_singlecol(pivot_grade_all, modify_input=False)
        pivot_mantap_all_p = self._percentage_singlecol(pivot_mantap_all, modify_input=False)

        # Join the multilevel column DataFrame first
        pivot_join = pivot_grade_s.join(pivot_mantap_s)
        pivot_join = pivot_join.join(pivot_grade_p)
        pivot_join = pivot_join.join(pivot_mantap_p)

        if flatten:
            # Flatten the Multi Level Columns
            new_column = pd.Index([str(x[0]+'_'+x[1].replace(' ', '')) for x in pivot_join.columns.values])
            pivot_join.columns = new_column

            # Join all the single level column DataFrame.
            pivot_join = pivot_join.join(pivot_grade_all_s)
            pivot_join = pivot_join.join(pivot_mantap_all_s)
            pivot_join = pivot_join.join(pivot_grade_all_p)  # Summary of all surface group
            pivot_join = pivot_join.join(pivot_mantap_all_p)  # Summary of all surface group

            # The grade average for all route
            avg_grade = self.graded_df.groupby(by=[self.route_col])[self.grading_col].mean()

            pivot_join = pivot_join.join(avg_grade)  # Join the average grade DataFrame.

        return pivot_join

    @staticmethod
    def _add_suffix(pivot_table, suffix, levels=1):
        """
        This static method will add suffix to a pivot table.
        :param pivot_table: The input pivot table.
        :param suffix: The suffix that will be added to the column.
        :param levels: The number of column level in the input pivot table.
        :return: Modified pivot table.
        """

        if levels == 0:  # For single level pivot table
            cols = np.array(pivot_table.columns.get_level_values(levels))
            result = pivot_table.rename(columns={x: (x + suffix) for x in cols})
            return result
        else:  # For multilevel pivot table
            compiled = None  # For compiling the result
            # Iterate over all upper column
            for upper_col in pivot_table.columns.get_level_values(levels-1).unique():
                lower = pivot_table[upper_col]  # The lower DataFrame
                cols = np.array(lower.columns.values)  # The columns in the lower DataFrame
                cols_w_suffix = pd.Index([(x + suffix) for x in cols])  # Create the column with the suffix
                lower.columns = cols_w_suffix  # Assign the columns with the suffix

                upper = dict()
                upper[upper_col] = lower
                lower = pd.concat(upper, axis=1)

                if compiled is None:
                    compiled = lower
                else:
                    compiled = compiled.join(lower)

            return compiled

    @staticmethod
    def _complete_surftype_grade(pivot_table, required_grades, required_surftype):
        """
        This static method is used to complete the required surface type and grade columns in every surface type upper
        index column. Example:

                    input                                         output

        paved                       ||  paved                       | unpaved
        baik  sedang  rusak ringan  ||  baik  sedang  rusak ringan  | baik  sedang  rusak ringan  rusak berat

        :param pivot_table:  The input pivot table.
        :param required_grades:  The required grades. Example ['baik', 'sedang', ...] or ['mantap', 'tidak mantap'].
        :param required_surftype:  The requierd surface type. Example ['paved', 'unpaved'].
        :return: Modified pivot table
        """
        if required_surftype is not None:  # If the surface type is specified (multilevel column)

            # Create the Column for Missing Grade in Every Surface Type.
            surftype_set = set(x for x in pivot_table.columns.get_level_values(0))  # All the list of surface type
            missing_surftype = np.setdiff1d(required_surftype, list(surftype_set))  # Check for missing surface type

            # Iterate over all available surftype:
            for surface in surftype_set:
                surface_grades = np.array(pivot_table[surface].columns.tolist())
                missing_grades = np.setdiff1d(required_grades, surface_grades)

                # Check for missing grade in available surface type
                for grade in missing_grades:
                    # Add the missing grade
                    pivot_table[surface, grade] = pd.Series(0, index=pivot_table.index)

            # If there is a missing surface type in the pivot table, then add the missing surface type to pivot table
            if len(missing_surftype) != 0:
                for surface in missing_surftype:
                    for grade in required_grades:
                        pivot_table[(surface, grade)] = pd.Series(0, index=pivot_table.index)  # Contain 0 value

        else:  # if the surface type is not specified (single level column)
            pivot_grade = pivot_table.columns.values  # The available grade in the pivot table
            missing_grade = np.setdiff1d(required_grades, pivot_grade)  # Check for missing grade
            for grade in missing_grade:
                # Add the missing grade
                pivot_table[grade] = pd.Series(0, index=pivot_table.index)  # Add the missing grade column

        return pivot_table

    @staticmethod
    def _percentage(pivot_table, required_surftype, suffix='_psn', modify_input=False):
        """
        This static method will add a percentage column for every required grades in the pivot table. The newly added
        column will have an suffix determined by a parameter.
        If the pivot table have a missing grade, then a new column will be added which contain 0 value.
        :param pivot_table: The input pivot table.
        :param required_surftype: The required surface type.
        :param suffix: The percentage column name suffix.
        :return: Modified pivot table
        """
        # Iterate over all required surface type
        result = None  # Variable for compiling the result
        for surface in required_surftype:

            surface_df = pivot_table.loc[:, [surface]]  # Create the DataFrame for a single surface
            grade_percent = surface_df.div(surface_df.sum(axis=1), axis=0) * 100
            surface_grades = np.array(grade_percent[surface].columns.values)
            percent_col = pd.Index([(x + suffix) for x in surface_grades])  # Create the percentage column. suffix '_p'
            grade_percent.columns = percent_col
            grade_percent.fillna(0, inplace=True)  # Fill the NA value with zero

            upper_col = dict()
            upper_col[surface] = grade_percent
            grade_percent = pd.concat(upper_col, axis=1)

            if modify_input:
                # Join the pivot table with the percent table
                result = pivot_table.join(grade_percent, how='inner')
            else:
                if result is None:
                    result = grade_percent  # Initalize the result variable
                else:
                    result = result.join(grade_percent, how='inner')  # Join the result

        return result  # Return the percent DataFrame without modifying the input pivot table.

    @staticmethod
    def _percentage_singlecol(pivot_table, suffix='_psn', modify_input=False):
        """
        This static method will add a percentage column for every required grades in the pivot table. The newly added
        column will have an suffix determined by a parameter.
        If the pivot table have a missing grade, then a new column will be added which contain 0 value.
        :param pivot_table: The input pivot table.
        :param required_grades: The required grades.
        :param suffix: The percentage column name suffix.
        :return: Modified pivot table.
        """

        grade_percent = pivot_table.div(pivot_table.sum(axis=1), axis=0) * 100
        grades = np.array(pivot_table.columns.values)
        percent_col = pd.Index([(x + suffix) for x in grades])  # Create the percentage column. suffix '_p'
        grade_percent.columns = percent_col
        grade_percent.fillna(0, inplace=True)  # Fill the NA value with zero

        if modify_input:
            # Join the pivot table with the percent table
            pivot_table = pivot_table.join(grade_percent, how='inner')
            return pivot_table
        else:
            return grade_percent

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
                                 sde_connection)

        if len(comp_df) == 0:  # If the comparison table is empty
            return None

        comp_df[from_m_col] = pd.Series(comp_df[from_m_col]*100)
        comp_df[to_m_col] = pd.Series(comp_df[to_m_col]*100)

        merge_comp = self.rni_table_join(self.df_rni, comp_df, route_col, from_m_col, to_m_col, grading_col,
                                         self.rni_route_col, self.rni_from_col, self.rni_to_col, self.surftype_col)
        graded_comp = self.grading(merge_comp, self.surftype_col, grading_col, self.group_details)
        mantap_comp = self.kemantapan_percentage(graded_comp, route_col, from_m_col, to_m_col)

        return mantap_comp

    def create_pivot(self, columns, lane_code=None, lane_km=False):
        """
        Create a pivot DataFrame from the DataFrame which already being graded
        :param columns: The column used to create pivot table.
        :param lane_code: The lane code column.
        :param lane_km: If True then the calculation will sum all the segment length from all available lane.
        :return:
        """
        if lane_code is None:  # If no lane code column is specified
            pivot = self.graded_df.pivot_table('_len', index=self.route_col, columns=columns, aggfunc=np.sum,
                                               fill_value=0)
        elif lane_code is not None:
            pivot = self.graded_df.pivot_table('_len', index=[self.route_col, lane_code], columns=columns, aggfunc=np.sum,
                                               fill_value=0)

            if lane_km:
                pivot_reset = pivot.reset_index()
                grouped = pivot_reset.groupby(self.route_col).sum()
                return grouped

        return pivot

    @staticmethod
    def rni_table_join(df_rni, df_event, route_col, from_m_col, to_m_col, grading_col, rni_route_col, rni_from_col,
                       rni_to_col, surftype_col, lane_based, match_only=True, lane_code=None, rni_lane_code=None,
                       agg_method='mean'):
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
        :param lane_based: If true then event DataFrame will be grouped before joined to RNI DataFrame.
        :param match_only: If True then this method only returns the 'both' merge result.
        :param lane_code: The Input DataFrame lane code column.
        :param rni_lane_code: The RNI Table lane code column.
        :param agg_method: Method used for grouping.
        :return: A DataFrame from the merge result between the RNI and input event table.
        """
        if not lane_based:  # Do the table join with linkid, from, and to as join key.
            input_group_col = [route_col, from_m_col, to_m_col]  # The column used for input groupby
            rni_group_col = [rni_route_col, rni_from_col, rni_to_col]  # The column used for the RNI groupby
            df_rni[surftype_col] = pd.Series(df_rni[surftype_col].astype(int))  # Convert the surftype to integer type

            # GroupBy the input event DataFrame to make summarize the value used for grading from all lane.
            if agg_method == 'mean':
                input_groupped = df_event.groupby(by=input_group_col)[grading_col].mean().reset_index()
            elif agg_method == 'max':
                input_groupped = df_event.groupby(by=input_group_col)[grading_col].max().reset_index()
            else:
                raise ValueError("'{0}' is not a valid agg_method.".format(agg_method))

            # GroupBy the RNI Table to get the summary of surface type from all lane in a segment.
            # Get the first surface type in single RNI segment group
            rni_groupped = df_rni.groupby(by=rni_group_col)[surftype_col].\
                agg(lambda x: x.value_counts().index[0]).reset_index()

            # Merge the RNI DataFrame and the event DataFrame
            df_merge = pd.merge(input_groupped, rni_groupped, how='outer', left_on=input_group_col, right_on=rni_group_col,
                                indicator=True, suffixes=['_INPUT', '_RNI'])

            df_match = df_merge.loc[df_merge['_merge'] == 'both']  # DataFrame for only match segment interval
            return df_match if match_only else df_merge  # If 'match_only' is true then only return the 'both'

        elif lane_based:  # Do the table join with linkid, from, to and lane code as join key
            input_key = list([route_col, from_m_col, to_m_col, lane_code])
            rni_key = list([rni_route_col, rni_from_col, rni_to_col, rni_lane_code])
            rni_col = list(rni_key)  # RNI table column used for merge
            rni_col.append(surftype_col)

            df_merge = pd.merge(df_event, df_rni[rni_col], how='outer', left_on=input_key, right_on=rni_key, indicator=True,
                                suffixes=['_INPUT', '_RNI'])

            df_match = df_merge.loc[df_merge['_merge'] == 'both']  # DataFrame for only match segment interval
            return df_match if match_only else df_merge

    @staticmethod
    def static_grading(df_merge, surftype_col, grading_col, group_details, kemantapan_type, grading_result='_grade',
                       grading_level='_grade_level', surftype_group='_surf_type', surftype_cat='_surf_group'):
        """
        This static method will grade every segment in the df_merge to ("baik", "sedang", "rusak_ringan",
        "rusak_berat")
        based on the segment surface type group and value in the grading column.
        :param df_merge: The merge result of event DataFrame and the RNI DataFrame.
        :param surftype_col: The surface type column in the merge result.
        :param grading_col: The value used in the grading process.
        :param group_details: The surface type group details (grading value and surface type (paved or unpaved)).
        :param kemantapan_type: The kemantapan type tha will be calculated
        :param grading_result: The new column used to store the grading result.
        :param grading_level: The new column used to store the grade leve in integer.
        :param surftype_group: The new column used to store the surface group (asphalt, penmac, rigid)
        :param surftype_cat: The new column used to store the surface category (paved or unpaved)
        :return: The df_merge with new column which store the grading result.
        """
        # Iterate over all row in the df_merge
        for index, row in df_merge.iterrows():
            group_not_found = True

            for group in group_details:
                if row[surftype_col] in group_details[group]['group']:  # If the group was found
                    group_not_found = False  # group not found is False
                    surface_group = str(group)  # surface group
                    paved_group = group_details[group]['category']
                    range = np.array(group_details[group]['iri_range'])  # group's range in np.array

            if group_not_found:
                return None

            lower_bound = np.amin(range)  # The lower bound
            upper_bound = np.amax(range)  # The upper bound
            mid = range[1]  # The mid value

            df_merge.loc[index, surftype_group] = surface_group  # Write the surface group name in '_surf_group'
            df_merge.loc[index, surftype_cat] = paved_group

            if kemantapan_type == 'IRI':  # If the kemantapan type is ROUGHNESS

                # Start the grading process
                if row[grading_col] <= lower_bound:
                    grade = 'good'
                    level = 1
                if (row[grading_col] > lower_bound) & (row[grading_col] <= mid):
                    grade = 'fair'
                    level = 2
                if (row[grading_col] > mid) & (row[grading_col] <= upper_bound):
                    grade = 'poor'
                    level = 3
                if row[grading_col] > upper_bound:
                    grade = 'bad'
                    level = 4

                df_merge.loc[index, grading_result] = grade
                df_merge.loc[index, grading_level] = level

            elif kemantapan_type == 'PCI':  # If the kemantapan type is PCI
                continue

            else:
                continue

        return df_merge

    def grading(self, surftype_col, grading_col, grading_result='_grade', grading_level='_grade_level',
                    surftype_group='_surf_type', surftype_cat='_surf_group', match_only=True):

        group_details_df = self.group_details_df
        group_details_df.rename(columns={'index': surftype_group, 'category': surftype_cat}, inplace=True)

        if match_only:  # Only grade route with 100% match with RNI.
            self.graded_df = self.match_only.merge(group_details_df, left_on=surftype_col, right_on='group')
        else:
            self.graded_df = self.merged_df.merge(group_details_df, left_on=surftype_col, right_on='group')

        self.graded_df[grading_result] = pd.Series(index=self.graded_df.index)
        self.graded_df[grading_level] = pd.Series(index=self.graded_df.index)

        if self.type == 'IRI':
            self.graded_df.loc[self.graded_df[grading_col] <= self.graded_df['lower'],
                               [grading_result, grading_level]] = ['good', 1]
            self.graded_df.loc[(self.graded_df[grading_col] <= self.graded_df['mid']) &
                               (self.graded_df[grading_col] > self.graded_df['lower']),
                               [grading_result, grading_level]] = ['fair', 2]
            self.graded_df.loc[(self.graded_df[grading_col] <= self.graded_df['upper']) &
                               (self.graded_df[grading_col] > self.graded_df['mid']),
                               [grading_result, grading_level]] = ['poor', 3]
            self.graded_df.loc[self.graded_df[grading_col] >= self.graded_df['upper'],
                               [grading_result, grading_level]] = ['bad', 4]

        elif self.type == 'PCI':
            self.graded_df.loc[self.graded_df[grading_col] >= self.graded_df['lower'],
                               [grading_result, grading_level]] = ['good', 1]
            self.graded_df.loc[(self.graded_df[grading_col] >= self.graded_df['mid']) &
                               (self.graded_df[grading_col] < self.graded_df['lower']),
                               [grading_result, grading_level]] = ['fair', 2]
            self.graded_df.loc[(self.graded_df[grading_col] >= self.graded_df['upper']) &
                               (self.graded_df[grading_col] < self.graded_df['mid']),
                               [grading_result, grading_level]] = ['poor', 3]
            self.graded_df.loc[self.graded_df[grading_col] <= self.graded_df['upper'],
                               [grading_result, grading_level]] = ['bad', 4]

        return self

    @staticmethod
    def kemantapan_percentage(df_graded, route_col, from_m_col, to_m_col, to_km_factor, grade_result_col='_grade',
                              kemantapan_col='_kemantapan'):
        """
        This function will find the length percentage of every route with 'Mantap' dan 'Tidak Mantap' status.
        :param df_graded: The event DataFrame which already being graded.
        :param grade_result_col: The column which store the grade statue for every segment.
        :param kemantapan_col: The newly added column which store the kemantapan status.
        :return: DataFrame with '_kemantapan' column.
        """
        df_graded.loc[:, '_len'] = pd.Series(df_graded[to_m_col]-df_graded[from_m_col])
        df_graded['_len'] = df_graded['_len']*to_km_factor
        df_graded.loc[df_graded[grade_result_col].isin(['good', 'fair']), kemantapan_col] = 'mantap'
        df_graded.loc[df_graded[grade_result_col].isin(['poor', 'bad']), kemantapan_col] = 'tdk_mantap'

        kemantapan_len = df_graded.groupby(by=[route_col, kemantapan_col]).agg({'_len': 'sum'})
        kemantapan_prcnt = kemantapan_len.groupby(level=0).apply(lambda x: 100*x/float(x.sum())).reset_index()
        kemantapan_prcnt.set_index(kemantapan_col, inplace=True)

        return kemantapan_prcnt

    @staticmethod
    def group_details():
        module_folder = os.path.dirname(os.path.dirname(__file__))
        surftype_json_file = os.path.join(module_folder, 'surftype_group.json')
        with open(surftype_json_file) as group_json:
            group_details = json.load(group_json)  # Load the surface type group JSON

        return group_details

    @staticmethod
    def surface_group_df():
        group_df = pd.DataFrame(Kemantapan.group_details()).transpose()
        stack = group_df.apply(lambda x: pd.Series(x['group']), axis=1).stack().reset_index(level=1, drop=True)
        stack.name = 'group'
        group_df = group_df.drop('group', axis=1).join(stack)

        return group_df

    def group_details_df(self):
        group_df = pd.DataFrame(self.group_details).transpose()
        stack = group_df.apply(lambda x: pd.Series(x['group']), axis=1).stack().reset_index(level=1, drop=True)
        stack.name = 'group'
        group_df = group_df.drop('group', axis=1).join(stack)

        if self.type == 'IRI':
            group_df['lower'] = group_df.apply(lambda x: x['iri_range'][0], axis=1)
            group_df['mid'] = group_df.apply(lambda x: x['iri_range'][1], axis=1)
            group_df['upper'] = group_df.apply(lambda x: x['iri_range'][2], axis=1)

        elif self.type == 'PCI':
            group_df['lower'] = group_df.apply(lambda x: x['pci_range'][0], axis=1)
            group_df['mid'] = group_df.apply(lambda x: x['pci_range'][1], axis=1)
            group_df['upper'] = group_df.apply(lambda x: x['pci_range'][2], axis=1)

        group_df['group'] = group_df['group'].astype(int)

        return group_df.reset_index()





