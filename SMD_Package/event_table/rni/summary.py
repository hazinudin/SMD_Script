import json
from SMD_Package.FCtoDataFrame import event_fc_to_df
from SMD_Package.load_config import SMDConfigs, Configs
from SMD_Package.TableWriter.GDBTableWriter import gdb_table_writer
from SMD_Package.event_table.kemantapan.kemantapan import Kemantapan
from arcpy import env
import os
import pandas as pd
import numpy as np
from datetime import datetime


class RNISummary(object):
    def __init__(self, output_table, routes, year=None, **kwargs):
        """
        Used for summarizing RNI dataset.
        """
        if SMDConfigs.smd_dir() == '':
            pass
        else:
            os.chdir(SMDConfigs.smd_dir())  # Change directory to SMD root dir.

        smd_config = SMDConfigs()
        db_connection = smd_config.smd_database['instance']
        env.workspace = db_connection

        rni_config = Configs('RNICheck/rni_config_2020.json')
        self.road_type_group_path = "SMD_Package/event_table/rni/roadtype_group.json"

        self.table_name = smd_config.table_names['rni']

        self.routeid_col = smd_config.table_fields['rni']['route_id']
        self.from_m_col = smd_config.table_fields['rni']['from_measure']
        self.to_m_col = smd_config.table_fields['rni']['to_measure']
        self.lane_code_col = smd_config.table_fields['rni']['lane_code']
        self.lane_width = smd_config.table_fields['rni']['lane_width']
        self.road_type_col = smd_config.table_fields['rni']['road_type']
        self.segment_len_col = smd_config.table_fields['rni']['length_col']
        self.surf_type_col = smd_config.table_fields['rni']['surface_type']
        self.route_req = routes
        self.width_range = [4.5, 6, 7, 8, 14]
        self.width_col_pref = "WIDTH_CAT_"
        self.road_type_list = range(1, 26)  # From 1 to 25
        self.road_type_col_pref = "ROAD_TYPE_"
        self.update_date_col = 'UPDATE_DATE'
        self.force_update = False
        self.output_table = output_table

        lrs_table = smd_config.table_names['lrs_network']
        self.lrs_routeid_col = smd_config.table_fields['lrs_network']['route_id']
        self.lrs_sklen_col = smd_config.table_fields['lrs_network']['sk_length']

        if year is None:
            self.year = datetime.now().year
        else:
            self.year = year

        self.__dict__.update(kwargs)  # Update all class attribute.

        # Put all columns variable to a list.
        columns = [self.routeid_col, self.from_m_col, self.to_m_col, self.lane_code_col, self.lane_width,
                   self.road_type_col, self.segment_len_col, self.surf_type_col]
        self.rni_columns = columns

        # Get the evenet DataFrame.
        self.df = event_fc_to_df(self.table_name, columns, self.route_req, self.routeid_col, env.workspace, True)
        self.df[[self.from_m_col, self.to_m_col]] = self.df[[self.from_m_col, self.to_m_col]].astype(int)

        self.status = None
        self.route_selection = self._route_date_selection(self.output_table)  # Create the route selection.

        # Get the LRS SK length DataFrame.
        self.sklen_df = event_fc_to_df(lrs_table, [self.lrs_routeid_col, self.lrs_sklen_col], self.route_selection,
                                       self.lrs_routeid_col, env.workspace).set_index(self.lrs_routeid_col)

    @property
    def roadtype_class_col(self):
        cols = list()
        for r_group in self.road_type_group_df['ROAD_TYPE_GROUP'].tolist():
            column = self.road_type_col_pref + str(r_group)
            cols.append(column)

        return cols

    @property
    def width_class_col(self):
        cols = list()
        for width in range(1, (len(self.width_range) + 2)):
            column = self.width_col_pref + str(width)
            cols.append(column)

        return cols

    def project_to_sklen(self, df):
        df['sum'] = df.sum(axis=1)
        joined = df.join(self.sklen_df[self.lrs_sklen_col])
        joined['factor'] = joined[self.lrs_sklen_col]/joined['sum']
        joined = joined.apply(lambda x:x*x['factor'], axis=1).drop(['factor', 'sum', self.lrs_sklen_col], axis=1)

        return joined

    def rni_route_df(self, route):
        df = event_fc_to_df(self.table_name, self.rni_columns, route, self.routeid_col, env.workspace, True)
        df[[self.from_m_col, self.to_m_col]] = df[[self.from_m_col, self.to_m_col]].astype(int)
        return df

    def _write_to_df(self, df, output_table):
        col_details = dict()
        year_col = 'YEAR'
        df[year_col] = self.year

        for col_name in df.dtypes.to_dict():
            col_details[col_name] = dict()
            col_dtype = df.dtypes[col_name]

            # Translate to GDB data type.
            if col_dtype == 'object':
                gdb_dtype = 'string'
            elif col_dtype == 'float64':
                gdb_dtype = 'double'
            elif col_dtype in ['int64', 'int32']:
                gdb_dtype = 'short'
            else:
                pass

            col_details[col_name]['dtype'] = gdb_dtype

        gdb_table_writer(env.workspace, df, output_table, col_details, replace_key=[self.routeid_col, year_col])

    def _route_date_selection(self, output_table):
        if self.route_req == 'ALL':
            routes = self.route_req
        elif (type(self.route_req) == unicode) or (type(self.route_req) == str):
            routes = [self.route_req]
        elif type(self.route_req) == list:
            routes = self.route_req
        elif self.route_req is None:
            raise ("No Route parameter in the input JSON.")
        else:
            raise ("Route selection is neither list or string.")

        self.status = {_route: "Missing RNI data." for _route in routes}  # Initialize route status.

        req_columns = [self.update_date_col, self.routeid_col]
        source_date = event_fc_to_df(self.table_name, req_columns, routes, self.routeid_col, env.workspace, True,
                                     sql_prefix='MAX ({0})'.format(self.update_date_col),
                                     sql_postfix='GROUP BY ({0})'.format(self.routeid_col))

        source_routes = source_date[self.routeid_col].tolist()  # Get the available route from source table.
        self.status.update({_route: "Updated." for _route in source_routes})  # Update the status attribute.

        if not self.force_update:
            output_date = event_fc_to_df(output_table, req_columns, routes, self.routeid_col, env.workspace, True)
            merged = pd.merge(source_date, output_date, on=self.routeid_col, how='outer', suffixes=('_SOURCE', '_TARGET'))
            selection = merged.loc[(merged['UPDATE_DATE_SOURCE'] > merged['UPDATE_DATE_TARGET']) |
                                   (merged['UPDATE_DATE_TARGET'].isnull())]
            route_selection = selection[self.routeid_col].tolist()
            not_updated = np.setdiff1d(source_routes, route_selection).tolist()  # Routes that will not be updated
            self.status.update({_route: "Not updated." for _route in not_updated})
        else:
            route_selection = source_routes

        return route_selection

    @property
    def road_type_group_df(self):
        with open(self.road_type_group_path) as j_file:
            type_dict = json.load(j_file)

        df = pd.DataFrame.from_dict(type_dict, orient='index').stack().reset_index(level=0)
        df.rename(columns={'level_0': 'ROAD_TYPE_GROUP', 0: 'ROAD_TYPE'}, inplace=True)
        df['ROAD_TYPE'] = df['ROAD_TYPE'].astype(int)

        return df

    @staticmethod
    def surface_group_df():
        module_folder = os.path.dirname(__file__)
        surftype_json_file = os.path.join(module_folder, 'surftype_group.json')
        with open(surftype_json_file) as group_json:
            group_details = json.load(group_json)  # Load the surface type group JSON

        group_df = pd.DataFrame(group_details).transpose()
        stack = group_df.apply(lambda x: pd.Series(x['group']), axis=1).stack().reset_index(level=1, drop=True)
        stack.name = 'group'
        group_df = group_df.drop('group', axis=1).join(stack)
        group_df['group'] = group_df['group'].astype(int)

        return group_df


class WidthSummary(RNISummary):
    def __init__(self, write_to_db=True, project_to_sk=False, **kwargs):
        super(WidthSummary, self).__init__(output_table="SMD.REKAP_LEBAR_RNI", **kwargs)

        routes = self._route_date_selection(self.output_table)

        for route in routes:
            df = self.rni_route_df(route)

            segment_g = df.groupby([self.routeid_col, self.from_m_col, self.to_m_col])
            lane_w_g = segment_g.agg({self.lane_width: 'sum', self.segment_len_col: 'mean'}).reset_index()
            width_cat_col = 'width_cat'
            lane_w_g[('%s' % width_cat_col)] = pd.Series(np.nan)

            first_range = self.width_range[0]
            last_range = self.width_range[len(self.width_range)-1]
            lane_w_g.loc[lane_w_g[self.lane_width] <= first_range, width_cat_col] = '{0}1'.format(self.width_col_pref)
            lane_w_g.loc[lane_w_g[self.lane_width] >= last_range, width_cat_col] = '{0}{1}'.\
                format(self.width_col_pref, len(self.width_range))

            for w_range in self.width_range:
                range_ind = self.width_range.index(w_range)
                if range_ind == 0:
                    continue
                else:
                    prev_range = self.width_range[range_ind-1]
                    lane_w_g.loc[((lane_w_g[self.lane_width] <= w_range) &
                                  (lane_w_g[self.lane_width] > prev_range)),
                                 width_cat_col] = '{0}{1}'.format(self.width_col_pref, range_ind + 1)

            pivot = lane_w_g.pivot_table(self.segment_len_col, index=self.routeid_col, columns=width_cat_col,
                                         aggfunc=np.sum)
            route_g = lane_w_g.groupby(self.routeid_col)[self.lane_width].mean()
            result = pivot.join(route_g)

            if project_to_sk:
                result = self.project_to_sklen(result).reset_index()

            result.reset_index(inplace=True)

            missing_col = np.setdiff1d(self.width_class_col, list(result))
            result[missing_col] = pd.DataFrame(0, columns=missing_col, index=result.index)
            result.fillna(0, inplace=True)

            if write_to_db:
                self._write_to_df(result, self.output_table)


class RoadTypeSummary(RNISummary):
    def __init__(self, write_to_db=True, lkm=False, project_to_sk=False, **kwargs):
        super(RoadTypeSummary, self).__init__(output_table="SMD.REKAP_TIPE_JALAN", **kwargs)

        routes = self._route_date_selection(self.output_table)
        type_group_df = self.road_type_group_df

        for route in routes:
            df = self.rni_route_df(route)

            pivot_roadtype_col = '_road_type'
            df = df.merge(type_group_df, on='ROAD_TYPE')
            df[pivot_roadtype_col] = df['ROAD_TYPE_GROUP'].apply(lambda x: self.road_type_col_pref + str(x))

            if lkm:
                pivot = df.pivot_table(self.segment_len_col, index=[self.routeid_col, self.lane_code_col],
                                       columns=pivot_roadtype_col, aggfunc=np.sum).reset_index()
                pivot = pivot.groupby([self.routeid_col]).sum().reset_index()
            else:
                centerline = df.groupby([self.routeid_col, self.from_m_col, self.to_m_col]).\
                    agg({self.segment_len_col: np.mean,
                         pivot_roadtype_col: (lambda x: x.value_counts().index[0])
                         }).reset_index()
                pivot = centerline.pivot_table(self.segment_len_col, index=[self.routeid_col],
                                               columns=pivot_roadtype_col, aggfunc=np.sum)

                if project_to_sk:
                    pivot = self.project_to_sklen(pivot)

                pivot.reset_index(inplace=True)

            missing_col = np.setdiff1d(self.roadtype_class_col, list(pivot))
            pivot[missing_col] = pd.DataFrame(0, columns=missing_col, index=pivot.index)
            pivot.fillna(0, inplace=True)

            if write_to_db:
                self._write_to_df(pivot, self.output_table)


class SurfaceTypeSummary(RNISummary):
    def __init__(self, write_to_db=True, lkm=False, project_to_sk=False, **kwargs):
        super(SurfaceTypeSummary, self).__init__(output_table="SMD.REKAP_TIPE_PERKERASAN", **kwargs)

        def select_surf_type(series):
            count = series.value_counts()

            if len(count) == 1:  # If there is only single value.
                return count.index[0]
            else:
                first_count = count[0]  # The most common value count.
                same_count = count.loc[count == first_count]  # The same count as the most common.

                if len(same_count) == 1:  # If there is no same count with the most common value.
                    return count.index[0]
                else:
                    same_count.reset_index(name='count', inplace=True)
                    same_count = same_count.merge(surface_order, on='index')
                    min_order = same_count['order'].min()  # The highest available order.
                    highest_ind = same_count.loc[same_count['order'] == min_order].index[0]

                    return highest_ind

        routes = self._route_date_selection(self.output_table)
        pivot_surface_type = '_surface_type'

        surface_g_df = self.surface_group_df().reset_index().rename(columns={'index': pivot_surface_type})
        surface_g_df[pivot_surface_type] = surface_g_df[pivot_surface_type].apply(lambda x: str(x).upper())
        surfaces = surface_g_df[pivot_surface_type].tolist()
        surface_order = surface_g_df.groupby(['_surface_type'])['order'].max().reset_index(name='order')

        for route in routes:
            df = self.rni_route_df(route)

            merged = df.merge(surface_g_df[[pivot_surface_type, 'group']], left_on=self.surf_type_col, right_on='group')

            if lkm:
                pivot = merged.pivot_table(self.segment_len_col, index=[self.routeid_col, self.lane_code_col],
                                           columns=pivot_surface_type, aggfunc=np.sum).reset_index()
                pivot = pivot.groupby(self.routeid_col).sum().reset_index()
            else:
                centerline = merged.groupby([self.routeid_col, self.from_m_col, self.to_m_col]).\
                    agg({self.segment_len_col: np.mean,
                         '_surface_type': (lambda x: select_surf_type(x))
                         }).reset_index()
                pivot = centerline.pivot_table(self.segment_len_col, index=[self.routeid_col],
                                               columns=pivot_surface_type, aggfunc=np.sum)
                if project_to_sk:
                    pivot = self.project_to_sklen(pivot)

                pivot.reset_index(inplace=True)

            missing_col = np.setdiff1d(surfaces, list(pivot))
            pivot[missing_col] = pd.DataFrame(0, index=pivot.index, columns=missing_col)
            pivot.fillna(0, inplace=True)

            if write_to_db:
                self._write_to_df(pivot, self.output_table)

