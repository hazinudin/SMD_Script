from SMD_Package import SMDConfigs, Configs, event_fc_to_df, gdb_table_writer
from SMD_Package.event_table.kemantapan.kemantapan import Kemantapan
from arcpy import env
import os
import pandas as pd
import numpy as np


class RNISummary(object):
    def __init__(self, routes, **kwargs):
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
        self.table_name = smd_config.table_names['rni']

        self.routeid_col = smd_config.table_fields['rni']['route_id']
        self.from_m_col = smd_config.table_fields['rni']['from_measure']
        self.to_m_col = smd_config.table_fields['rni']['to_measure']
        self.lane_code_col = smd_config.table_fields['rni']['lane_code']
        self.lane_width = smd_config.table_fields['rni']['lane_width']
        self.road_type_col = smd_config.table_fields['rni']['road_type']
        self.segment_len_col = smd_config.table_fields['rni']['length_col']
        self.surf_type_col = smd_config.table_fields['rni']['surface_type']
        self.routes = routes
        self.width_range = [4.5, 6, 7, 8, 14]
        self.width_col_pref = "WIDTH_CAT_"
        self.road_type_list = range(1, 26)  # From 1 to 25
        self.road_type_col_pref = "ROAD_TYPE_"

        self.__dict__.update(kwargs)  # Update all class attribute.

        # Put all columns variable to a list.
        columns = [self.routeid_col, self.from_m_col, self.to_m_col, self.lane_code_col, self.lane_width,
                   self.road_type_col, self.segment_len_col, self.surf_type_col]

        self.df = event_fc_to_df(self.table_name, columns, self.routes, self.routeid_col, env.workspace, True)
        self.df[[self.from_m_col, self.to_m_col]] = self.df[[self.from_m_col, self.to_m_col]].astype(int)

    def width_summary(self, write_to_db=True, return_df=True, output_table='SMD.REKAP_LEBAR_RNI'):
        """
        Classify segments based on its surface width, create a DataFrame and write it to a database table.
        """
        df = self.df.copy(deep=True)
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
        result = pivot.join(route_g).reset_index()
        missing_col = np.setdiff1d(self.width_class_col, list(result))
        result[missing_col] = pd.DataFrame(0, columns=missing_col, index=result.index)
        result.fillna(0, inplace=True)

        if write_to_db:
            self._write_to_df(result, output_table)

        if return_df:
            return result
        else:
            return self

    def roadtype_summary(self, write_to_db=True, return_df=True, output_table='SMD.REKAP_TIPE_JALAN'):
        """
        Classification based on the roadtype.
        """
        df = self.df.copy(deep=True)
        pivot_roadtype_col = '_road_type'
        df[pivot_roadtype_col] = df[self.road_type_col].apply(lambda x: self.road_type_col_pref+str(x))
        pivot = df.pivot_table(self.segment_len_col, index=[self.routeid_col, self.lane_code_col],
                               columns=pivot_roadtype_col, aggfunc=np.sum).reset_index()
        pivot_lkm = pivot.groupby([self.routeid_col]).sum().reset_index()
        missing_col = np.setdiff1d(self.roadtype_class_col, list(pivot_lkm))
        pivot_lkm[missing_col] = pd.DataFrame(0, columns=missing_col, index=pivot_lkm.index)
        pivot_lkm.fillna(0, inplace=True)

        if write_to_db:
            self._write_to_df(pivot_lkm, output_table)

        if return_df:
            return pivot_lkm
        else:
            return self

    def surface_summary(self, write_to_db=True, return_df=True, output_table='SMD.REKAP_TIPE_PERKERASAN'):
        """
        Classification based on the surface type.
        """
        df = self.df.copy(deep=True)
        pivot_surface_type = '_surface_type'

        surface_g_df = Kemantapan.surface_group_df().reset_index().rename(columns={'index': pivot_surface_type})
        surface_g_df['group'] = surface_g_df['group'].astype(int)  # Convert to integer.
        surface_g_df[pivot_surface_type] = surface_g_df[pivot_surface_type].apply(lambda x: str(x).upper())
        surfaces = surface_g_df[pivot_surface_type].tolist()

        merged = df.merge(surface_g_df[[pivot_surface_type, 'group']], left_on=self.surf_type_col, right_on='group')
        pivot = merged.pivot_table(self.segment_len_col, index=[self.routeid_col, self.lane_code_col],
                                   columns=pivot_surface_type, aggfunc=np.sum).reset_index()
        pivot_lkm = pivot.groupby(self.routeid_col).sum().reset_index()
        missing_col = np.setdiff1d(surfaces, list(pivot_lkm))
        pivot_lkm[missing_col] = pd.DataFrame(0, index=pivot_lkm.index, columns=missing_col)
        pivot_lkm.fillna(0, inplace=True)

        if write_to_db:
            self._write_to_df(pivot_lkm, output_table)

        if return_df:
            return pivot_lkm
        else:
            return self

    @property
    def roadtype_class_col(self):
        cols = list()
        for r_type in self.road_type_list:
            column = self.road_type_col_pref + str(r_type)
            cols.append(column)

        return cols

    @property
    def width_class_col(self):
        cols = list()
        for width in range(1, (len(self.width_range) + 2)):
            column = self.width_col_pref + str(width)
            cols.append(column)

        return cols

    def _write_to_df(self, df, output_table):
        col_details = dict()

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

        gdb_table_writer(env.workspace, df, output_table, col_details)
