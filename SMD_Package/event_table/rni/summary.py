from SMD_Package import SMDConfigs, Configs, event_fc_to_df
from arcpy import env
import os
import pandas as pd
import numpy as np


class RNISummary(object):
    def __init__(self, **kwargs):
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
        self.table_name = "SMD.RNI_2020"

        self.routeid_col = rni_config.kwargs['routeid_col']
        self.from_m_col = rni_config.kwargs['from_m_col']
        self.to_m_col = rni_config.kwargs['to_m_col']
        self.lane_code_col = rni_config.kwargs['lane_code']
        self.lane_width = rni_config.kwargs['lane_width_col']
        self.road_type_col = rni_config.kwargs['road_type_col']
        self.segment_len_col = rni_config.kwargs['length_col']
        self.routes = list()
        self.output_table = "SMD.REKAP_LEBAR_RNI"
        self.width_range = [4.5, 6, 7, 8, 14]

        self.__dict__.update(kwargs)  # Update all class attribute.

        # Put all columns variable to a list.
        columns = [self.routeid_col, self.from_m_col, self.to_m_col, self.lane_code_col, self.lane_width,
                   self.road_type_col, self.segment_len_col]

        self.df = event_fc_to_df(self.table_name, columns, self.routes, self.routeid_col, env.workspace, True)
        self.df[[self.from_m_col, self.to_m_col]] = self.df[[self.from_m_col, self.to_m_col]].astype(int)

    def width_summary(self, write_to_db=True, return_df=True):
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
        lane_w_g.loc[lane_w_g[self.lane_width] <= first_range, width_cat_col] = 'WIDTH_CAT_1'
        lane_w_g.loc[lane_w_g[self.lane_width] >= last_range, width_cat_col] = 'WIDTH_CAT_{0}'.format(len(self.width_range))

        for w_range in self.width_range:
            range_ind = self.width_range.index(w_range)
            if range_ind == 0:
                continue
            else:
                prev_range = self.width_range[range_ind-1]
                lane_w_g.loc[((lane_w_g[self.lane_width] <= w_range) &
                              (lane_w_g[self.lane_width] > prev_range)),
                             width_cat_col] = 'WIDTH_CAT_{0}'.format(range_ind + 1)

        pivot = lane_w_g.pivot_table(self.segment_len_col, index=self.routeid_col, columns=width_cat_col,
                                     aggfunc=np.sum)
        route_g = lane_w_g.groupby(self.routeid_col)[self.lane_width].mean()
        result = pivot.join(route_g).reset_index()

        if return_df:
            return result
        else:
            return self
