from SMD_Package import Configs, event_fc_to_df
from arcpy import env
import numpy as np
from pandas import DataFrame, concat


def rni_to_csv(routes, file_name, outpath=env.scratchFolder):

    smd_config = Configs()
    rni_table = smd_config.table_names['rni']
    rni_route_id = smd_config.table_fields['rni']['route_id']
    rni_from_m = smd_config.table_fields['rni']['from_measure']
    rni_to_m = smd_config.table_fields['rni']['to_measure']
    rni_lane_code = smd_config.table_fields['rni']['lane_code']
    rni_search_col = [rni_route_id, rni_from_m, rni_to_m, rni_lane_code]

    env.workspace = smd_config.smd_database['instance']

    rni_df = event_fc_to_df(rni_table, rni_search_col, routes, rni_route_id, env.workspace, is_table=True)
    missing_route = np.setdiff1d(routes, rni_df[rni_route_id].tolist())
    msg = "Data RNI tidak tersedia"
    missing_df = DataFrame(columns=rni_df.columns)

    for route in missing_route:
        missing_df.loc[len(missing_df)] = [route, msg, msg, msg]

    added = concat([rni_df, missing_df])
    added.to_csv('{0}/{1}'.format(outpath, file_name), index=False)

    return
