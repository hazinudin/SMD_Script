"""
Ths script accept return RNI details from a specified route in requested by the user.
"""
from arcpy import GetParameterAsText
import json
import sys
import os
from SMD_Package import input_json_check, event_fc_to_df, SMDConfigs, GetRoutes
import multiprocessing as mp
from pandas import concat

if SMDConfigs.smd_dir() == '':
    pass
else:
    os.chdir(SMDConfigs.smd_dir())

# Load the SMD config JSON file
smd_config = SMDConfigs()

# Get the input JSON from user
inputJSON = GetParameterAsText(0)

# Load the input JSON
input_j = input_json_check(inputJSON, 1, req_keys=["prov", "year"])
prov_req = input_j["prov"]
data_year = input_j["year"]

# Get the route list
route_req = GetRoutes('no_prov', str(prov_req)).route_list()

# The SMD config JSON detail
rni_table = smd_config.table_names["rni"]
routeid_col = smd_config.table_fields["rni"]["route_id"]
from_m = smd_config.table_fields["rni"]["from_measure"]
to_m = smd_config.table_fields["rni"]["to_measure"]
medwidth = smd_config.table_fields["rni"]["median"]
road_type = smd_config.table_fields["rni"]["road_type"]
lane_code = smd_config.table_fields["rni"]["lane_code"]
db_connection = smd_config.smd_database["instance"]


if __name__ == '__main__':
    # Process the request
    grp_arg = [[rni_table, [routeid_col, lane_code, from_m, to_m, road_type, medwidth], route, routeid_col,
                db_connection, True] for route in route_req]
    result_list = list()


    def callback(results):
        result_list.append(results.to_dict())

    pool = mp.Pool(processes=(mp.cpu_count()-1))
    for arg in grp_arg:
        pool.apply_async(event_fc_to_df, args=arg, callback=callback)
    pool.close()
    pool.join()

    target_df = concat(result_list)
    print len(target_df)

    # rni_df = event_fc_to_df(rni_table, [routeid_col, lane_code,
    #                                     from_m, to_m, road_type, medwidth],
    #                         route_req, routeid_col, db_connection, is_table=True)
