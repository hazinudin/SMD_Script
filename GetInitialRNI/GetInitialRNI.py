"""
Ths script accept return RNI details from a specified route in requested by the user.
"""
from arcpy import GetParameterAsText, SetParameterAsText
# import json
import os
import sys
from SMD_Package import input_json_check, event_fc_to_df, SMDConfigs, GetRoutes, Configs
# import multiprocessing as mp
# from pandas import concat
import time

if SMDConfigs.smd_dir() == '':
    pass
else:
    os.chdir(SMDConfigs.smd_dir())

# Load the SMD config JSON file
smd_config = SMDConfigs()
rni_config = Configs('RNICheck/rni_config_2020.json')

# Get the input JSON from user
inputJSON = GetParameterAsText(0)

# Load the input JSON
input_j = input_json_check(inputJSON, 1, req_keys=["no_prov", "year"])
prov_req = input_j["no_prov"]
data_year = input_j["year"]

# Get the route list
route_req = GetRoutes('no_prov', str(prov_req)).route_list()
db_connection = smd_config.smd_database["instance"]

# The SMD config JSON detail
if data_year == 2019:
    rni_table = smd_config.table_names["rni"]
    routeid_col = smd_config.table_fields["rni"]["route_id"]
    from_m = smd_config.table_fields["rni"]["from_measure"]
    to_m = smd_config.table_fields["rni"]["to_measure"]
    medwidth = smd_config.table_fields["rni"]["median"]
    road_type = smd_config.table_fields["rni"]["road_type"]
    lane_code = smd_config.table_fields["rni"]["lane_code"]

elif data_year == 2020:
    rni_table = "SMD.RNI_2020"  # TODO: Don't forget to update this table name.
    routeid_col = rni_config.kwargs['routeid_col']
    from_m = rni_config.kwargs['from_m_col']
    to_m = rni_config.kwargs['to_m_col']
    medwidth = rni_config.kwargs['median_col']
    road_type = rni_config.kwargs['road_type_col']
    lane_code = rni_config.kwargs['lane_code']

else:
    SetParameterAsText(1, "Data tahun {0} tidak tersedia.".format(data_year))
    sys.exit(0)

if __name__ == '__main__':
    start = time.time()
    rni_df = event_fc_to_df(rni_table, [routeid_col, lane_code,
                                        from_m, to_m, road_type, medwidth],
                            route_req, routeid_col, db_connection, is_table=True)
    end = time.time()

    if rni_df.empty:
        SetParameterAsText(1, "Data yang diminta tidak tersedia.")
        sys.exit(0)

    rni_df[[from_m, to_m]] = rni_df[[from_m, to_m]].apply(lambda x: x/100, axis=1)
    rni_df[[routeid_col, lane_code]] = rni_df[[routeid_col, lane_code]].astype(str)
    output_j = rni_df.to_dict(orient='records')
    SetParameterAsText(1, output_j)
    print len(rni_df)
    print (end-start)

# if __name__ == '__main__':
#     # Process the request
#     start = time.time()
#     grp_arg = [[rni_table, [routeid_col, lane_code, from_m, to_m, road_type, medwidth], route, routeid_col,
#                 db_connection, True] for route in route_req]
#     result_list = list()
#
#
#     def callback(results):
#         result_list.append(results.to_dict())
#
#     pool = mp.Pool(processes=(mp.cpu_count()-1))
#     for arg in grp_arg:
#         pool.apply_async(event_fc_to_df, args=arg, callback=callback)
#     pool.close()
#     pool.join()
#
#     end = time.time()
#     print (end-start)
