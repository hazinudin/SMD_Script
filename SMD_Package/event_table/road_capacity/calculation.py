from SMD_Package import SMDConfigs, event_fc_to_df, RNISummary
import os
import pandas as pd
import json
from arcpy import env


class RoadCapacity(RNISummary):
    def __init__(self, aadt_table="SMD.AADT", veh8_col="NUM_VEH8", **kwargs):
        """
        Class initialization.
        """
        super(RoadCapacity, self).__init__(output_table="SMD.ROAD_CAPACITY", **kwargs)
        if SMDConfigs.smd_dir() == '':
            pass
        else:
            os.chdir(SMDConfigs.smd_dir())

        with open(os.path.dirname(__file__)+'\\'+'coefficient.json') as j_file:  # Read the coefficient JSON file.
            coeff_dict = json.load(j_file)

        self.coefficient_df = pd.DataFrame.from_dict(coeff_dict, orient='index')

        for route in self.route_selection:
            aadt_df = event_fc_to_df(aadt_table, [self.routeid_col, veh8_col], route,
                                     self.routeid_col, env.workspace, True)
            aadt_route = aadt_df[self.routeid_col].unique().tolist()
            rni_df = self.rni_route_df(aadt_route)
            merged = rni_df.merge(aadt_df, on=self.routeid_col)
