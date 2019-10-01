from arcpy import env
from SMD_Package.event_table.checks.coordinate import FindCoordinateError, distance_series
from SMD_Package.event_table.lrs import route_geometry
from SMD_Package.load_config import Configs
from flip import flip_measurement
from trim_convert import convert_and_trim


class Adjust(object):
    def __init__(self, df, routeid_col, from_m_col, to_m_col, lane_code):
        self.df = df
        self.routeid = routeid_col
        self.from_m = from_m_col
        self.to_m = to_m_col
        self.lane_code = lane_code
        self.conversion = 100
        self.flipped = list()  # List flipped routes

        config = Configs()
        self.lrs_network = config.table_names['lrs_network']
        self.lrs_routeid = config.table_fields['lrs_network']['route_id']
        workspace = config.smd_database['instance']
        env.workspace = workspace

    def survey_direction(self, lat_col, long_col, segment_len):
        dist_column = ['segDistance', 'rniDistance', 'lrsDistance', 'measureOnLine']

        for route in self.df[self.routeid].unique().tolist():
            df_route = self.df.loc[self.df[self.routeid] == route]
            route_geom = route_geometry(route, self.lrs_network, self.lrs_routeid)
            df_route[dist_column] = df_route.apply(lambda x: distance_series(x[lat_col],
                                                                             x[long_col],
                                                                             route_geom,
                                                                             from_m=x[self.from_m],
                                                                             to_m=x[self.to_m],
                                                                             lane=x[self.lane_code]), axis=1)

            check = FindCoordinateError(df_route, self.from_m, self.to_m, self.lane_code)
            errors = check.find_non_monotonic('measureOnLine', route)

            if errors:
                self.flipped.append(route)
                result = flip_measurement(df_route, self.from_m, self.to_m, segment_len)
                self.df.loc[self.df[self.routeid] == route] = result

        return self

    def trim_to_reference(self, fit_to='LRS'):
        convert_and_trim(self.df, self.routeid, self.from_m, self.to_m, self.lane_code, conversion=self.conversion,
                         fit_to=fit_to)
        return self
