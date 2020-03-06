"""
This script provide the function and class used by coordinate check class method in the EventValidation Class.
"""
from arcpy import Point, PointGeometry, Polyline, Array, SpatialReference
import numpy as np
from pandas import Series, DataFrame


def distance_series(latitude, longitude, route_geom, projections='4326', from_m=None, to_m=None, lane=None,
                    at_start=False, rni_df=None, rni_from_m=None, rni_to_m=None, rni_lane_code=None,
                    rni_lat=None, rni_long=None, rni_polyline=None):
    """
    This function create a series which will be appended to a Pandas DataFrame.
    :param latitude: The latitude value.
    :param longitude: The longitude value.
    :param route_geom: The route geometry.
    :param projections: The point projections.
    :param from_m: From measurement value.
    :param to_m: To measurement value.
    :param lane: The lane code.
    :param at_start: If true then the reference point is the starting point of a segment. Otherwise, the end point will
    be used as a reference point.
    :param rni_df: RNI DataFrame
    :param rni_from_m: RNI from measure column
    :param rni_to_m: RNI to measure column
    :param rni_lane_code: RNI lane code
    :param rni_lat: RNI latitude column
    :param rni_long: RNI longitude column
    :param rni_polyline: RNI data as polyline.
    :return: Pandas Series.
    """
    input_point = InputPoint(longitude, latitude, projections)  # Initialized InputPoint class
    lrs_distance = input_point.distance_to_centerline(route_geom)
    meas_value = np.nan
    segment_distance = np.nan
    rni_distance = np.nan

    if (from_m is not None) or (to_m is not None) or (lane is not None):  # If the measurement column is not available
        segment_distance = input_point.distance_to_segment(from_m, to_m, lane, route_geom, segm_start=at_start)
        meas_value = input_point.point_meas_on_route(route_geom)

        if (rni_df is not None) and (rni_polyline is None):  # Comparison to RNI segment coordinate
            rni_distance = input_point.distance_to_rni(from_m, to_m, lane, rni_df, rni_from_m, rni_to_m, rni_lane_code,
                                                       rni_lat, rni_long)

    elif rni_df is not None:  # Where the measurement column from the input table is not defined.
        rni_point = InputPoint(rni_df.at[0, rni_long], rni_df.at[0, rni_lat])
        rni_distance = input_point.point_geom.distanceTo(rni_point.point_geom)

    if rni_polyline is not None:  # Comparison to RNI as a polyline
        rni_distance = input_point.distance_to_centerline(rni_polyline)
        meas_value = input_point.point_meas_on_route(rni_polyline)

    return Series([segment_distance, rni_distance, lrs_distance, meas_value])


def to_polyline(dataframe, sorting_col, long_col, lat_col, to_m_col, projections='4326'):
    if dataframe.empty:
        return None
    else:
        dataframe.sort_values(by=sorting_col, inplace=True)
        dataframe['Point'] = dataframe.apply(lambda x: Point(x[long_col], x[lat_col], M=x[to_m_col]), axis=1)
        coord_array = dataframe['Point'].values.tolist()
        arcpy_ar = Array(coord_array)
        spat_ref = SpatialReference(int(projections))

        line = Polyline(arcpy_ar, spat_ref, False, True)  # Construct the polyline

        return line


class InputPoint(object):
    def __init__(self, x, y, projection='4326'):
        """
        This class used to process input point coordinates.
        :param x: Input longitude.
        :param y: Input latitude.
        :param projection: The projection used by the input coordinates.
        """
        self.point_geom = self._point_geom(x, y, projection=projection)

    def distance_to_segment(self, from_m, to_m, lane, route_geom, segm_start=False, to_meter_conversion=10):
        """
        This method calculate the distance from input point to specified segment in the LRS network.
        :param from_m: From measure value.
        :param to_m: To measure value.
        :param lane: The lane where the point lies.
        :param route_geom: The LRS route geometry. Polyline object geometry
        :param segm_start: If True then the segment end point is defined at start, if False then end point is at the end.
        :param to_meter_conversion: The conversion factor to meter.
        :return:
        """
        lane_type = str(lane[0])
        route_max_m = route_geom.lastPoint.M
        to_km_conversion = to_meter_conversion*100

        if segm_start:
            # The starting point of a segment in LRS route geometry
            if lane_type == 'L':
                point_meas = from_m*to_meter_conversion
            elif lane_type == 'R':
                point_meas = to_m*to_meter_conversion
            else:
                return None
        else:
            # The end point of a segment in LRS route geometry
            if lane_type == 'L':
                point_meas = to_m*to_meter_conversion
            elif lane_type == 'R':
                point_meas = from_m*to_meter_conversion
            else:
                return None

        if (point_meas / to_km_conversion) > route_max_m:  # The point measurement exceed LRS max M
            return None
        else:
            segment_endpoint = route_geom.positionAlongLine(point_meas)  # The end point of a segment
            reprojected_point = self._reproject(route_geom, self.point_geom)
            distance_to_ref = reprojected_point.distanceTo(segment_endpoint)  # Distance from input point to endpoint

        return distance_to_ref

    def distance_to_centerline(self, route_geom):
        """
        This method calculate the distance from input point to a nearest point in LRS route geometry.
        :param route_geom: The LRS route geometry. Polyline object geometry.
        :return:
        """
        reprojected_point = self._reproject(route_geom, self.point_geom)
        dist_to_center_line = reprojected_point.distanceTo(route_geom)
        return dist_to_center_line

    def point_meas_on_route(self, route_geom):
        """
        The measurement value of point geometry on the LRS route geometry.
        :param route_geom: The LRS route geometry. Polyline object geometry.
        :return:
        """
        reprojected_point = self._reproject(route_geom, self.point_geom)
        point_meas = route_geom.measureOnLine(reprojected_point)
        return point_meas

    def distance_to_rni(self, from_m, to_m, lane, rni_df, rni_from_m, rni_to_m, rni_lane_code, rni_lat, rni_long):
        """
        This method calculate the input point distance to a specified RNI segment coordinate.
        :param from_m: From measure value of the input point
        :param to_m: To measure value of the input point
        :param lane: Lane of the input point
        :param rni_df: The RNI DataFrame
        :param rni_from_m: The RNI from measure column
        :param rni_to_m: The RNI to measure column
        :param rni_lane_code: The RNI lane code column
        :param rni_lat: The RNI latitude column
        :param rni_long: The RNI longitude column
        :return:
        """
        to_m_condition = rni_df[rni_to_m] == to_m
        from_m_condition = rni_df[rni_from_m] == from_m
        lane_condition = rni_df[rni_lane_code] == lane

        segment = rni_df.loc[from_m_condition & to_m_condition & lane_condition, [rni_long, rni_lat]]

        if len(segment) != 0:  # If the segment does not exist
            segment_coords = segment.values[0]
            segment_x = segment_coords[0]
            segment_y = segment_coords[1]
            segment_point = self._point_geom(segment_x, segment_y)

            return self.point_geom.distanceTo(segment_point)
        else:
            return np.nan

    @staticmethod
    def _reproject(reference_geom, point_geom):
        route_spat_ref = reference_geom.spatialReference
        point_geom_projected = point_geom.projectAs(route_spat_ref)

        return point_geom_projected

    @staticmethod
    def _point_geom(x, y, projection='4326'):
        point_obj = Point(x, y)
        return PointGeometry(point_obj).projectAs(projection)  # The input point geometry


class FindCoordinateError(object):
    """
    This class is used to process the input point distance to a specified point of reference
    and also point's measurement pattern.
    """
    def __init__(self, data_frame, from_m_col, to_m_col, lane_code_col):
        """
        Initialization.
        :param data_frame: The input DataFrame with distance column.
        :param from_m_col: The from measure column in the input DataFrame.
        :param to_m_col: The to measure column in the input DataFrame.
        :param lane_code_col: The lane code column in the input DataFrame.
        """
        self.df = data_frame
        self.from_m_col = from_m_col
        self.to_m_col = to_m_col
        self.lane_code_col = lane_code_col

    def distance_double_check(self, column1, column2, window=5, threshold=30):
        """

        :param column1:
        :param column2:
        :param window:
        :param threshold:
        :return:
        """
        col1_error = self.find_distance_error(column1, window=window, threshold=threshold)
        col2_error = self.find_distance_error(column2, window=window, threshold=threshold)

        for lane in col1_error.keys():
            if lane in col2_error:
                runs1 = col1_error[lane]
                runs2_df = DataFrame(col2_error[lane], columns=['from', 'to'])

                for run in runs1:
                    run_index = runs1.index(run)
                    start = run[0]
                    end = run[1]

                    existin2 = runs2_df.loc[(runs2_df['from'] <= start) and (runs2_df >= end)].empty()

                    if not existin2:  # If there is no overlay then pop the current runs
                        col1_error[lane].pop(run_index)

        return col1_error

    def find_distance_error(self, distance_column, window=5, threshold=30):
        """
        This class method find error related to distance error.
        :param distance_column: The distance from input point to a reference point.
        :param window: The minimal window for error detection.
        :param threshold: The distance threshold for error detection.
        :return: If there is no error detected then None will be returned, otherwise a list object will be returned.
        {lane: [from_m, to_m, [dist, dist, dist,...]}
        """
        lanes = self.df[self.lane_code_col].unique().tolist()
        errors = dict()
        for lane in lanes:
            df_lane = self.df.loc[self.df[self.lane_code_col] == lane]
            df_lane.sort_values(by=[self.from_m_col, self.to_m_col], inplace=True)
            df_lane.reset_index(inplace=True)
            runs = _find_error_runs(df_lane, distance_column, window, threshold)  # Find runs
            index_ranges = _run_to_range(runs)  # Convert run to range

            for index_range in index_ranges:
                range_start = index_range[0]
                range_end = index_range[1]
                meas_start = df_lane.at[range_start, self.from_m_col]
                meas_end = df_lane.at[range_end, self.to_m_col]
                distance_list = df_lane.loc[range_start:range_end, distance_column].tolist()

                if lane not in errors.keys():
                    errors[lane] = list()

                errors[lane].append([meas_start, meas_end, distance_list])  # Append the value

        return errors

    def find_non_monotonic(self, measure_column, route):
        """
        This class method find any error related to measurement value pattern.
        :param measure_column:
        :return:
        """
        lanes = self.df[self.lane_code_col].unique().tolist()
        errors = list()
        for lane in lanes:
            df_lane = self.df.loc[self.df[self.lane_code_col] == lane]  # Create a DataFrame for every available lane
            df_lane.sort_values(by=[self.from_m_col, self.to_m_col], inplace=True)  # Sort the DataFrame
            monotonic_check = np.diff(df_lane[measure_column]) >= 0
            check_unique = np.unique(monotonic_check)

            if check_unique.all():  # Check whether the result only contain True
                pass  # This means OK
            elif len(check_unique) == 1:  # Else if only contain one value, then the result is entirely False
                error_message = 'Data koordinat di lajur {0} pada rute {1} tidak sesuai dengan arah geometri ruas.'.format(lane, route)
                errors.append(error_message)

        return errors

    def find_end_error(self, route, ref_polyline, end_type, ref_distance='lrsDistance', long_col='STATO_LONG',
                       lat_col='STATO_LAT', threshold=30):
        """
        This class method find error for start point.
        :param route which currently being processed.
        :param ref_polyline: The reference data used, should be a Polyline object.
        :param end_type: The end type either 'start' or 'end'.
        :param ref_distance: The reference distance column.
        :param long_col: The longitude column of the input table.
        :param lat_col: The latitude column of the input table.
        :param threshold: The distance threshold in meters.
        :return:
        """
        grouped = self.df.groupby([self.to_m_col])
        groups = grouped.groups

        if end_type not in ['start', 'end']:
            raise ValueError("end_type is not 'start' or 'end'.")
        elif end_type == 'start':
            ref_geom = PointGeometry(ref_polyline.firstPoint).projectAs(ref_polyline.spatialReference)
            start_ind = np.min(groups.keys())
            rads = 100
        else:
            ref_geom = PointGeometry(ref_polyline.lastPoint).projectAs(ref_polyline.spatialReference)
            start_ind = np.max(groups.keys())
            rads = 0

        first_ind_rows = self.df.loc[groups[start_ind]]
        error_messages = list()

        for index, row in first_ind_rows.iterrows():
            lane = row[self.lane_code_col]
            ref_dist = row[ref_distance]
            input_p = InputPoint(row[long_col], row[lat_col])
            distance_to_ref = input_p.point_geom.projectAs(ref_geom.spatialReference).distanceTo(ref_geom)

            if end_type == 'start':
                if (distance_to_ref < (rads-threshold)) and \
                   (distance_to_ref > (rads+threshold)) and \
                   (ref_dist > threshold):
                    msg = "Koordinat awal pada rute {0} di lane {1} berjarak lebih dari {2}m dari titik koordinat awal data referensi. (start_dist = {2}, line_dist = {3})".\
                        format(route, lane, distance_to_ref, ref_dist)
                    error_messages.append(msg)

            else:
                if (distance_to_ref > threshold) and (ref_dist > threshold):
                    msg = "Koordinat akhir pada rute {0} di lane {1} berjarak lebih dari {2}m dari titik koordinat akhir data referensi. (end_dist = {2}, line_dist = {3})".\
                        format(route, lane, distance_to_ref, ref_dist)
                    error_messages.append(msg)

        if len(error_messages) == 0:
            return None
        else:
            return error_messages


def _find_error_runs(df, column, window, threshold):
    runs = list()  # Runs list result
    error_rows = df.loc[df[column] > threshold]
    error_ind = error_rows.index.tolist()  # Find row with value above the threshold
    padded_ind = np.concatenate(([0], error_ind, [0]))  # Add zero at start and end of the array
    ind_diff = np.diff(padded_ind)

    if ind_diff[0] == 1:  # This means there is a run a the beginning
        error_runs_end = list([0])
        error_runs_end = error_runs_end + np.where(ind_diff != 1)[0].tolist()
    else:
        error_runs_end = np.where(ind_diff != 1)[0].tolist()

    runs_count = len(error_runs_end) - 1  # The total count of runs found in the column

    for runs_end in range(0, runs_count):
        start = error_runs_end[runs_end]
        end = error_runs_end[runs_end+1]
        run_index = error_rows.index[start:end].tolist()

        if len(run_index) >= window:  # Check the runs length
            runs.append(run_index)

    return runs


def _run_to_range(runs_list):
    error_ranges = list()

    for run in runs_list:
        run_starts = run[0]
        run_ends = run[len(run) - 1]
        index_range = [run_starts, run_ends]
        error_ranges.append(index_range)

    return error_ranges
