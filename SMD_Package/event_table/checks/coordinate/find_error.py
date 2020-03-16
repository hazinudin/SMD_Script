import numpy as np
from arcpy import PointGeometry
from pandas import DataFrame

from SMD_Package.event_table.checks.coordinate.coordinate import InputPoint


class FindCoordinateError(object):
    """
    This class is used to process the input point distance to a specified point of reference
    and also point's measurement pattern.
    """
    def __init__(self, data_frame, route, from_m_col, to_m_col, lane_code_col, routeid_col='LINKID',
                 long_col='STATO_LONG', lat_col='STATO_LAT'):
        """
        Initialization.
        :param data_frame: The input DataFrame with distance column.
        :param route: The selected route.
        :param from_m_col: The from measure column in the input DataFrame.
        :param to_m_col: The to measure column in the input DataFrame.
        :param lane_code_col: The lane code column in the input DataFrame.
        :param routeid_col: The Route ID column in the input DataFrame.
        :param long_col: The longitude column.
        :param lat_col: The latitude column.
        """
        self.df = data_frame
        self.route = route
        self.routeid = routeid_col
        self.from_m_col = from_m_col
        self.to_m_col = to_m_col
        self.lane_code_col = lane_code_col
        self.long_col = long_col
        self.lat_col = lat_col
        self.error_msg = list()

    def distance_double_check(self, column1, column2, window=5, threshold=30):
        """
        This class method finds distance error on two specified columns within the specified threshold. If the error
        exist in both of the specified distance column, then an error message will be written.
        :param column1: The first distance column.
        :param column2:  The second distance column.
        :param window:  The error window.
        :param threshold:  The distance threshold in meters.
        :return:
        """
        col1_error = self.find_distance_error(column1, window=window, threshold=threshold, write_message=False)
        col2_error = self.find_distance_error(column2, window=window, threshold=threshold, write_message=False)

        for lane in col1_error.keys():
            if lane in col2_error:
                runs1 = col1_error[lane]
                runs2_df = DataFrame(col2_error[lane], columns=['from', 'to', 'distance'])

                for run in runs1:
                    run_index = runs1.index(run)
                    start = run[0]
                    end = run[1]

                    no_match_in_2 = runs2_df.loc[(runs2_df['from'] <= start) & (runs2_df['to'] >= end)].empty

                    if no_match_in_2:  # If there is no overlay then pop the current runs
                        col1_error[lane].pop(run_index)
                    else:
                        msg = "Rute {0} pada lane {1} dari {2}-{3} memiliki koordinat yang melebihi batas {4}m {5}.".\
                            format(self.route, lane, start, end, threshold, run[2])
                        self.error_msg.append(msg)

        return col1_error

    def find_distance_error(self, distance_column, window=5, threshold=30, write_message=True):
        """
        This class method find error related to distance error.
        :param distance_column: The distance from input point to a reference point.
        :param window: The minimal window for error detection.
        :param threshold: The distance threshold for error detection.
        :param write_message: If True then an error message will be written to error_msg class attribute list.
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
                range_start = index_range[0]  # The runs starting index
                range_end = index_range[1]  # The runs ends
                meas_start = df_lane.at[range_start, self.from_m_col]  # Runs start measurement
                meas_end = df_lane.at[range_end, self.to_m_col]  # Runs end measurement
                distance_list = df_lane.loc[range_start:range_end, distance_column].tolist()  # All the error distance

                if write_message:
                    msg = "Rute {0} pada lane {1} dari {2}-{3} memiliki koordinat yang melebihi batas {4}m {5}.".\
                        format(self.route, lane, meas_start, meas_end, threshold, distance_list)
                    self.error_msg.append(msg)  # Append the error message

                if lane not in errors.keys():  # If the lane does not exist in the errors dictionary
                    errors[lane] = list()

                errors[lane].append([meas_start, meas_end, distance_list])  # Append the value

        return errors

    def find_non_monotonic(self, measure_column):
        """
        This class method find any error related to measurement value pattern.
        :param measure_column: The column which contain the segment measurement value.
        :return:
        """
        lanes = self.df[self.lane_code_col].unique().tolist()
        for lane in lanes:
            df_lane = self.df.loc[self.df[self.lane_code_col] == lane]  # Create a DataFrame for every available lane
            df_lane.sort_values(by=[self.from_m_col, self.to_m_col], inplace=True)  # Sort the DataFrame
            monotonic_check = np.diff(df_lane[measure_column]) >= 0
            check_unique = np.unique(monotonic_check)

            if check_unique.all():  # Check whether the result only contain True
                pass  # This means OK
            elif len(check_unique) == 1:  # Else if only contain one value, then the result is entirely False
                error_message = 'Data koordinat di lajur {0} pada rute {1} tidak sesuai dengan arah geometri ruas.'.\
                    format(lane, self.route)
                self.error_msg.append(error_message)

        return self

    def find_end_error(self, ref_polyline, end_type, ref_distance='lrsDistance', long_col='STATO_LONG',
                       lat_col='STATO_LAT', threshold=30):
        """
        This class method find error for start or end point.
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
        elif end_type == 'start':  # Get the 'end point' based on end_type
            ref_geom = PointGeometry(ref_polyline.firstPoint).projectAs(ref_polyline.spatialReference)
            start_ind = np.min(groups.keys())
            rads = 100  # Radius threshold for start is rads +- threshold
        else:
            ref_geom = PointGeometry(ref_polyline.lastPoint).projectAs(ref_polyline.spatialReference)
            start_ind = np.max(groups.keys())
            rads = 0  # Actually not used

        first_ind_rows = self.df.loc[groups[start_ind]]  # Rows of the last/start segment

        for index, row in first_ind_rows.iterrows():  # Iterate all row in last/start segment
            lane = row[self.lane_code_col]
            ref_dist = row[ref_distance]  # Distance to nearest point in referenced line
            input_p = InputPoint(row[long_col], row[lat_col])  # The input point
            dist_to_end = input_p.point_geom.projectAs(ref_geom.spatialReference).distanceTo(ref_geom)  # End point dist

            if end_type == 'start':
                if (dist_to_end < (rads-threshold)) and \
                   (dist_to_end > (rads+threshold)) and \
                   (ref_dist > threshold):
                    msg = "Koordinat awal pada rute {0} di lane {1} berjarak lebih dari {2}m dari titik koordinat awal data referensi. (start_dist = {2}, line_dist = {3})".\
                        format(self.route, lane, dist_to_end, ref_dist)
                    self.error_msg.append(msg)

            else:
                if (dist_to_end > threshold) and (ref_dist > threshold):
                    msg = "Koordinat akhir pada rute {0} di lane {1} berjarak lebih dari {2}m dari titik koordinat akhir data referensi. (end_dist = {2}, line_dist = {3})".\
                        format(self.route, lane, dist_to_end, ref_dist)
                    self.error_msg.append(msg)

        return self  # Return all error message

    def find_lane_error(self, rni_df=None, lane_w_col='LANE_WIDTH', default_width=3.6, ref='L1',
                        m_col='measureOnLine', m_threshold=30):
        """
        This class method find distance between every lane coordinate in a segment to a referenced lane coordinate. If
        any lane coordinate distance to reference is greater than the threshold then an error message will be raised.
        :param rni_df: RNI DataFrame for calculating lane width.
        :param lane_w_col: The lane width column in the RNI DataFrame.
        :param default_width: Default width for every lane if the RNI DataFrame is not specified.
        Distance error threshold in meters.
        :param ref: Lane coordinate used as reference.
        :param m_col: The measurement value column.
        :param m_threshold: The threshold for M-Value difference.
        :return:
        """
        grouped = self.df.groupby([self.from_m_col, self.to_m_col])

        if rni_df is not None:
            rni_lane_w = rni_df.groupby([self.from_m_col, self.to_m_col])[lane_w_col].sum()
        else:
            rni_lane_w = None

        groups = grouped.groups

        for group in groups.keys():  # Iterate over all available group
            group_rows = self.df.loc[groups[group]]  # All row from a group
            ref_row = group_rows[self.lane_code_col] == ref  # Referenced lane row
            ref_missing = ~np.any(ref_row)  # Referenced lane is missing
            other_row = group_rows.loc[~ref_row]  # All row from other lane (not referenced lane)

            if (rni_lane_w is None) or (group not in rni_lane_w.index.tolist()):
                width = len(group_rows)*default_width  # Determine the surface width
            else:
                width = rni_lane_w[group]

            if (len(other_row) == 0) or ref_missing:
                continue

            other_dist = dict()  # Dictionary for every other lane distance to referenced lane
            other_meas_diff = dict()

            ref_x = group_rows.loc[ref_row, self.long_col].values[0]  # Referenced lane coordinate
            ref_y = group_rows.loc[ref_row, self.lat_col].values[0]
            ref_p = InputPoint(ref_x, ref_y)
            ref_m = group_rows.loc[ref_row, m_col].values[0]

            for index, row in other_row.iterrows():  # Iterate over all row from other lane
                lane = row[self.lane_code_col]  # Other lane coordinates
                other_x = row[self.long_col]
                other_y = row[self.lat_col]
                other_m = row[m_col]
                distance = ref_p.distance_to_point(other_x, other_y)  # Distance to referenced lane
                m_diff = abs(ref_m-other_m)  # Calculate M-Value difference from reference.
                other_dist[lane] = distance  # Insert to other distance dictionary
                other_meas_diff[lane] = m_diff

            # If any lane exceeds threshold
            if (np.any(np.array([other_dist[x] for x in other_dist]) > width)) or \
               (np.any(np.array([other_meas_diff[x] for x in other_meas_diff]) > m_threshold)):
                msg = "Rute {0} pada segmen {1}-{2} memiliki koordinat dengan jarak lebih dari lebar ruas ({3}m) terhadap {4} atau memiliki selisih nilai pengukuran yang melebihi ({5}m) terhadap {4} yaitu (Jarak = {6}, selisih M-Value = {7})".\
                    format(self.route, group[0], group[1], width, ref, m_threshold, other_dist, other_meas_diff)
                self.error_msg.append(msg)

        return self

    def close_to_zero(self, distance_col, tolerance=0.3, percentage=0.2):
        """
        This class method finds any coordinate which has a distance of 0 within tolerance, and if the amount of
        coordinate which fulfill the first condition exceeds 80% of total row count of the input DataFrame then an
        error message will be raised.
        :param distance_col: The distance column which the value will be evaluated.
        :param tolerance: The distance threshold.
        :param percentage: The minimum percentage of error rows.
        :return:
        """
        total = len(self.df)
        error_row = self.df.loc[np.isclose(self.df[distance_col], 0, atol=tolerance)]
        error_count = len(error_row)
        err_percentage = float(error_count)/float(total)

        if err_percentage > percentage:
            error_msg = "Rute {0} memiliki {1}% koordinat survey yang berjarak kurang dari {2}m.".\
                format(self.route, percentage*100, tolerance)
            self.error_msg.append(error_msg)

        return self


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
