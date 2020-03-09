import numpy as np
from arcpy import PointGeometry
from pandas import DataFrame

from SMD_Package.event_table.checks.coordinate.coordinate import InputPoint


class FindCoordinateError(object):
    """
    This class is used to process the input point distance to a specified point of reference
    and also point's measurement pattern.
    """
    def __init__(self, data_frame, from_m_col, to_m_col, lane_code_col, long_col='STATO_LONG', lat_col='STATO_LAT'):
        """
        Initialization.
        :param data_frame: The input DataFrame with distance column.
        :param from_m_col: The from measure column in the input DataFrame.
        :param to_m_col: The to measure column in the input DataFrame.
        :param lane_code_col: The lane code column in the input DataFrame.
        :param long_col: The longitude column.
        :param lat_col: The latitude column.
        """
        self.df = data_frame
        self.from_m_col = from_m_col
        self.to_m_col = to_m_col
        self.lane_code_col = lane_code_col
        self.long_col = long_col
        self.lat_col = lat_col

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
        This class method find error for start or end point.
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
        elif end_type == 'start':  # Get the 'end point' based on end_type
            ref_geom = PointGeometry(ref_polyline.firstPoint).projectAs(ref_polyline.spatialReference)
            start_ind = np.min(groups.keys())
            rads = 100  # Radius threshold for start is rads +- threshold
        else:
            ref_geom = PointGeometry(ref_polyline.lastPoint).projectAs(ref_polyline.spatialReference)
            start_ind = np.max(groups.keys())
            rads = 0  # Actually not used

        first_ind_rows = self.df.loc[groups[start_ind]]  # Rows of the last/start segment
        error_messages = list()  # List for storing error messages

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
                        format(route, lane, dist_to_end, ref_dist)
                    error_messages.append(msg)

            else:
                if (dist_to_end > threshold) and (ref_dist > threshold):
                    msg = "Koordinat akhir pada rute {0} di lane {1} berjarak lebih dari {2}m dari titik koordinat akhir data referensi. (end_dist = {2}, line_dist = {3})".\
                        format(route, lane, dist_to_end, ref_dist)
                    error_messages.append(msg)

        return error_messages  # Return all error message

    def find_lane_error(self, route, threshold=30, ref='L1'):
        """
        This class method find distance between every lane coordinate in a segment to a referenced lane coordinate. If
        any lane coordinate distance to reference is greater than the threshold then an error message will be raised.
        :param threshold: Distance error threshold in meters.
        :param ref: Lane coordinate used as reference.
        :return:
        """
        grouped = self.df.groupby([self.from_m_col, self.to_m_col])
        groups = grouped.groups
        error_messages = list()

        for group in groups.keys():
            group_rows = self.df.loc[groups[group]]
            ref_row = group_rows[self.lane_code_col] == ref
            ref_missing = np.any(ref_row)
            other_row = group_rows.loc[~ref_row]

            if (len(other_row) == 0) or ref_missing:
                continue

            other_dist = dict()

            ref_x = group_rows.loc[ref_row, self.long_col].values[0]
            ref_y = group_rows.loc[ref_row, self.lat_col].values[0]
            ref_p = InputPoint(ref_x, ref_y)

            for index, row in other_row.iterrows():
                lane = row[self.lane_code_col]
                other_x = row[self.long_col]
                other_y = row[self.lat_col]
                distance = ref_p.distance_to_point(other_x, other_y)
                other_dist[lane] = distance

            if np.any(np.array([other_dist[x] for x in other_dist]) > threshold):
                msg = "Rute {0} pada segmen {1}-{2} memiliki kelompok koordinat lane yang berjarak lebih dari {3}m dari {4}. {5}".\
                    format(route, group[0], group[1], threshold, ref, other_dist)
                error_messages.append(msg)

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