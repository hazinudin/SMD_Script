import numpy as np
from arcpy import PointGeometry
from pandas import DataFrame

from SMD_Package.event_table.checks.coordinate.coordinate import InputPoint
from SMD_Package.event_table.checks.error_runs import find_runs
from SMD_Package.load_config import SMDConfigs


class FindCoordinateError(object):
    """
    This class is used to process the input point distance to a specified point of reference
    and also point's measurement pattern.
    """
    def __init__(self, data_frame, route, from_m_col, to_m_col, lane_code_col, routeid_col='LINKID',
                 long_col='STATO_LONG', lat_col='STATO_LAT', comparison=None):
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
        self.error_msg = list()  # List for all error message string
        self.warning_msg = list()  # List for all warning message string
        self.comparison = comparison
        self.side_col = '_side'

        config = SMDConfigs()
        self.rni_routeid = config.table_fields['rni']['route_id']
        self.rni_from_m = config.table_fields['rni']['from_measure']
        self.rni_to_m = config.table_fields['rni']['to_measure']
        self.rni_lane_width = config.table_fields['rni']['lane_width']

        if self.lane_code_col is not None:
            self.df[self.side_col] = self.df.apply(lambda x: x[lane_code_col][0], axis=1)  # Adding side column

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
                        msg = "Rute {0} pada lane {1} dari {2}-{3} memiliki koordinat yang melebihi batas {4}m {5} (Pembanding={6}).".\
                            format(self.route, lane, start, end, threshold, run[2], self.comparison)
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
            error_rows = df_lane.loc[df_lane[distance_column] > threshold]
            index_ranges = find_runs(error_rows, window)  # Convert run to range

            for index_range in index_ranges:
                range_start = index_range[0]  # The runs starting index
                range_end = index_range[1]  # The runs ends
                meas_start = df_lane.at[range_start, self.from_m_col]  # Runs start measurement
                meas_end = df_lane.at[range_end, self.to_m_col]  # Runs end measurement
                distance_list = df_lane.loc[range_start:range_end, distance_column].tolist()  # All the error distance

                if write_message:
                    msg = "Rute {0} pada lane {1} dari {2}-{3} memiliki koordinat yang melebihi batas {4}m {5}. (Pembanding={6})".\
                        format(self.route, lane, meas_start, meas_end, threshold, distance_list, self.comparison)
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
                error_message = 'Data koordinat di lajur {0} pada rute {1} tidak sesuai dengan arah geometri ruas. Pembanding={2}'.\
                    format(lane, self.route, self.comparison)
                self.error_msg.append(error_message)

        return self

    def find_end_error(self, ref_polyline, end_type, ref_distance='lrsDistance', threshold=30, side='ALL',
                       same_method=True):
        """
        This class method find error for start or end point.
        :param ref_polyline: The reference data used, should be a Polyline object.
        :param end_type: The end type either 'start' or 'end'.
        :param ref_distance: The reference distance column.
        :param long_col: The longitude column of the input table.
        :param lat_col: The latitude column of the input table.
        :param threshold: The distance threshold in meters.
        :param side: Selected side 'L', 'R' or 'ALL', if 'ALL' then both side will be analyzed.
        :param same_method: The method to calculate valid radius is same for both start and end.
        :return:
        """
        long_col = self.long_col  # Get the coordinate column from the class attribute.
        lat_col = self.lat_col

        if side not in ['L', 'R', 'ALL']:
            raise TypeError("{0} is invalid side type".format(side))
        elif side == 'ALL':
            side_selection = self.df  # Use both side
        else:
            side_selection = self.df.loc[self.df[self.side_col] == side]  # Only use the selected side

        if side_selection.empty:  # If the selection is empty
            return self

        grouped = side_selection.groupby([self.to_m_col])
        groups = grouped.groups
        rads = 100  # Radius threshold for start is rads +- threshold

        if end_type not in ['start', 'end']:
            raise TypeError("end_type {0} is not 'start' or 'end'.".format(end_type))
        elif end_type == 'start':  # Get the 'end point' based on end_type
            ref_geom = PointGeometry(ref_polyline.firstPoint).projectAs(ref_polyline.spatialReference)
            start_ind = np.min(groups.keys())
            end_msg = 'awal'
        else:
            ref_geom = PointGeometry(ref_polyline.lastPoint).projectAs(ref_polyline.spatialReference)
            start_ind = np.max(groups.keys())
            end_msg = 'akhir'

        first_ind_rows = self.df.loc[groups[start_ind]]  # Rows of the last/start segment

        for index, row in first_ind_rows.iterrows():  # Iterate all row in last/start segment
            lane = row[self.lane_code_col]
            ref_dist = row[ref_distance]  # Distance to nearest point in referenced line
            input_p = InputPoint(row[long_col], row[lat_col])  # The input point
            dist_to_end = input_p.point_geom.projectAs(ref_geom.spatialReference).angleAndDistanceTo(ref_geom)[1]

            if end_type == 'start' and not same_method:
                if (dist_to_end < (rads-threshold)) or \
                   (dist_to_end > (rads+threshold)) or \
                   (ref_dist > threshold):
                    msg = "Koordinat awal pada rute {0} di lane {1} berjarak lebih dari {2}m atau kurang dari {3}m dari titik koordinat awal data referensi (Jarak ke awal = {4}) atau memiliki jarak lebih dari {7}m dari geometri referensi (Jarak ke referensi = {5}, referensi = {6})".\
                        format(self.route, lane, rads+threshold, rads-threshold, dist_to_end, ref_dist, self.comparison,
                               threshold)
                    self.error_msg.append(msg)

            elif end_type == 'end' or same_method:
                if (dist_to_end > rads+threshold) or (ref_dist > threshold):
                    msg = "Koordinat {6} pada rute {0} di lane {1} berjarak lebih dari {7}m dari titik koordinat {6} data referensi (Jarak koordinat {6} terhadap titik {6} referensi = {3} dan jarak ke geometri referensi = {4}. Referensi yang digunakan adalah {5})".\
                        format(self.route, lane, threshold, dist_to_end, ref_dist, self.comparison, end_msg,
                               rads+threshold)
                    self.error_msg.append(msg)

        return self  # Return all error message

    def find_lane_error(self, rni_df=None, default_width=3.6, left_ref='L1', right_ref='R1',
                        m_col='measureOnLine', m_threshold=30):
        """
        This class method find distance between every lane coordinate in a segment to a referenced lane coordinate. If
        any lane coordinate distance to reference is greater than the threshold then an error message will be raised.
        :param rni_df: RNI DataFrame for calculating lane width.
        :param default_width: Default width for every lane if the RNI DataFrame is not specified.
        Distance error threshold in meters.
        :param left_ref: Lane used as reference for left side.
        :param right_ref = Lane used as reference for right side.
        :param m_col: The measurement value column.
        :param m_threshold: The threshold for M-Value difference.
        :return:
        """
        # Group by using from measure, to measure and side column
        grouped = self.df.groupby([self.from_m_col, self.to_m_col, self.side_col])

        if rni_df is not None:
            rni_lane_w = rni_df.groupby([self.rni_from_m, self.rni_to_m])[self.rni_lane_width].sum()
        else:
            rni_lane_w = None

        groups = grouped.groups

        for group in groups.keys():  # Iterate over all available group
            group_side = group[2]  # The group side whether left or right

            if group_side == 'L':  # Determine the referenced lane from group side
                ref = left_ref
                side_msg = 'kiri'
            else:
                ref = right_ref
                side_msg = 'kanan'

            group_rows = self.df.loc[groups[group]]  # All row from a group
            ref_row = group_rows[self.lane_code_col] == ref  # Referenced lane row
            ref_missing = ~np.any(ref_row)  # Referenced lane is missing
            other_row = group_rows.loc[~ref_row]  # All row from other lane (not referenced lane)

            if (rni_lane_w is None) or (group not in rni_lane_w.index.tolist()):
                width = len(group_rows)*default_width  # Use default value if RNI data is not available
            else:
                width = rni_lane_w[group]  # Get lane width from RNI data

            if (len(other_row) == 0) or ref_missing:
                continue

            other_dist = dict()  # Dictionary for every other lane distance to referenced lane
            other_meas_diff = dict()  # Dictionary for every other lane M-Value difference to ref

            ref_x = group_rows.loc[ref_row, self.long_col].values[0]  # Referenced lane coordinate
            ref_y = group_rows.loc[ref_row, self.lat_col].values[0]
            ref_p = InputPoint(ref_x, ref_y)  # Construct reference Point as InputPoint class
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
            if (np.any(np.array([other_dist[x] for x in other_dist]) > (width + m_threshold))) or \
               (np.any(np.array([other_meas_diff[x] for x in other_meas_diff]) > m_threshold)):
                msg = "Rute {0} pada segmen {1}-{2} lajur {9} memiliki koordinat dengan jarak lebih dari lebar ruas ({3}m) terhadap {4} yaitu({6}) atau memiliki selisih nilai pengukuran yang melebihi ({5}m) terhadap {4} yaitu ({7}, referensi = {8})".\
                    format(self.route, group[0], group[1], width, ref, m_threshold, other_dist, other_meas_diff,
                           self.comparison, side_msg)
                self.error_msg.append(msg)

        return self

    def close_to_zero(self, distance_col, tolerance=0.3, percentage=0.2, as_error=False):
        """
        This class method finds any coordinate which has a distance of 0 within tolerance, and if the amount of
        coordinate which fulfill the first condition exceeds 80% of total row count of the input DataFrame then an
        error message will be raised.
        :param distance_col: The distance column which the value will be evaluated.
        :param tolerance: The distance threshold.
        :param percentage: The minimum percentage of error rows.
        :return:
        """
        total = len(self.df)  # The total row count
        error_row = self.df.loc[np.isclose(self.df[distance_col], 0, atol=tolerance)]  # Error rows
        error_count = len(error_row)  # Error row count
        err_percentage = float(error_count)/float(total)

        if err_percentage > percentage:  # If the error percentage exceed the specified percentage
            msg = "Rute {0} memiliki {1}% koordinat survey yang berjarak kurang dari {2}m. Pembanding = {3}.".\
                format(self.route, err_percentage*100, tolerance, distance_col)

            if as_error:
                self.error_msg.append(msg)
            else:
                self.warning_msg.append(msg)

        return self

    def find_segment_len_error(self, measure_col, tolerance=30, to_meters=10):
        """
        This class method check the consistency between measured M-Value difference between segment and stated segment
        length in the input table, if the M-Value difference between two segment is not the same with the stated
        segment value within the defined tolerance then an error message will be written.
        :param measure_col: The M-Value column of each segment.
        :param tolerance: The error tolerance in meters.
        :param to_meters: Multiplier for converting units to meters.
        :return:
        """
        if self.lane_code_col is not None:
            grouped = self.df.groupby(self.lane_code_col)
        else:
            grouped = self.df.groupby(self.routeid)

        groups = grouped.groups

        for group in groups:
            indexes = groups[group]  # Group index
            group_df = self.df.loc[indexes, [self.routeid, self.from_m_col,
                                             self.to_m_col, self.lane_code_col,
                                             measure_col]]  # Group rows
            group_df.sort_values(self.from_m_col, inplace=True)
            group_df[self.to_m_col] = group_df[self.to_m_col].astype(float)*to_meters

            group_df['_diff'] = group_df[measure_col].diff()
            group_df['_seg_len'] = group_df[self.to_m_col].diff()  # Calculate segment length from to_meas diff.
            group_df.dropna(inplace=True)

            error_rows = group_df.loc[~np.isclose(group_df['_seg_len'], group_df['_diff'], atol=tolerance)]
            for index, row in error_rows.iterrows():
                route = row[self.routeid]
                from_m = row[self.from_m_col]
                to_m = row[self.to_m_col]
                length = row['_seg_len']
                m_diff = row['_diff']

                if self.lane_code_col is not None:
                    lane = row[self.lane_code_col]
                    msg = "Rute {0} pada segmen {1}-{2} lane {3} memiliki nilai panjang segmen ({4}m) yang berbeda dengan jarak koordinat segmen sebelumnya ({5}m).".\
                        format(route, from_m, to_m, lane, length, m_diff)
                    self.error_msg.append(msg)
                else:
                    msg = "Rute {0} pada segmen {1}-{2} memiliki nilai panjang segmen ({3}m) yang berbeda dengan jarak koordinat segmen sebelumnya ({4}m}.".\
                        format(route, from_m, to_m, length, m_diff)
                    self.error_msg.append(msg)

        return self
