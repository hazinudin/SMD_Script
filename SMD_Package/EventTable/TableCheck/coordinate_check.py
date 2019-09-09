"""
This script provide the function and class used by coordinate check class method in the EventValidation Class.
"""
from arcpy import Point, PointGeometry
import numpy as np


class InputPoint(object):
    def __init__(self, x, y, projection='4326'):
        """
        This class used to process input point coordinates.
        :param x: Input longitude.
        :param y: Input latitude.
        :param projection: The projection used by the input coordinates.
        """
        point_obj = Point(x, y)
        self.point_geom = PointGeometry(point_obj).projectAs(projection)  # The input point geometry

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
            distance_to_ref = self.point_geom.distanceTo(segment_endpoint)  # Distance from input point to endpoint

        return distance_to_ref

    def distance_to_centerline(self, route_geom):
        """
        This method calculate the distance from input point to a nearest point in LRS route geometry.
        :param route_geom: The LRS route geometry. Polyline object geometry.
        :return:
        """
        dist_to_center_line = self.point_geom.distanceTo(route_geom)
        return dist_to_center_line

    def point_meas_on_route(self, route_geom):
        """
        The measurement value of point geometry on the LRS route geometry.
        :param route_geom: The LRS route geometry. Polyline object geometry.
        :return:
        """
        point_meas = route_geom.measureOnLine(self.point_geom)
        return point_meas


class PointProperties(object):
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

    def find_distance_error(self, distance_column, window=5, threshold=30):
        """
        This class method find error related to distance error.
        :param distance_column: The distance from input point to a reference point.
        :param window: The minimal window for error detection.
        :param threshold: The distance threshold for error detection.
        :return: If there is no error detected then None will be returned, otherwise a list object will be returned
        """
        distance_array = self.df[distance_column].to_numpy()  # The column distance in array
        ranges = _find_error_runs(distance_array, window, threshold)

        return ranges

    def find_non_monotonic(self, measure_column):
        """
        This class method find any error related to measurement value pattern.
        :param measure_column:
        :return:
        """
        pass


def _find_error_runs(array, window, threshold):
    above_threshold = np.concatenate(([0], (array > threshold).view(np.int8), [0]))  # The array with padding
    abs_diff = np.abs(np.diff(above_threshold))
    ranges = np.where(abs_diff == 1)[0].reshape(-1, 2)
    list_ranges = list()

    for range_window in ranges:  # Iterate over all available range
        range_len = range_window[1]-range_window[0]
        if range_len >= window:
            list_ranges.append(range_window.tolist())

    if len(list_ranges) == 0:  # Check if the list ranges is empty.
        return None
    else:
        return ranges
