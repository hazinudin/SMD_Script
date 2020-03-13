"""
This script provide the function and class used by coordinate check class method in the EventValidation Class.
"""
from arcpy import Point, PointGeometry, Polyline, Array, SpatialReference
import numpy as np
from pandas import Series


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
            # Distance from input point to endpoint
            distance_to_ref = reprojected_point.angleAndDistanceTo(segment_endpoint)[1]

        return distance_to_ref

    def distance_to_centerline(self, route_geom):
        """
        This method calculate the distance from input point to a nearest point in LRS route geometry.
        :param route_geom: The LRS route geometry. Polyline object geometry.
        :return:
        """
        reprojected_point = self._reproject(route_geom, self.point_geom)  # Re-project the line to match the line
        nearest_p = route_geom.queryPointAndDistance(reprojected_point)[0]  # The nearest Point Geometry
        dist_to_center_line = reprojected_point.angleAndDistanceTo(nearest_p)[1]

        return dist_to_center_line  # Return distance in meters

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

    def distance_to_point(self, x, y, spat_ref='4326'):
        """
        This class method calculate the distance between the input point and other point defined in the parameter.
        :param x: The longitude of other point.
        :param y: The latitude of other point.
        :param spat_ref: Spatial reference used by the other point.
        :return:
        """
        other = self._point_geom(x, y, projection=spat_ref)
        distance = self.point_geom.angleAndDistanceTo(other)[1]

        return distance

    @staticmethod
    def _reproject(reference_geom, point_geom):
        route_spat_ref = reference_geom.spatialReference
        point_geom_projected = point_geom.projectAs(route_spat_ref)

        return point_geom_projected

    @staticmethod
    def _point_geom(x, y, projection='4326'):
        point_obj = Point(x, y)
        return PointGeometry(point_obj).projectAs(projection)  # The input point geometry
