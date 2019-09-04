"""
This script provide the function and class used by coordinate check class method in the EventValidation Class.
"""
from arcpy import Point, PointGeometry

class InputPoint(object):
    def __init__(self, x, y, projection='4326'):
        """

        :param x:
        :param y:
        :param projection:
        """
        point_obj = Point(x, y)
        self.point_geom = PointGeometry(point_obj).projectAs(projection)  # The input point geometry

    def distance_to_segment(self, from_m, to_m, lane, route_geom, segm_start=False, to_meter_conversion=10):
        """

        :param from_m:
        :param to_m:
        :param lane:
        :param route_geom:
        :param segm_start:
        :param to_meter_conversion:
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

        :param route_geom:
        :return:
        """
        dist_to_centerline = self.point_geom.distanceTo(route_geom)
        return dist_to_centerline

    def point_meas_on_route(self, route_geom):
        """

        :param route_geom:
        :return:
        """
        point_meas = route_geom.measureOnLine(self.point_geom)
        return point_meas
