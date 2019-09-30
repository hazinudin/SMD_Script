from arcpy import da


def route_geometry(route, lrs_network, lrs_routeid):
    """
    This static method return a Polyline object geometry from the LRS Network.
    :param route: The requested route.
    :param lrs_network: LRS Network feature class.
    :param lrs_routeid: The LRS Network feature class RouteID column.
    :return: Arcpy Polyline geometry object if the requested route exist in the LRS Network, if the requested route
    does not exist in the LRS Network then the function will return None.
    """
    where_statement = "{0}='{1}'".format(lrs_routeid, route)  # The where statement for SearchCursor
    route_exist = False  # Variable for determining if the requested route exist in LRS Network

    with da.SearchCursor(lrs_network, "SHAPE@", where_clause=where_statement) as cursor:
        for row in cursor:
            route_exist = True
            route_geom = row[0]  # The Polyline geometry object

    if route_exist:
        return route_geom
    if not route_exist:
        return None