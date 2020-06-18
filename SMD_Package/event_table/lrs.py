from arcpy import da


def route_geometry(route, lrs_network, lrs_routeid, from_date_col='FROMDATE', to_date_col='TODATE'):
    """
    This static method return a Polyline object geometry from the LRS Network.
    :param route: The requested route.
    :param lrs_network: LRS Network feature class.
    :param lrs_routeid: The LRS Network feature class RouteID column.
    :param from_date_col: Table From Date column
    :param to_date_col: Table To Date column
    :return: Arcpy Polyline geometry object if the requested route exist in the LRS Network, if the requested route
    does not exist in the LRS Network then the function will return None.
    """
    where_statement = "{0}='{1}'".format(lrs_routeid, route)  # The where statement for SearchCursor
    route_exist = False  # Variable for determining if the requested route exist in LRS Network
    date_query = "({0} is null or {0}<=CURRENT_TIMESTAMP) and ({1} is null or {1}>CURRENT_TIMESTAMP)". \
        format(from_date_col, to_date_col)

    with da.SearchCursor(lrs_network, "SHAPE@", where_clause=where_statement) as cursor:
        for row in cursor:
            route_exist = True
            route_geom = row[0]  # The Polyline geometry object

    if route_exist:
        return route_geom
    if not route_exist:
        return None