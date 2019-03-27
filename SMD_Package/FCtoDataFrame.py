from arcpy import da, env
from pandas import DataFrame


def fc_to_dataframe(gdb_table, search_field, route_selection, route_identifier, sde_connection, orderby='FROMMEASURE'):
    """
    Create a Pandas DataFrame from ArcGIS feature class/table, from a set of route selection.
    :param gdb_table: GeoDataBase table or FeatureClass to be converted as pandas DataFrame.
    :param search_field: The field which will be included in the Pandas DataFrame.
    :param route_selection: The selected route to be included in the Pandas DataFrame.
    :param route_identifier: The RouteID column in the GeoDataBase table or FeatureClass.
    :param orderby: The column which the table will be sorted.
    :param sde_connection: The SDE Instance for accessing the FeatureClass
    :return df = this function will return a Pandas DataFrame
    """
    env.workspace = sde_connection  # The workspace for accessing the SDE Feature Class

    # Create the where_clause for DataAccess module
    if route_selection == 'ALL':  # If the requested route is 'ALL' then there is no where_clause
        where_clause = None
    elif route_selection != 'ALL':
        if type(route_selection) == str:  # If its a string then its only one route
            pass
        elif type(route_selection) == list:  # If its a list then the strip the square bracket
            route_selection = str(route_selection).strip('[]')

        where_clause = "{0} IN ({1})".format(route_identifier, route_selection)

    # Create the sql_clause for DataAccess module
    if orderby is None:
        sql_clause = (None, None)
    elif orderby is not None:
        if type(orderby) == str:
            pass
        elif type(orderby) == list:
            orderby = str(orderby).strip('[]')

        sql_clause = (None, 'ORDER BY {0}'.format(orderby))

    # Create the numpy array of the requested table or feature class from GeoDataBase
    table_search = da.FeatureClassToNumPyArray(gdb_table, search_field, where_clause=where_clause, sql_clause=sql_clause)

    # Creating DataFrame from the numpy array
    df = DataFrame(table_search)

    return df  # Return the DataFrame
