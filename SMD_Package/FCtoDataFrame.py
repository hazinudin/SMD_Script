from arcpy import da, env, ListFields, AddMessage
from pandas import DataFrame
import numpy as np


def event_fc_to_df(gdb_table, search_field, route_selection, route_identifier, sde_connection, is_table=False,
                   include_all=False, sql_prefix=None, sql_postfix=None, add_date_query=False, from_date='FROMDATE',
                   to_date='TODATE', replace_null=True, *args, **kwargs):
    """
    Create a Pandas DataFrame from ArcGIS feature class/table, from a set of route selection.
    :param gdb_table: GeoDataBase table or FeatureClass to be converted as pandas DataFrame.
    :param search_field: The field which will be included in the Pandas DataFrame.
    :param route_selection: The selected route to be included in the Pandas DataFrame.
    :param route_identifier: The RouteID column in the GeoDataBase table or FeatureClass.
    :param sql_prefix: SQL prefix used in the cursor SQL clause.
    :param sql_postfix: SQL postfix used in the cursor SQL clause.
    :param sde_connection: The SDE Instance for accessing the FeatureClass
    :param is_table: If True then the requested table are a table without geometry.
    :param include_all: If True then the method will return all features including features with null geometry, if False
    then the method will only return features with geometry.
    :param orderby: The column used for sorting the feature in the DataFrame.
    :param add_date_query: If True then date query statement will be added.
    :param from_date: The From Date column.
    :param to_date: The To Date column.
    :param replace_null: If True then all Null value will be replaced with -9999
    :return df = this function will return a Pandas DataFrame.
    """
    env.workspace = sde_connection  # The workspace for accessing the SDE Feature Class
    date_value = kwargs.get('date')  # Date value for query

    # Create the where_clause for DataAccess module
    if route_selection == 'ALL':  # If the requested route is 'ALL' then there is no where_clause
        where_clause = None
    else:
        if type(route_selection) == str or type(route_selection) == unicode:  # If its a string then its only one route
            where_clause = "({0} IN ('{1}'))".format(route_identifier, route_selection)
        elif type(route_selection) == list:  # If its a list then the strip the square bracket
            route_selection = str(route_selection).strip('[]')
            where_clause = "({0} IN ({1}))".format(route_identifier, route_selection)

    # Modify the where_clause to prevent null event row with null segment to be included
    if is_table:  # If the inputted is an SDE Table without geometry then include all records
        pass
    elif not include_all and (route_selection != 'ALL'):
        where_clause += "AND (SHAPE.LEN IS NOT NULL)"
    elif not include_all and (route_selection == 'ALL'):
        where_clause = 'SHAPE.LEN IS NOT NULL'

    if add_date_query and (date_value is None):  # If True, then add the date query statement.
        date_query = "AND ({0} is null or {0}<=CURRENT_TIMESTAMP) and ({1} is null or {1}>CURRENT_TIMESTAMP)".\
            format(from_date, to_date)
        where_clause += date_query
    elif add_date_query:
        date_query = "AND ({0} is null or {0}<={2}) and ({1} is null or {1}>{2})".\
            format(from_date, to_date, 'time_stamp ' + str(date_value))
        where_clause += date_query

    # Create the sql_clause for DataAccess module
    sql_clause = (sql_prefix, sql_postfix)

    # Find shape field in the search_field, if exist then pop it
    if search_field == '*':
        lf = ListFields(gdb_table)
        search_field = [x.name for x in lf]

    if type(search_field) == list:
        for field in search_field:
            if 'shape' in field.lower():
                search_field.remove(field)

    if replace_null:
        # Create the numpy array of the requested table or feature class from GeoDataBase
        table_search = da.FeatureClassToNumPyArray(gdb_table, search_field, where_clause=where_clause,
                                                   sql_clause=sql_clause, null_value=-9999)
    else:
        table_search = da.FeatureClassToNumPyArray(gdb_table, search_field, where_clause=where_clause,
                                                   sql_clause=sql_clause)

    # Creating DataFrame from the numpy array
    df = DataFrame(table_search)
    df.replace(-9999, np.nan, inplace=True)

    return df  # Return the DataFrame
