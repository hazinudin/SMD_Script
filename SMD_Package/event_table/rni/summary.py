import json
from SMD_Package.FCtoDataFrame import event_fc_to_df
from SMD_Package.load_config import SMDConfigs, Configs
from SMD_Package.TableWriter.GDBTableWriter import gdb_table_writer
from SMD_Package.event_table.kemantapan.kemantapan import Kemantapan
from arcpy import env, Exists
import os
import pandas as pd
import numpy as np
from datetime import datetime
import cx_Oracle


class RNISummary(object):
    def __init__(self, output_table, routes, year=None, **kwargs):
        """
        Used for summarizing RNI dataset.
        """
        if SMDConfigs.smd_dir() == '':
            pass
        else:
            os.chdir(SMDConfigs.smd_dir())  # Change directory to SMD root dir.

        smd_config = SMDConfigs()
        db_connection = smd_config.smd_database['instance']
        env.workspace = db_connection

        rni_config = Configs('RNICheck/rni_config_2020.json')
        self.road_type_group_path = "SMD_Package/event_table/rni/roadtype_group.json"

        self.table_name = smd_config.table_names['rni']

        self.routeid_col = smd_config.table_fields['rni']['route_id']
        self.from_m_col = smd_config.table_fields['rni']['from_measure']
        self.to_m_col = smd_config.table_fields['rni']['to_measure']
        self.lane_code_col = smd_config.table_fields['rni']['lane_code']
        self.lane_width = smd_config.table_fields['rni']['lane_width']
        self.road_type_col = smd_config.table_fields['rni']['road_type']
        self.segment_len_col = smd_config.table_fields['rni']['length_col']
        self.surf_type_col = smd_config.table_fields['rni']['surface_type']
        self.route_req = routes
        self.width_range = [4.5, 6, 7, 8, 14]
        self.width_col_pref = "WIDTH_CAT_"
        self.road_type_list = range(1, 26)  # From 1 to 25
        self.road_type_col_pref = "ROAD_TYPE_"
        self.update_date_col = 'UPDATE_DATE'
        self.force_update = False
        self.output_table = output_table
        self.total_len_col = 'TOTAL_LENGTH'

        lrs_table = smd_config.table_names['lrs_network']
        self.lrs_routeid_col = smd_config.table_fields['lrs_network']['route_id']
        self.lrs_sklen_col = smd_config.table_fields['lrs_network']['sk_length']

        if year is None:
            self.year = datetime.now().year
        else:
            self.year = year

        self.__dict__.update(kwargs)  # Update all class attribute.

        # Put all columns variable to a list.
        columns = [self.routeid_col, self.from_m_col, self.to_m_col, self.lane_code_col, self.lane_width,
                   self.road_type_col, self.segment_len_col, self.surf_type_col]
        self.rni_columns = columns

        # Get the evenet DataFrame.
        # self.df = event_fc_to_df(self.table_name, columns, self.route_req, self.routeid_col, env.workspace, True)
        # self.df[[self.from_m_col, self.to_m_col]] = self.df[[self.from_m_col, self.to_m_col]].astype(int)

        self.status = dict()
        self.route_selection = self._route_date_selection(self.output_table)  # Create the route selection.

        # Get the LRS SK length DataFrame.
        if (len(self.route_selection) > 1000) or (self.route_req == 'ALL'):
            self.sklen_df = event_fc_to_df(lrs_table, [self.lrs_routeid_col, self.lrs_sklen_col], "ALL",
                                           self.lrs_routeid_col, env.workspace)
        elif len(self.route_selection) > 0:
            self.sklen_df = event_fc_to_df(lrs_table, [self.lrs_routeid_col, self.lrs_sklen_col], self.route_selection,
                                           self.lrs_routeid_col, env.workspace)
        else:
            self.sklen_df = None

    @property
    def roadtype_class_col(self):
        cols = list()
        for r_group in self.road_type_group_df['ROAD_TYPE_GROUP'].tolist():
            column = self.road_type_col_pref + str(r_group)
            cols.append(column)

        return cols

    @property
    def width_class_col(self):
        cols = list()
        for width in range(1, (len(self.width_range) + 2)):
            column = self.width_col_pref + str(width)
            cols.append(column)

        return cols

    def project_to_sklen(self, df, columns=None):
        joined = pd.merge(df, self.sklen_df, left_on=self.routeid_col, right_on=self.lrs_routeid_col)
        joined['factor'] = joined[self.lrs_sklen_col]/joined[self.total_len_col]

        if columns is None:
            joined = joined.apply(lambda x: x*x['factor'], axis=1)
        else:
            joined[columns] = joined[columns].apply(lambda x: x*joined['factor'])
            joined[self.total_len_col] = joined[self.lrs_sklen_col]

        return joined.drop(['factor', self.lrs_sklen_col], axis=1)

    def rni_route_df(self, route):
        df = event_fc_to_df(self.table_name, self.rni_columns, route, self.routeid_col, env.workspace, True)
        df[[self.from_m_col, self.to_m_col]] = df[[self.from_m_col, self.to_m_col]].astype(int)
        return df

    def _write_to_df(self, df, output_table, decimal_rounding=2):
        col_details = dict()
        year_col = 'YEAR'
        df = df.round(decimal_rounding)
        df[year_col] = self.year

        for col_name in df.dtypes.to_dict():
            col_details[col_name] = dict()
            col_dtype = df.dtypes[col_name]

            # Translate to GDB data type.
            if col_dtype == 'object':
                gdb_dtype = 'string'
            elif col_dtype == 'float64':
                gdb_dtype = 'double'
            elif col_dtype in ['int64', 'int32']:
                gdb_dtype = 'short'
            else:
                pass

            col_details[col_name]['dtype'] = gdb_dtype

        gdb_table_writer(env.workspace, df, output_table, col_details, replace_key=[self.routeid_col, year_col])

    def _route_date_selection(self, output_table, chunk_size=600):
        if self.route_req == 'ALL':
            routes = self.route_req
        elif (type(self.route_req) == unicode) or (type(self.route_req) == str):
            routes = [self.route_req]
        elif type(self.route_req) == list:
            routes = self.route_req
        elif self.route_req is None:
            raise ("No Route parameter in the input JSON.")
        else:
            raise ("Route selection is neither list or string.")

        if self.route_req != 'ALL':
            self.status = {_route: "Missing RNI data." for _route in routes}  # Initialize route status.

        req_columns = [self.update_date_col, self.routeid_col]
        source_date = event_fc_to_df(self.table_name, req_columns, routes, self.routeid_col, env.workspace, True,
                                     sql_prefix='MAX ({0})'.format(self.update_date_col),
                                     sql_postfix='GROUP BY ({0})'.format(self.routeid_col))
        output_table_exist = Exists(output_table)

        source_routes = source_date[self.routeid_col].tolist()  # Get the available route from source table.
        self.status.update({_route: "Updated." for _route in source_routes})  # Update the status attribute.

        if not self.force_update and output_table_exist:
            output_date = event_fc_to_df(output_table, req_columns, routes, self.routeid_col, env.workspace, True)
            merged = pd.merge(source_date, output_date, on=self.routeid_col, how='outer', suffixes=('_SOURCE', '_TARGET'))
            selection = merged.loc[(merged['UPDATE_DATE_SOURCE'] > merged['UPDATE_DATE_TARGET']) |
                                   (merged['UPDATE_DATE_TARGET'].isnull())]
            route_selection = selection[self.routeid_col].tolist()
            not_updated = np.setdiff1d(source_routes, route_selection).tolist()  # Routes that will not be updated
            self.status.update({_route: "Not updated." for _route in not_updated})
        else:
            route_selection = source_routes

        if chunk_size < 1:  # Divide into chunks
            raise ValueError("Chunk size should be equal or larger than 1.")
        elif chunk_size == 1:
            return route_selection
        else:
            chunk_index = [x for x in range(0, len(route_selection), chunk_size)]
            chunks = [route_selection[x: x+chunk_size] if x+chunk_size < len(route_selection) else
                      route_selection[x: len(route_selection)+1] for x in chunk_index]
            return chunks

    @property
    def road_type_group_df(self):
        with open(self.road_type_group_path) as j_file:
            type_dict = json.load(j_file)

        df = pd.DataFrame.from_dict(type_dict, orient='index').stack().reset_index(level=0)
        df.rename(columns={'level_0': 'ROAD_TYPE_GROUP', 0: 'ROAD_TYPE'}, inplace=True)
        df['ROAD_TYPE'] = df['ROAD_TYPE'].astype(int)

        return df

    @staticmethod
    def surface_group_df():
        module_folder = os.path.dirname(__file__)
        surftype_json_file = os.path.join(module_folder, 'surftype_group.json')
        with open(surftype_json_file) as group_json:
            group_details = json.load(group_json)  # Load the surface type group JSON

        group_df = pd.DataFrame(group_details).transpose()
        stack = group_df.apply(lambda x: pd.Series(x['group']), axis=1).stack().reset_index(level=1, drop=True)
        stack.name = 'group'
        group_df = group_df.drop('group', axis=1).join(stack)
        group_df['group'] = group_df['group'].astype(int)

        return group_df

    @staticmethod
    def _create_length_col(df, input_columns, length_columns='TOTAL_LENGTH'):
        df.loc[:, length_columns] = np.sum(df[input_columns], axis=1)

        return df

    @staticmethod
    def execute_sql(query, params=None):
        """
        Execute SQL script from a string.
        :param query: SQL string.
        :param params: SQL parameter.
        :return: Pandas DataFrame.
        """
        dsn_tns = cx_Oracle.makedsn('10.10.1.97', '1521', service_name='geodbbm')
        connection = cx_Oracle.connect('SMD', 'SMD123M', dsn_tns)
        df_ora = pd.read_sql(query, con=connection, params=params)

        return df_ora


class WidthSummary(RNISummary):
    def __init__(self, write_to_db=True, project_to_sk=False, sql=False, **kwargs):

        output_table = "SMD.REKAP_LEBAR_RNI"  # Define the output table.
        if project_to_sk:
            output_table = output_table + "_SK"

        super(WidthSummary, self).__init__(output_table=output_table, **kwargs)
        self.columns = None

        for route in self.route_selection:
            if sql:
                self.columns = list()  # Change columns into list variable.
                sql_query = self.sql_route_groupby(route)
                result = self.execute_sql(sql_query)
            else:
                df = self.rni_route_df(route)

                segment_g = df.groupby([self.routeid_col, self.from_m_col, self.to_m_col])
                lane_w_g = segment_g.agg({self.lane_width: 'sum', self.segment_len_col: 'mean'}).reset_index()
                width_cat_col = 'width_cat'
                lane_w_g[('%s' % width_cat_col)] = pd.Series(np.nan)

                first_range = self.width_range[0]
                last_range = self.width_range[len(self.width_range)-1]
                lane_w_g.loc[lane_w_g[self.lane_width] <= first_range, width_cat_col] = '{0}1'.\
                    format(self.width_col_pref)
                lane_w_g.loc[lane_w_g[self.lane_width] >= last_range, width_cat_col] = '{0}{1}'.\
                    format(self.width_col_pref, len(self.width_range))

                for w_range in self.width_range:
                    range_ind = self.width_range.index(w_range)
                    if range_ind == 0:
                        continue
                    else:
                        prev_range = self.width_range[range_ind-1]
                        lane_w_g.loc[((lane_w_g[self.lane_width] <= w_range) &
                                      (lane_w_g[self.lane_width] > prev_range)),
                                     width_cat_col] = '{0}{1}'.format(self.width_col_pref, range_ind + 1)

                pivot = lane_w_g.pivot_table(self.segment_len_col, index=self.routeid_col, columns=width_cat_col,
                                             aggfunc=np.sum)
                route_g = lane_w_g.groupby(self.routeid_col)[self.lane_width].mean()
                result = pivot.join(route_g)
                result[self.total_len_col] = result.sum(axis=1)  # Create the total length column.

            if project_to_sk:
                result = self.project_to_sklen(result, columns=self.columns)

            missing_col = np.setdiff1d(self.width_class_col, list(result))
            result[missing_col] = pd.DataFrame(0, columns=missing_col, index=result.index)
            result.fillna(0, inplace=True)

            if write_to_db:
                self._write_to_df(result, self.output_table)
                print str(self.route_selection.index(route)+1) + "/" + str(len(self.route_selection))

    def sql_segment_groupby(self, routes):
        if (type(routes) == str) or (type(routes) == unicode):
            pass
        elif type(routes) == list:
            routes = [str(_) for _ in routes]
            routes = str(routes).strip('[').strip(']')
        else:
            raise (Exception('Input routes is neither a str or list'))  # Raise an exception.

        seg_groupby = "SELECT {routeid_col}, {from_m_col}, MAX({to_m_col}) AS {to_m_col}, " \
                      "SUM({lane_width}) AS {lane_width}, MAX({segment_len_col}) AS {segment_len_col}\n" \
                      "FROM {table_name} \n" \
                      "WHERE {routeid_col} IN ({routes}) \n" \
                      "GROUP BY {routeid_col}, {from_m_col}".format(routes=routes, **self.__dict__)

        return seg_groupby

    def sql_route_groupby(self, routes):
        sql_select = 'SELECT t1.{routeid_col}, \n' \
                     'SUM(t1.{segment_len_col}) AS {total_len_col}, \n' \
                     'AVG(t1.{lane_width}) AS {lane_width}, \n'.format(**self.__dict__)

        upper_end_case = 'SUM(CASE WHEN t1.{lane_width} > {value} THEN t1.{segment_len_col} ELSE 0 END)'
        lower_end_case = 'SUM(CASE WHEN t1.{lane_width} <= {value} THEN t1.{segment_len_col} ELSE 0 END)'
        middle_case = 'SUM(CASE WHEN t1.{lane_width} > {lower_bound} AND t1.{lane_width} <= {upper_bound} THEN ' \
                      't1.{segment_len_col} ELSE 0 END)'

        for i, width in enumerate(self.width_range, start=1):  # Iterate over all width range.
            column_name = self.width_col_pref + str(i)
            self.columns.append(column_name)

            if i == 1:
                statement = lower_end_case.format(value=width, **self.__dict__) + " AS " + column_name + ", \n"
                sql_select += statement

            # elif i != len(self.width_range):
            else:
                lower_bound = self.width_range[i-2]
                upper_bound = width
                statement = middle_case.format(lower_bound=lower_bound, upper_bound=upper_bound, **self.__dict__)
                statement += " AS " + column_name + ", \n"
                sql_select += statement

        # CASE for the last classification.
        statement = upper_end_case.format(value=max(self.width_range), **self.__dict__) + " AS " + \
                    self.width_col_pref + str(len(self.width_range)+1) + "\n"

        self.columns.append(self.width_col_pref + str(len(self.width_range)+1))

        sql_select += statement

        sql_select += " FROM( \n" + self.sql_segment_groupby(routes=routes) + ") t1 \n " \
                                                                                          "GROUP BY t1.{routeid_col}".\
            format(**self.__dict__)

        return sql_select


class RoadTypeSummary(RNISummary):
    def __init__(self, write_to_db=True, project_to_sk=False, sql=True, **kwargs):

        output_table = "SMD.REKAP_TIPE_JALAN"
        if project_to_sk:
            output_table = output_table + "_SK"

        super(RoadTypeSummary, self).__init__(output_table=output_table, **kwargs)

        type_group_df = self.road_type_group_df

        for route in self.route_selection:
            df = self.rni_route_df(route)

            pivot_roadtype_col = '_road_type'
            df = df.merge(type_group_df, on='ROAD_TYPE')
            df[pivot_roadtype_col] = df['ROAD_TYPE_GROUP'].apply(lambda x: self.road_type_col_pref + str(x))

            # if lkm:
            #     pivot = df.pivot_table(self.segment_len_col, index=[self.routeid_col, self.lane_code_col],
            #                            columns=pivot_roadtype_col, aggfunc=np.sum).reset_index()
            #     pivot = pivot.groupby([self.routeid_col]).sum().reset_index()
            # else:
            #     centerline = df.groupby([self.routeid_col, self.from_m_col, self.to_m_col]).\
            #         agg({self.segment_len_col: np.mean,
            #              pivot_roadtype_col: (lambda x: x.value_counts().index[0])
            #              }).reset_index()
            #     pivot = centerline.pivot_table(self.segment_len_col, index=[self.routeid_col],
            #                                    columns=pivot_roadtype_col, aggfunc=np.sum)
            #
            #     if project_to_sk:
            #         pivot = self.project_to_sklen(pivot)
            #
            #     pivot.reset_index(inplace=True)

            centerline = df.groupby([self.routeid_col, self.from_m_col, self.to_m_col]). \
                agg({self.segment_len_col: np.mean,
                     pivot_roadtype_col: (lambda x: x.value_counts().index[0])
                     }).reset_index()
            pivot = centerline.pivot_table(self.segment_len_col, index=[self.routeid_col],
                                           columns=pivot_roadtype_col, aggfunc=np.sum)

            if project_to_sk:
                pivot = self.project_to_sklen(pivot)

            pivot.reset_index(inplace=True)
            pivot[self.total_len_col] = pivot.sum(axis=1)

            missing_col = np.setdiff1d(self.roadtype_class_col, list(pivot))
            pivot[missing_col] = pd.DataFrame(0, columns=missing_col, index=pivot.index)
            pivot.fillna(0, inplace=True)

            if write_to_db:
                self._write_to_df(result, self.output_table)
                print str(self.route_selection.index(route)+1) + "/" + str(len(self.route_selection))

    def _sql_segment_groupby(self, routes):
        if (type(routes) == str) or (type(routes) == unicode):
            pass
        elif type(routes) == list:
            routes = [str(_) for _ in routes]
            routes = str(routes).strip('[').strip(']')
        else:
            raise (Exception('Input routes is neither a str or list'))  # Raise an exception.

        seg_groupby = "SELECT {routeid_col}, {from_m_col}, MAX({to_m_col}) AS {to_m_col}, " \
                      "MAX({road_type_col}) AS {road_type_col}, MAX({segment_len_col}) AS {segment_len_col}\n" \
                      "FROM {table_name} \n" \
                      "WHERE {routeid_col} IN ({routes}) \n" \
                      "GROUP BY {routeid_col}, {from_m_col}".format(routes=routes, **self.__dict__)

        return seg_groupby

    def sql_route_groupby(self, routes):
        sql_select = 'SELECT t1.{routeid_col}, \n' \
                     'SUM(t1.{segment_len_col}) AS {total_len_col}\n'.format(**self.__dict__)

        road_type_df = self.road_type_group_df
        road_type_ser = road_type_df.groupby('ROAD_TYPE_GROUP')[self.road_type_col].apply(list)

        case = ", SUM(CASE WHEN t1.{road_type_col} IN ({type_group}) THEN t1.{segment_len_col} ELSE 0 END) AS " \
               "{column_name} \n"

        for road_type, type_group in road_type_ser.iteritems():
            str_group = str(type_group).strip('[').strip(']')
            statement = case.format(type_group=str_group, column_name=str(self.road_type_col_pref + "_" + road_type),
                                    **self.__dict__)
            sql_select += statement

        sql_select += " FROM( \n" + self._sql_segment_groupby(routes=routes) + ") t1 \n " \
                                                                                        "GROUP BY t1.{routeid_col}".\
            format(**self.__dict__)

        return sql_select


class SurfaceTypeSummary(RNISummary):
    def __init__(self, write_to_db=True, lkm=False, project_to_sk=False, sql=False, **kwargs):

        output_table = "SMD.REKAP_TIPE_PERKERASAN"
        if project_to_sk:
            output_table = output_table + '_SK'
        elif lkm:
            output_table = output_table + '_LKM'

        super(SurfaceTypeSummary, self).__init__(output_table=output_table, **kwargs)

        def select_surf_type(series):
            count = series.value_counts()

            if len(count) == 1:  # If there is only single value.
                return count.index[0]
            else:
                first_count = count[0]  # The most common value count.
                same_count = count.loc[count == first_count]  # The same count as the most common.

                if len(same_count) == 1:  # If there is no same count with the most common value.
                    return count.index[0]
                else:
                    same_count = same_count.reset_index(name='count')
                    same_count = same_count.merge(surface_order, left_on='index', right_on='_surface_type')
                    min_order = same_count['order'].min()  # The highest available order.
                    highest_ind = same_count.loc[same_count['order'] == min_order, '_surface_type'].tolist()[0]

                    return highest_ind

        pivot_surface_type = '_surface_type'
        surface_g_df = self.surface_group_df().reset_index().rename(columns={'index': pivot_surface_type})
        surface_g_df[pivot_surface_type] = surface_g_df[pivot_surface_type].apply(lambda x: str(x).upper())
        surfaces = surface_g_df[pivot_surface_type].tolist()
        surface_order = surface_g_df.groupby(['_surface_type'])['order'].max().reset_index(name='order')

        for route in self.route_selection:
            if sql:
                query = self.sql_route_groupby(routes=route, lkm=lkm)
                result = self.execute_sql(query)
            else:
                    df = self.rni_route_df(route)

                    merged = df.merge(surface_g_df[[pivot_surface_type, 'group']], left_on=self.surf_type_col,
                                      right_on='group')

                    if lkm:
                        pivot = merged.pivot_table(self.segment_len_col, index=[self.routeid_col, self.lane_code_col],
                                                   columns=pivot_surface_type, aggfunc=np.sum).reset_index()
                        pivot = pivot.groupby(self.routeid_col).sum()
                    else:
                        centerline = merged.groupby([self.routeid_col, self.from_m_col, self.to_m_col]).\
                            agg({self.segment_len_col: np.mean,
                                 '_surface_type': (lambda x: select_surf_type(x))
                                 }).reset_index()
                        pivot = centerline.pivot_table(self.segment_len_col, index=[self.routeid_col],
                                                       columns=pivot_surface_type, aggfunc=np.sum)

                    result = pivot.reset_index(inplace=True)

            if project_to_sk and not lkm:
                result = self.project_to_sklen(result, surfaces)

            missing_col = np.setdiff1d(surfaces, list(result))
            result[missing_col] = pd.DataFrame(0, index=result.index, columns=missing_col)
            result.fillna(0, inplace=True)

            if write_to_db:
                self._write_to_df(result, self.output_table)
                print str(self.route_selection.index(route)+1) + "/" + str(len(self.route_selection))

    def _sql_segment_groupby(self, routes, lkm=False):
        if (type(routes) == str) or (type(routes) == unicode):
            pass
        elif type(routes) == list:
            routes = [str(_) for _ in routes]
            routes = str(routes).strip('[').strip(']')
        else:
            raise (Exception('Input routes is neither a str or list'))  # Raise an exception.

        if lkm:
            seg_groupby = "SELECT {routeid_col}, {from_m_col}, {to_m_col}, " \
                          "{surf_type_col}, {segment_len_col}\n" \
                          "FROM {table_name} \n" \
                          "WHERE {routeid_col} IN ({routes}) \n".format(routes=routes, **self.__dict__)

        else:
            seg_groupby = "SELECT {routeid_col}, {from_m_col}, MAX({to_m_col}) AS {to_m_col}, " \
                          "MAX({surf_type_col}) AS {surf_type_col}, MAX({segment_len_col}) AS {segment_len_col}\n" \
                          "FROM {table_name} \n" \
                          "WHERE {routeid_col} IN ({routes}) \n" \
                          "GROUP BY {routeid_col}, {from_m_col}".format(routes=routes, **self.__dict__)

        return seg_groupby

    def sql_route_groupby(self, routes, lkm=False):
        sql_select = 'SELECT t1.{routeid_col}, \n' \
                     'SUM(t1.{segment_len_col}) AS {total_len_col}\n'.format(**self.__dict__)

        surface_df = self.surface_group_df().reset_index()
        surface_df = surface_df.groupby('index')['group'].apply(lambda x: list(x))

        case = ", SUM(CASE WHEN t1.{surf_type_col} IN ({surface_group}) THEN t1.{segment_len_col} ELSE 0 END) AS " \
               "{surface_type} \n"

        for surface_type, surface_group in surface_df.iteritems():
            str_group = str(surface_group).strip('[').strip(']')
            statement = case.format(surface_group=str_group, surface_type=str(surface_type).upper(),
                                    **self.__dict__)
            sql_select += statement

        sql_select += " FROM( \n" + self._sql_segment_groupby(routes=routes, lkm=lkm) + ") t1 \n " \
                                                                                          "GROUP BY t1.{routeid_col}".\
            format(**self.__dict__)

        return sql_select
