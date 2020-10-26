from SMD_Package import event_fc_to_df, SMDConfigs, GetRoutes, output_message
from arcpy import env, Exists
import zipfile
import pandas as pd
import os


class DownloadTable(object):
    def __init__(self, table_name, routeid_col="LINKID", **kwargs):

        smd_config = SMDConfigs()
        if SMDConfigs.smd_dir() == '':
            pass
        else:
            os.chdir(SMDConfigs.smd_dir())
        self.smd_config = smd_config

        env.workspace = smd_config.smd_database['instance']

        self.table_name = table_name
        self.output_folder = env.scratchFolder  # All of the output file will be stored in this folder.
        self.routeid_col = routeid_col
        self.table_exists = Exists(table_name)

    def download_as_csv(self, routes, output_file, columns="*", **kwargs):
        """
        Download the requested table as csv.
        :param routes: Requested routes.
        :param output_file: Output csv file name.
        :param columns: The requested columns.
        :return:
        """
        df = event_fc_to_df(self.table_name, columns, routes, self.routeid_col, env.workspace, True)
        df.to_csv('{0}/{1}'.format(self.output_folder, output_file), index=False)

        return self

    def create_zipfile(self, output_zip_file, input_files, **kwargs):
        """
        Create zipfile containing the input file.
        :param output_zip_file: Output zip file name.
        :param input_files: Files to be zipped.
        :return:
        """

        with zipfile.ZipFile(output_zip_file, 'w') as new_zip:  # Creating a new empty zip file.
            for input_file in input_files:
                new_zip.write('{0}/{1}'.format(self.output_folder, input_file), input_file)

        return self


class DownloadBalaiTable(DownloadTable):
    """
    Class for downloading table for each Balai.
    """
    def __init__(self, type, codes, table_name, sub_div='satker', **kwargs):
        super(DownloadBalaiTable, self).__init__(table_name, **kwargs)

        self.get_route = GetRoutes(type, codes)
        self.routes = self.get_route.route_list()
        self.table_name = table_name
        self.route_divs = dict()  # The route division dictionary.
        self.files = list()

        if str(type) == 'balai':  # The request type either 'BALAI' or 'PROVINSI'
            self.request_type = 'BALAI'
        elif str(type) == 'no_prov':
            self.request_type = 'PROVINSI'
        else:
            self.request_type = '_'

        self.create_sub_divs(sub_div)

        if self.table_exists:
            for division in self.route_divs:  # Start download all the data as CSV file.
                division_routes = self.route_divs[division]
                division_file = division+'.csv'
                self.files.append(division_file)
                self.download_as_csv(division_routes, division_file)

            self.output_zipfile = '{0}/{1}'.format(self.output_folder,
                                                   self.request_type + '_' + codes + '.zip')
            self.create_zipfile(self.output_zipfile, self.files)
            self.status = output_message("Succeeded.", "-")
        else:
            self.output_zipfile = "Table does not exists."
            self.status = output_message("Failed.", "-")

    def create_sub_divs(self, sub_div):
        """
        Create route sub division group based on the sub_div request type.
        :param sub_div: 'satker' or 'no_prov'
        :return:
        """
        if sub_div == 'satker':
            satker_ppk_route_table = self.smd_config.table_names['ppk_route_table']
            satker_route_fields = self.smd_config.table_fields['ppk_route_table']
            satker_routeid = satker_route_fields['route_id']
            satker_ppk_id = satker_route_fields['satker_ppk_id']

            if len(self.routes) < 1000:
                satker_df = event_fc_to_df(satker_ppk_route_table, [satker_routeid, satker_ppk_id], self.routes,
                                           satker_routeid, env.workspace, True)
            else:
                satker_df = event_fc_to_df(satker_ppk_route_table, [satker_routeid, satker_ppk_id], 'ALL',
                                           satker_routeid, env.workspace, True)
                satker_df = satker_df.loc[satker_df[satker_routeid].isin(self.routes)].reset_index()

            grouped = satker_df.groupby(satker_ppk_id)[satker_routeid].apply(list)

        elif sub_div == 'no_prov':
            route_prov_df = pd.DataFrame([[x, str(x)[:2]] for x in self.routes], columns=['LINKID', 'NO_PROV'])
            grouped = route_prov_df.groupby('NO_PROV')['LINKID'].apply(list)
        else:
            raise ValueError("sub_div is neither 'satker' nor 'no_prov'")

        self.route_divs.update(grouped.to_dict())  # Update the route division dictionary.
        return self
