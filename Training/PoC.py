from arcpy import env, ListDatasets, CreateFeatureDataset_management, Describe, Delete_management, ListFeatureClasses,\
    CreateFeatureclass_management, AddField_management, da, RegisterAsVersioned_management, GetCount_management,\
    mapping, server
import json
import pandas as pd
import urllib2
import urllib
import ssl


def gentoken(user, passw, referer, tokenURL, expiration=60):
    # Re-usable function to get a token

    query_dict = {
        'username': user,
        'password': passw,
        'expiration': str(expiration),
        'client': 'referer',
        'referer': referer,
        'f': 'json'
    }

    query_string = urllib.urlencode(query_dict)

    tokenResponse = urllib.urlopen(tokenURL, query_string)
    token = json.loads(tokenResponse.read())

    if "token" not in token:
        print token['messages']
        exit()
    else:
        # Return the token to the function which called for it
        return token['token']


def createfolder(folder_name, gisserver, token, SSLcontext):

    query_dict = {
        'folderName': folder_name,
        'description': '',
        'f': 'json'
    }

    query_string = urllib.urlencode(query_dict)

    createFolderresp = urllib2.urlopen(gisserver +'createFolder/?f=json&token=' + token,
                                       query_string, context=SSLcontext)
    folder = json.loads(createFolderresp.read())
    return folder


# reading the config file
with open('config.json') as config_f:
    config = json.load(config_f)

# reading the input json file
with open('input.json') as input_f:
    input_detail = json.load(input_f)

# creating the workspace based on the specified database connection
# define local variable
env.workspace = config['gdb_conn']
LRS_network_fc = config['LRS_Network']
fds_name = "Data_{0}_DEV".format(input_detail['tahun'])
fds_SDE_name = str('ELRS.') + fds_name
req_fc = "{2}_{1}_{0}".format(input_detail['tahun'], input_detail['sms'], input_detail['data'])

#arcgis server credentials and URLs
username = 'subditadps'
password = 'data0217210371'
tokenUrl = "https://gisportal.binamarga.pu.go.id/arcgis/sharing/rest/generateToken"
gcontext = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
gisserver_url = 'https://gisserver.binamarga.pu.go.id:6443/arcgis/admin/services/'

# check for required FDS in database connection
# the fds name format is "Data_[year]"
if fds_SDE_name in ListDatasets():

    # check if the matched FDS name use the correct spatial reference
    # the spatial reference should be the same with the one used for LRS Network
    FDS_spatial_ref = Describe(fds_SDE_name).spatialReference.exportToString()
    LRS_spatial_ref = Describe(LRS_network_fc).spatialReference.exportToString()

    if FDS_spatial_ref == LRS_spatial_ref:
        pass
    else:

        # delete the FDS with the same name but with different spatial reference with the LRS Network
        # create new FDS with correct name and spatial reference
        Delete_management(fds_SDE_name)
        CreateFeatureDataset_management(env.workspace, fds_SDE_name, LRS_network_fc)

else:

    # create required FDS name with LRS Network spatial reference
    CreateFeatureDataset_management(env.workspace, fds_name, LRS_network_fc)

# update the environment workspace with FDS
env.workspace = (config['gdb_conn'] + '\\' + fds_SDE_name)


# check if the req_fc already exist in the FDS
# if the required feature class does not exist in the FDS, then create an empty FC
if 'ELRS.'+req_fc in ListFeatureClasses():
    pass
else:

    # create the required feature class
    # the fc geometry type is Polyline
    CreateFeatureclass_management(env.workspace, req_fc, geometry_type='POLYLINE', has_m='ENABLED')

    # add new field based on the config.json file into the newly created feature class
    for field in config[input_detail['data']]:
        AddField_management(req_fc, field['name'], field['type'], field_is_nullable=field['null'],
                            field_length=field['length'])


# insert features to the the created feature class in the FDS
# the input file is the input row for the new feature class in the FDS

# read the input file from the input.json
df_input = pd.read_excel(input_detail['file_path']+'/'+input_detail['file_name'], converters={'LINKID':str, 'Prov':str})
df_input.columns = df_input.columns.str.upper()
input_header = [str(_) for _ in list(df_input)]

# read the config header detail for event layer
df_config = pd.DataFrame.from_dict(config['roughness'])
config_header = [str(_) for _ in df_config['name'].tolist()]

# check for intersection between the input header and the header defined in the config file
intrsct_header = list(set(config_header).intersection(set(input_header)))
# add SHAPE@ field to the header list
intrsct_header.append('SHAPE@')


# insert row from the input file to the feature class using InsertCursor
# LRS FC with the absolute path, because the env.workspace has been updated
LRS_network_fc_path = config['gdb_conn']+ '\\' + LRS_network_fc

# start inserting new row
if int(GetCount_management(req_fc)[0]) == 0:
    with da.InsertCursor(req_fc, intrsct_header) as ins_cur:
        for index, row in df_input.iterrows():
            row_obj = []
            for col in intrsct_header[:-1]:
                row_obj.append(row[col])

            # insert the geometry object taken from the LRS Network and sliced based on ROUTEID, FROM_M and TO_M
            with da.SearchCursor(LRS_network_fc_path,
                                 'SHAPE@', where_clause="ROUTEID = '{0}'".format(row['ROUTEID'])) as s_cur:
                for lrs_geom in s_cur:
                    # multiplied by 1000 because of conversion from kilometers to meters
                    event_geom = lrs_geom[0].segmentAlongLine(row['FROMMEASURE']*1000, row['TOMEASURE']*1000)

            row_obj.append(event_geom)
            ins_cur.insertRow(row_obj)


# check if the FDS is not registered as versioned
# in order to register the newly created FC to the ALRS, the FC needed to be registered as a versioned
if not Describe(env.workspace).isVersioned:
    RegisterAsVersioned_management(env.workspace)


# Check for service folder in the arcgis server

# Generate token to access the arcgis server directory
generated_token = gentoken(username, password, tokenUrl, tokenUrl)

# Get the arcgis service directory
response = urllib2.urlopen(gisserver_url+'/?f=json&token='+generated_token, context=gcontext)
gisserver_dir = json.load(response)['folders']

# Check if the required already exist in the gis server directory
if config['GISServer_folder'] not in gisserver_dir:
    print createfolder(config['GISServer_folder'], gisserver_url, generated_token, gcontext)


# Create a map document for service definition
# Add all necessary data to the data frame
# Save the map document to .mxd format, need to specify the .mxd file path
# Read the .mxd file to create SDDraft file
# Upload the service definition draft file

# Access the blank mxd
# Add data to the data frame, then save the mxd file as a copy with different name
mxd_file = mapping.MapDocument(config['in_MXD_file'])
dataFrame = mxd_file.activeDataFrame
lyr = mapping.Layer(req_fc)
mapping.AddLayer(dataFrame, lyr)
mxd_file.saveACopy(config['out_MXD_path'] + "/Project2.mxd")

# Creating the SDDraft from the newly created mxd map document
wrkspc = config['out_MXD_path']
mxd_file = mapping.MapDocument(config['out_MXD_path'] + "/Project2.mxd")
con = 'GIS Servers/arcgis on gisserver.binamarga.pu.go.id_6443 (admin).ags'
service = input_detail['data']
sddraft = wrkspc + service + '.sddraft'
sd = wrkspc + service + '.sd'
foldername = config['GISServer_folder']

analysis = mapping.CreateMapSDDraft(mxd_file, sddraft, service, 'ARCGIS_SERVER',
                                          con, True, folder_name=foldername)

server.StageService(sddraft, sd)
server.UploadServiceDefinition(sd, con)