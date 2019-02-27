from arcpy import env, ListDatasets, CreateFeatureDataset_management, Describe, Delete_management, ListFeatureClasses,\
    CreateFeatureclass_management, AddField_management, da, RegisterAsVersioned_management, GetCount_management,\
    mapping, server, GetParameterAsText, AddMessage, SetParameterAsText
import json
import pandas as pd
import os
import sys


def outputProcess(status, message):
    """Write output message to the ouput JSON"""

    outDict = {
        "status": status,
        "message": message
    }

    return outDict


# geoprocessing parameter
inputJSON = GetParameterAsText(0)
createMapService = GetParameterAsText(1)

pyScript = sys.argv[0]
toolDir = os.path.dirname(pyScript)
configJSON = os.path.join(toolDir, "config.json")

# read the config file
with open("config.json") as config_f:
    config = json.load(config_f)


# read the input json file
InputDetails = json.loads(inputJSON)

# creating the workspace based on the specified database connection
# define variable
GDB_connection = config['gdb_conn']
GDB_user = Describe(GDB_connection).connectionProperties.user
env.workspace = GDB_connection
env.overwriteOutput = True

LRSNetwork_fc = config['LRS_Network']
FDS_name = "Data_{0}_DEV_1".format(InputDetails['tahun'])
FDS_SDE_name = GDB_user+'.'+FDS_name
inputData = InputDetails['data']
ReqFC = "{2}_{1}_{0}_DEV".format(InputDetails['tahun'], InputDetails['sms'], inputData)
ReqFC_SDE_name = GDB_user+'.'+ReqFC

mapDocumentName = 'Project2.mxd'
messageString = ''


# check for required FDS in database connection
# the fds name format is "Data_[year]"
newFeatureDataset = False
if FDS_SDE_name in ListDatasets():

    # check if the matched FDS name use the correct spatial reference
    # the spatial reference should be the same with the one used for LRS Network
    FDS_spatial_ref = Describe(FDS_SDE_name).spatialReference.exportToString()
    LRS_spatial_ref = Describe(LRSNetwork_fc).spatialReference.exportToString()

    if FDS_spatial_ref == LRS_spatial_ref:
        pass
    else:

        # delete the FDS with the same name but with different spatial reference with the LRS Network
        # create new FDS with correct name and spatial reference
        Delete_management(FDS_SDE_name)
        CreateFeatureDataset_management(env.workspace, FDS_SDE_name, LRSNetwork_fc)
        newFeatureDataset = True

else:

    # create required FDS name with LRS Network spatial reference
    CreateFeatureDataset_management(env.workspace, FDS_name, LRSNetwork_fc)
    newFeatureDataset = True

# update the environment workspace with FDS
env.workspace = (GDB_connection + '\\' + FDS_SDE_name)

# check if the required fc already exist in the FDS
# if the required feature class does not exist in the FDS, then create an empty FC
if ReqFC_SDE_name not in ListFeatureClasses():

    newFeatureClass = True

    # create the required feature class
    # the fc geometry type is Polyline
    CreateFeatureclass_management(env.workspace, ReqFC, geometry_type='POLYLINE', has_m='ENABLED')

    # add new field based on the config.json file into the newly created feature class
    for field in config[inputData]:
        AddField_management(ReqFC, field['name'], field['type'], field_is_nullable=field['null'],
                            field_length=field['length'])
else:
    newFeatureClass = False


# insert features to the newly created feature class in the FDS
# the input file is the input row for the new feature class in the FDS

# read the input file from the input.json
df_input = pd.read_excel(InputDetails['file_name'])

# prepare the converter for all column in Excel file
sConverters = {col: str for col in list(df_input)}

# re read the excel file using the sConverters
# read all the column as str
# check if the file exist
try:
    df_input = pd.read_excel(InputDetails['file_name'], converters=sConverters)
except:
    AddMessage('{0} does not exist or cannot be opened'.format(InputDetails['file_name']))

# convert header name to upper case
df_input.columns = df_input.columns.str.upper()
input_header = [str(_) for _ in list(df_input)]

# read the config header detail for event layer
df_config = pd.DataFrame.from_dict(config[inputData])
# create list containing all header name in the config.json
config_header = [str(_) for _ in df_config['name'].tolist()]
# create list containing all numerical columns defined in the config.json
numerical_cols = df_config.loc[df_config['type'].isin(['DOUBLE', 'SHORT', 'LONG']), 'name'].tolist()

# Check for non numerical value in numerical column defined in the config.json
# If non numerical value exist in the defined numerical column, then the no new row will be inserted to the FC
while 1:
    try:
        for col in numerical_cols:
            df_input[col] = pd.to_numeric(df_input[col], errors='raise')

        # check for intersection between the input header and the header defined in the config file
        intrsct_header = list(set(config_header).intersection(set(input_header)))
        # add SHAPE@ field to the header list
        intrsct_header.append('SHAPE@')

        # insert row from the input file to the feature class using InsertCursor
        # LRS FC with the absolute path, because the env.workspace has been updated
        LRS_network_fc_path = GDB_connection + '\\' + LRSNetwork_fc

        # start inserting new row
        # only insert new row if the fc is empty
        if int(GetCount_management(ReqFC)[0]) == 0:
            routeList = []
            rowCount = 0
            newRowInserted = True
            with da.InsertCursor(ReqFC, intrsct_header) as ins_cur:
                for index, row in df_input.iterrows():
                    row_obj = []
                    for col in intrsct_header[:-1]:
                        row_obj.append(row[col])

                    # insert the geometry object taken from the LRS Network and sliced based on ROUTEID, FROM_M and TO_M
                    # create a list storing all route_id written into the feature class
                    if row['ROUTEID'] not in routeList:
                        routeList.append(str(row['ROUTEID']))
                    with da.SearchCursor(LRS_network_fc_path,
                                         'SHAPE@', where_clause="ROUTEID = '{0}'".format(row['ROUTEID'])) as s_cur:
                        for lrs_geom in s_cur:
                            # multiplied by 1000 because of conversion from kilometers to meters
                            event_geom = lrs_geom[0].segmentAlongLine(row['FROMMEASURE'] * 1000,
                                                                      row['TOMEASURE'] * 1000)

                    row_obj.append(event_geom)
                    ins_cur.insertRow(row_obj)
                    rowCount += 1
        else:
            newRowInserted = False

        # check if the FDS is not registered as versioned
        # in order to register the newly created FC to the ALRS, the FC needed to be registered as a versioned
        if not Describe(env.workspace).isVersioned:
            RegisterAsVersioned_management(env.workspace)

        # Create a map document for service definition
        # Add all necessary data to the data frame
        # Save the map document to .mxd format, need to specify the .mxd file path
        # Read the .mxd file to create SDDraft file
        # Upload the service definition file

        # Access the blank mxd
        # Add data to the data frame, then save the mxd file as a copy with different name
        mxd_file = mapping.MapDocument(config['in_MXD_file'])
        dataFrame = mxd_file.activeDataFrame

        # Add all data in the FDS to the data frame
        for feature_class in ListFeatureClasses():
            lyr = mapping.Layer(feature_class)
            lyr.name = feature_class
            mapping.AddLayer(dataFrame, lyr)

        mxd_file.saveACopy(config['out_MXD_path'] + mapDocumentName)

        # if create map service is true, then create SDDraft, SD and upload the service definition
        if createMapService:
            # Creating the SDDraft from the newly created mxd map document
            mapDocumentWorkspace = config['out_MXD_path']
            mxd_file = mapping.MapDocument(config['out_MXD_path'] + mapDocumentName)
            con = config['ArcGIS_Server_conn']
            service = inputData
            sddraft = mapDocumentWorkspace + service + '.sddraft'
            sd = mapDocumentWorkspace + service + '.sd'
            foldername = config['GISServer_folder']

            analysis = mapping.CreateMapSDDraft(mxd_file, sddraft, service, 'ARCGIS_SERVER',
                                                con, True, folder_name=foldername)

            server.StageService(sddraft, sd)
            server.UploadServiceDefinition(sd, con)

    except:
        AddMessage('Non numerical value exist in the {0} column'.format(col))
        newRowInserted = False
        dataTypeError = True
        break

# create output message
# feature dataset message output
if newFeatureDataset:
    messageString += 'New Feature Dataset successfully created. \n'
else:
    messageString += '{0} Feature Dataset already exist, no new feature dataset was created. \n'.format(FDS_SDE_name)

# feature class message output
if newFeatureClass:
    messageString += 'New Feature Class successfully created. \n'
else:
    messageString += '{0} Feature Class already exist, no new feauture class was created. \n'.format(ReqFC_SDE_name)

# insert row message output
if newRowInserted:
    messageString += '{0} rows successfully inserted to the {1}. \n'.format(rowCount, ReqFC_SDE_name)
elif dataTypeError:
    messageString += 'Failed inserting new row, {0} contain non numerical value. \n'.format(col)
else:
    messageString += '{0} is not empty, no new row inserted. \n'.format(ReqFC_SDE_name)


outMessage = outputProcess('success', messageString)

AddMessage(messageString)
outputJSON = json.dumps(outMessage)
SetParameterAsText(0, outputJSON)