from arcpy import da, env
import pandas as pd

route = raw_input("Select Route: ")
env.workspace = 'Database Connections/ELRS@GEODBBM 144.sde'
rni_fc = 'ELRS.RNI_National_2_REV'
lrs_network = 'ELRS.National_Network2018'

ar = da.FeatureClassToNumPyArray(rni_fc, ['KMPOST', 'KMPOSTTO', 'LANE_CODE', 'LINKID'],
                                 where_clause="LINKID='{0}'".format(route))
df = pd.DataFrame(ar)  # Create the DataFrame

with da.SearchCursor(lrs_network, 'SHAPE@', where_clause="ROUTEID='{0}'".format(route)) as cursor:
    for row in cursor:
        lrs_geom = row[0]

df['LONGITUDE'] = pd.Series(0)  # Create empty column
df['LATITUDE'] = pd.Series(0)
df['ALTITUDE'] = pd.Series(0)

for index, row in df.iterrows():
    from_meas = row['KMPOST']*10  # In meters
    to_meas = row['KMPOSTTO']*10
    lane = str(row['LANE_CODE'])

    if lane[0] == 'L':
        point = lrs_geom.positionAlongLine(to_meas)
    if lane[0] == 'R':
        point = lrs_geom.positionAlongLine(from_meas)

    point = point.projectAs('4326')
    df.loc[index, ['LONGITUDE']] = point.lastPoint.X
    df.loc[index, ['LATITUDE']] = point.lastPoint.Y
    df.loc[index, ['ALTITUDE']] = 0

df['LENGTH'] = (df['KMPOSTTO']-df['KMPOST'])/100
df.sort_values(by=['KMPOST', 'LANE_CODE'], inplace=True)
df.to_csv("{0}/ValidTable_{1}.csv".format('C:\Users\Asus\Documents\ArcGIS\scratch', route))
