import pandas as pd

df = pd.read_csv('RNI_test_dissolve.csv', dtype=object)
fromM = 0
result = []

df['CODE_LANE'] = df['CODE_LANE'].astype(object, inplace=True)

for index, row in df.iterrows():
    
    if index == 0:
        fromM = row['FROMMEASURE']
        toM = row['TOMEASURE']
        codeLane = row['CODE_LANE']

        initial_i = index
    else:
        if codeLane == row['CODE_LANE']:
            toM = row['TOMEASURE']
        else:
            obj = [df.iloc[initial_i]['FROMMEASURE'], df.iloc[index]['TOMEASURE']]
            obj.append(df.iloc[index]['CODE_LANE'])
            result.append(obj)
            
            fromM = row['FROMMEASURE']
            toM = row['TOMEASURE']
            codeLane = row['CODE_LANE']

    if index == len(df)-1:
        obj = [fromM, toM]
        obj.append(codeLane)
        result.append(obj)


for row in df.itertuples():
    print row
    
print result
print len(df)
    
