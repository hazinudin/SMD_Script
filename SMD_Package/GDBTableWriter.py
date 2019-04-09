from arcpy import da, env, CreateTable_management, AddField_management

env.overwriteOutput = True


def gdb_table_writer(workspace, dataframe, table_name, cols_dtype, new_table=False):
    """
    This function writes input DataFrame as geodatabase event table
    :param workspace:
    :param dataframe:
    :param new_table:
    :param table_name:
    :param cols_dtype:
    :return:
    """

    if new_table:
        CreateTable_management(workspace, table_name)
        for col in cols_dtype.keys():
            AddField_management(table_name, col, cols_dtype[col])

    cursor = da.InsertCursor(table_name, cols_dtype.keys())
    for index, row in dataframe.iterrows():
        row_object = []
        for col_name in cols_dtype.keys():
            row_object.append(row[col_name])
        cursor.insertRow(row_object)