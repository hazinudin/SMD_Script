from arcpy import da, env, CreateTable_management, AddField_management

env.overwriteOutput = True


def gdb_table_writer(workspace, dataframe, table_name, cols_dtype, new_table=False):
    """
    This function writes input DataFrame as geodatabase event table
    :param workspace: The workspace for target database table
    :param dataframe: The input DataFrame
    :param new_table: If true then a new table will be created
    :param table_name: The target table name
    :param cols_dtype: The colums of target table name, if new 'new_table' is True then the column will be used to
    create new column in the newly created table.
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