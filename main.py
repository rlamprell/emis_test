from src.extract                                    import Extract
from src.transform                                  import Transform
from src.db_connections.db_connection               import mysql_conn
from src.db_connections.configs.mysql               import mySQL_connection_details

import csv

def validate():
    pass


def main():
    raw_files           = Extract(folder_name="data").getFiles()
    file_transformer    = Transform()
    unpacked_files      = file_transformer.unpack_by_map(raw_files)
    seperated_files     = file_transformer.seperate_by_uniqueness_map(unpacked_files, 'resource.resourceType')
    connection          = mysql_conn(mySQL_connection_details())

    for df_name in seperated_files:
        current_df       = seperated_files[df_name]
        fields_to_remove = [
            'resource.extension',
            'resource.identifier',
            'resource.address',
            'resource.name',
            'resource.item',
            'resource.category',
            'resource.target']


        current_df = file_transformer.explode_nested_arrays(current_df, df_name, fields_to_remove)
        current_df = file_transformer.flatten_df(current_df)
        current_df = current_df.drop_duplicates()

        # hotfix for unsupported characters - not sure if I caused this through transformations or if it's MySQl's support for utf-8
        if df_name=='Patient':
            file_transformer.remove_ascii_chars(current_df, 'resource.extension.valueAddress.state')

        connection.executePost(table_name=df_name, df=current_df)
        
        # get count on dataframe
        # get count on executeGet(df_name)
        # compare the two
        #
        # df_count = len(current_df.index)
        # mysql_count = connection.executeGet(df_name, "count(*)")
        # status = False
        # all_rows_exported = df_count==mysql_count
        # if all_rows_exported:
        #     status = True
        # print(f"Finished Exporting: {df_name}, Success: {status}, df_row_count: {df_count}, mysql_row_count: {mysql_count}")
        print(f"Finished Exporting: {df_name}")


if __name__ == '__main__':
    main()