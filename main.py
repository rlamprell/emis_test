from src.extract                                    import Extract
from src.transform                                  import Transform
from src.db_connections.db_connection               import mysql_conn
from src.db_connections.configs.mysql               import mySQL_connection_details
#    src\db_connections\configs\db_connection_config.py

def main():
    raw_files           = Extract(folder_name="data_test").getFiles()
    file_transformer    = Transform()
    unpacked_files      = file_transformer.unpack_by_map(raw_files)
    seperated_files     = file_transformer.seperate_by_uniqueness_map(unpacked_files, 'resource.resourceType')

    connection          = mysql_conn(mySQL_connection_details())

    for df in seperated_files:
        current_df       = seperated_files[df]
        fields_to_remove = [
            'resource.extension',
            'resource.identifier',
            'resource.address',
            'resource.name',
            'resource.item',
            'resource.category',
            'resource.target']

        current_df = file_transformer.explode_nested_arrays(current_df, df, fields_to_remove)
        current_df = file_transformer.flatten_df(current_df)
        current_df = current_df.drop_duplicates()

        connection.executePost(table_name=df, df=current_df)


if __name__ == '__main__':
    main()