
from src.extract            import Extract
from src.transform          import Transform
# from src.mySQL_connection   import mySQL_connection
# from src.db_connection      import db_connection
# from src.db_connections     import db_connection
from src.db_connections.mySQL_connection import mySQL_connection


def main():
    raw_files           = Extract(folder_name="test_data").getFiles()
    file_transformer    = Transform()
    unpacked_files      = file_transformer.unpack_by_map(raw_files)
    seperated_files     = file_transformer.seperate_by_uniqueness_map(unpacked_files, 'resource.resourceType')

    for df in seperated_files:
        current_df = seperated_files[df]
        # current_df = file_transformer.explode_nested_arrays(current_df, df)
        current_df = file_transformer.flatten_df(current_df)
        current_df = current_df.drop_duplicates()

        mySQL_connection().post(df, current_df)
        # output = mySQL_connection().get('Patient')
        # print(output)
        # mySQL = db_connection("mysql")

if __name__ == '__main__':
    main()