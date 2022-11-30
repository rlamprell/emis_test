import pandas as pd

import glob
import os

class File:
    def __init__(self, file_name):
        self.file_name = file_name
        self.file_path = file_path



# import the files
class Extract:
    __slots__ = ('files')

    def __init__(self, files):
        current_path    = os.getcwd()
        print(current_path)
        self.files      = glob.glob(f"{current_path}\data\*.json")

    def getFiles(self):
        return self.files


from    multiprocessing import Pool, freeze_support
from    dataclasses import dataclass


# transform the files
class Transform:
    __slots__ = ('files')

    def __init__(self):#, files):
        # self.files = files
        pass

    def unpack_by_loop(self, files):
        file_count  = len(files)
        list_of_dfs = [0]*file_count 

        for index, file in enumerate(files):
            data    = pd.read_json(file)
            df      = pd.json_normalize(data["entry"])
            list_of_dfs[index] = df

        combined_df = pd.concat(list_of_dfs)
        combined_df = combined_df.reset_index()

        return combined_df


    # Might have trouble scaling this if:
    # -- the number of files is massive
    # -- the content of one or more files is massive
    def unpack_by_map(self, files, workerCount=10):
        with Pool(workerCount) as p:
            dfs = p.map(self._file_unpacker, files)

        dfs = pd.concat(list(dfs))
        dfs = dfs.reset_index(drop=True)

        return dfs


    def _file_unpacker(self, file):
        data    = pd.read_json(file)
        df      = pd.json_normalize(data["entry"])

        return df
        

    def seperate_by_uniqueness(self, df, column):
        unique_items    = df[column].unique()
        unique_count    = len(unique_items)
        df_list         = [0]*unique_count
        row_count       = len(df.index)
        incr_row_count  = 0

        for index, item in enumerate(unique_items):
            # df_list = df[['ItemId', 'ItemDescription']].drop_duplicates().set_index('ItemId')
            item_filter     = df[column]==item
            df_list[index]  = df.where(item_filter, inplace=False)
            # print(df_list[index])
            df_list[index]  = df_list[index].dropna(how='all')
            print(df_list[index])
            incr_row_count += len(df_list[index].index)
        
        print(row_count)
        print(incr_row_count)
        if row_count!=incr_row_count:
            print("data integrity issue")

        output_dict = dict(zip(unique_items, df_list))
        print(output_dict)

        return output_dict


# load the files into a db
class Load:
    def __init__(self, files, db):
        pass


from abc import ABC

class db_interface(ABC):
    def __init__(self):
        pass

    def get():
        pass

    def post():
        pass

    def put():
        pass

    def patch():
        pass

    def delete():
        pass


# class mySQL(database):
class db_mySQL(db_interface):
    def __init__(self, connection):
        pass


from dataclasses import dataclass

# This should really be in a secrets manager or something to keep it secure
@dataclass
class mySQL_connection_details:
    config = {
        'container':    'mysql',
        'host':         'localhost',
        'port':         3306,
        'user':         'root',
        'password':     'mypassword',
        'database':     'emis_test'
    }


import sqlalchemy as db
from sqlalchemy import select, insert
from sqlalchemy.dialects import mysql


class db_connection:
    def __init__(self, connection_details):
        self.config             = mySQL_connection_details().config
        self._connectionString  = self._connectionString()
    

    def _connectionString(self):
        db_container    = self.config.get('container')
        db_user         = self.config.get('user')
        db_pwd          = self.config.get('password')
        db_host         = self.config.get('host')
        db_port         = self.config.get('port')
        db_name         = self.config.get('database')
        connection_str  = f'{db_container}+pymysql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}'
        return connection_str


    def executeGet(self, table, columns="*"):
        engine = db.create_engine(self._connectionString)
        with engine.connect() as connection:
            metadata = db.MetaData(bind=engine)
            metadata.reflect(only=[f'{table}'])

            test_table  = metadata.tables[f'{table}']
            stmt        = select(f"{columns}").select_from(test_table)
            results     = connection.execute(stmt).fetchall()

        return results

    
    def executePost(self, table):
        engine = db.create_engine(self._connectionString)
        with engine.connect() as connection:
            metadata = db.MetaData(bind=engine)
            metadata.reflect(only=[f'{table}'])

            test_table  = metadata.tables[f'{table}']
            # stmt        = select('*').select_from(test_table)
            stmt        = db.insert()
            results     = connection.execute(stmt).fetchall()

        return results



class mySQL_connection:
    def __init__(self, connection_details):
        pass

    def get(self, table):
        conn_details    = mySQL_connection_details()
        db_conn         = db_connection(conn_details)
        return db_conn.executeGet(table)

    def post(db, table, data):
        pass

    def put():
        pass

    def patch():
        pass

    def delete():
        pass


def main():
    raw_files       = Extract(files=1).getFiles()
    # # unpacked_files  = Transform(raw_files).unpack_by_loop()

    file_transformer    = Transform()
    unpacked_files      = file_transformer.unpack_by_map(raw_files)

    # unpacked_files  = Transform(raw_files).unpack_by_map()
    print(unpacked_files)
    print(unpacked_files.columns.tolist())
    print(unpacked_files['resource.resourceType'].unique())

    seperated_files = file_transformer.seperate_by_uniqueness(unpacked_files, 'resource.resourceType')

    # print(unpacked_files)

    # mySQL_connection().run()
    #  mySQL_connection("conn").post("table_name", "data")
    # results = mySQL_connection("connection_details").get("table_name")
    # print(results)


if __name__ == '__main__':
    main()