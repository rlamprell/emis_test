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

    def __init__(self, files):
        self.files = files

    def unpack_by_loop(self):
        file_count  = len(self.files)
        list_of_dfs = [0]*file_count 

        for index, file in enumerate(self.files):
            data    = pd.read_json(file)
            df      = pd.json_normalize(data["entry"])
            list_of_dfs[index] = df

        combined_df = pd.concat(list_of_dfs)
        combined_df = combined_df.reset_index()

        return combined_df


    def unpack_by_map(self, workerCount=10):
        with Pool(workerCount) as p:
            dfs = p.map(self._file_unpacker, self.files)

        dfs = pd.concat(list(dfs))
        dfs = dfs.reset_index(drop=True)

        return dfs


    def _file_unpacker(self, file):
        data    = pd.read_json(file)
        df      = pd.json_normalize(data["entry"])

        return df
        


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


# class database:
#     def __init__(self):
#         pass

#     def get():
#         pass

#     def post(db, table, data):
#         import sqlalchemy as db
#         from sqlalchemy import select
#         from sqlalchemy.dialects import mysql

#         # specify database configurations
#         config = {
#             'host': 'localhost',
#             'port': 3306,
#             'user': 'root',
#             'password': 'mypassword',
#             'database': 'emis_test'
#         }
#         db_user = config.get('user')
#         db_pwd = config.get('password')
#         db_host = config.get('host')
#         db_port = config.get('port')
#         db_name = config.get('database')
#         # specify connection string
#         connection_str = f'mysql+pymysql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}'
#         # connect to database
#         engine = db.create_engine(connection_str)
#         connection = engine.connect()
#         # pull metadata of a table
#         metadata = db.MetaData(bind=engine)
#         metadata.reflect(only=[r'{table}'])

#         # test_table = metadata.tables['table_name']
#         test_table = metadata.tables[r'{table}']

#         # print(type(test_table))
#         stmt = select('*').select_from(test_table)
#         # print(output)
#         print(stmt.compile(dialect=mysql.dialect(),compile_kwargs={"literal_binds": True}))
#         results = connection.execute(stmt).fetchall()
#         print(results)

#     def put():
#         pass

#     def patch():
#         pass

#     def delete():
#         pass


# class mySQL(database):
class db_mySQL(db_interface):
    def __init__(self, connection):
        pass


from dataclasses import dataclass

@dataclass
class mySQL_connection_details:
    config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'mypassword',
        'database': 'emis_test'
    }


class db_connection:
    def __init__(self, connection_details):
        self.config = mySQL_connection_details()
    
    def _connectionString(self):
        db_user = self.config.get('user')
        db_pwd  = self.config.get('password')
        db_host = self.config.get('host')
        db_port = self.config.get('port')
        db_name = self.config.get('database')
        connection_str = f'mysql+pymysql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}'

    def executeOperation(self):
        engine = db.create_engine(self.connection_str)
        with engine.connect() as connection:
            # pull metadata of a table
            metadata = db.MetaData(bind=engine)
            metadata.reflect(only=[f'{table}'])

            # test_table = metadata.tables['table_name']
            test_table = metadata.tables[f'{table}']

            # print(type(test_table))
            stmt = select('*').select_from(test_table)
            # print(output)
            print(stmt.compile(dialect=mysql.dialect(),compile_kwargs={"literal_binds": True}))
            results = connection.execute(stmt).fetchall()
            print(results)

        return results


    def openConnection(self):
        engine = db.create_engine(connection_str)
        connection = engine.connect()
        
        # engine = create_engine('...')
        # with engine.connect() as conn:
        #     conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS..."))
        # engine.dispose()


    def executeOperation(self):
        pass

    def closeConnection(self):
        pass


class mySQL_connection:
    def __init__(self, connection_details):
        pass

    def get():
        pass

    def post(db, table, data):
        import sqlalchemy as db
        from sqlalchemy import select
        from sqlalchemy.dialects import mysql

        # specify database configurations
        config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': 'mypassword',
            'database': 'emis_test'
        }
        db_user = config.get('user')
        db_pwd  = config.get('password')
        db_host = config.get('host')
        db_port = config.get('port')
        db_name = config.get('database')
        # specify connection string
        connection_str = f'mysql+pymysql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}'
        # connect to database
        engine = db.create_engine(connection_str)
        with engine.connect() as connection:
            # pull metadata of a table
            metadata = db.MetaData(bind=engine)
            metadata.reflect(only=[f'{table}'])

            # test_table = metadata.tables['table_name']
            test_table = metadata.tables[f'{table}']

            # print(type(test_table))
            stmt = select('*').select_from(test_table)
            # print(output)
            print(stmt.compile(dialect=mysql.dialect(),compile_kwargs={"literal_binds": True}))
            results = connection.execute(stmt).fetchall()
            print(results)
        
        # connection = engine.connect()
        # # pull metadata of a table
        # metadata = db.MetaData(bind=engine)
        # metadata.reflect(only=[f'{table}'])

        # # test_table = metadata.tables['table_name']
        # test_table = metadata.tables[f'{table}']

        # # print(type(test_table))
        # stmt = select('*').select_from(test_table)
        # # print(output)
        # print(stmt.compile(dialect=mysql.dialect(),compile_kwargs={"literal_binds": True}))
        # results = connection.execute(stmt).fetchall()
        # print(results)

    def put():
        pass

    def patch():
        pass

    def delete():
        pass


    def run(self):
        import sqlalchemy as db
        from sqlalchemy import select
        from sqlalchemy.dialects import mysql

        # specify database configurations
        config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': 'mypassword',
            'database': 'emis_test'
        }
        db_user = config.get('user')
        db_pwd = config.get('password')
        db_host = config.get('host')
        db_port = config.get('port')
        db_name = config.get('database')
        # specify connection string
        connection_str = f'mysql+pymysql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}'
        # connect to database
        engine = db.create_engine(connection_str)
        connection = engine.connect()
        # pull metadata of a table
        metadata = db.MetaData(bind=engine)
        metadata.reflect(only=['table_name'])

        test_table = metadata.tables['table_name']
        print(type(test_table))
        stmt = select('*').select_from(test_table)
        # print(output)
        print(stmt.compile(dialect=mysql.dialect(),compile_kwargs={"literal_binds": True}))
        results = connection.execute(stmt).fetchall()
        print(results)
        # stmt = select('*').select_from(test_table)
        # result = session.execute(stmt).fetchall()


def main():
    # raw_files       = Extract(files=1).getFiles()
    # # unpacked_files  = Transform(raw_files).unpack_by_loop()
    # unpacked_files  = Transform(raw_files).unpack_by_map()

    # print(unpacked_files)
    # mySQL_connection().run()
     mySQL_connection("conn").post("table_name", "data")



if __name__ == '__main__':
    main()