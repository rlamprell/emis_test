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



class db_interface:
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


class database:
    def __init__(self):
        pass


class mySQL(database):
    def __init__(self):
        pass


class mySQL_connection():
    def __init__(self):
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
    mySQL_connection().run()


if __name__ == '__main__':
    main()