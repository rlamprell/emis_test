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
from    functools import partial
from    itertools import repeat


# from flatten_json import flatten_json
# amended from PyPi flatten_json
from flatten_json_copy import flatten_json
# def flatten_json(nested_json, exclude=['']):
#     """Flatten json object with nested keys into a single level.
#         Args:
#             nested_json: A nested json object.
#             exclude: Keys to exclude from output.
#         Returns:
#             The flattened json object if successful, None otherwise.
#     """
#     out = {}

#     def flatten(x, name='', exclude=exclude):
#         if type(x) is dict:
#             for a in x:
#                 if a not in exclude: flatten(x[a], name + a + '_')
#                 # if a not in exclude: flatten(x[a], name + a + '_')
#         elif type(x) is list:
#             i = 0
#             for a in x:
#                 flatten(a, name + str(i) + '_')
#                 # flatten(a, name + '_')
#                 i += 1
#         else:
#             out[name[:-1]] = x

#     flatten(nested_json)
#     return out



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
            df      = pd.json_normalize(data["entry"], max_level=20)
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
        df      = pd.json_normalize(data["entry"], max_level=20)
        # df = pd.DataFrame([flatten_json(x) for x in data["entry"]])
        # flatten_json.flatten_json()

        return df
        

    def seperate_by_uniqueness_loop(self, df, column):
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
            df_list[index]  = df_list[index].dropna(axis=1)
            print(df_list[index])
            incr_row_count += len(df_list[index].index)
        
        print(row_count)
        print(incr_row_count)
        if row_count!=incr_row_count:
            print("data integrity issue")

        output_dict = dict(zip(unique_items, df_list))
        print(output_dict)

        return output_dict



    # PASSING THE WHOLE ITERABLE EACH TIME IS TOO SLOW
    def seperate_by_uniqueness_map(self, df, column, workerCount=20):
        unique_items    = df[column].unique()
        unique_count    = len(unique_items)
        # df_list         = [0]*unique_count
        row_count       = len(df.index)

        with Pool(workerCount) as p:
            df_list = p.starmap(self._df_seperator, zip(unique_items, repeat(column), repeat(df)))

        # NEED A DI CHECK HERE

        output_dict = dict(zip(unique_items, df_list))
        
        return output_dict

    
    def _df_seperator(self, item, column, df):
        item_filter = df[column]==item
        df          = df.where(item_filter, inplace=False)
        df          = df.dropna(how='all')
        df          = df.dropna(axis=1)
        # df          = self._flatten_df(df)

        return df


    # it's dumb that we go json->df->json->df
    def flatten_df(self, df):
        data = df.to_dict('records')
        print(f"mydata: {data}")
        # df = pd.DataFrame([flatten_json(data)])    

        df = pd.DataFrame([flatten_json(x) for x in data])
        # df = pd.DataFrame([flatten_json(data[key]) for key in data])
        return df
        # return     


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

    
    def executePost(self, table_name, df):
        engine = db.create_engine(self._connectionString)
        print(f"df: {type(df)}")
        print(type(table_name))
        with engine.connect() as connection:
            df.to_sql(table_name, connection, if_exists='replace', index=False)#, dtype='dict')

            # metadata = db.MetaData(bind=engine)
            # metadata.reflect(only=[f'{table}'])

            # test_table  = metadata.tables[f'{table}']
            # # stmt        = select('*').select_from(test_table)
            # stmt        = 
            # results     = connection.execute(stmt).fetchall()

        # return results


    def createTable(self, table_name):
        engine = db.create_engine(self._connectionString)
        with engine.connect() as connection:
            pass


            # meta_data = {}
            # this_table = db.Table(
            #     "this_table",
            #     meta_data,

            # )

            # metadata = db.MetaData(bind=engine)
            # metadata.reflect(only=[f'{table}'])

            # test_table  = metadata.tables[f'{table}']
            # # stmt        = select('*').select_from(test_table)
            # stmt        = db.insert()
            # results     = connection.execute(stmt).fetchall()

        return results


# NO DEFENSE AGAINST SHADOW READS AND CONCURENCY ISSUES
class mySQL_connection:
    def __init__(self, connection_details):
        self.connection_details = connection_details

    def get(self, table):
        conn_details    = mySQL_connection_details()
        db_conn         = db_connection(conn_details)
        return db_conn.executeGet(table)

    def post(self, table, data):
        conn_details    = mySQL_connection_details()
        db_conn         = db_connection(conn_details)
        db_conn.executePost(table, data)
        # return db_conn.executeGet(table)

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

    # seperated_files = file_transformer.seperate_by_uniqueness_loop(unpacked_files, 'resource.resourceType')

    seperated_files = file_transformer.seperate_by_uniqueness_map(unpacked_files, 'resource.resourceType')
    print(seperated_files)
    print(seperated_files['Patient'].info(verbose=True))
    print(seperated_files['Patient'].convert_dtypes().dtypes)
    seperated_files['Patient'] = seperated_files['Patient'].convert_dtypes()
    print(seperated_files['Patient'].info(verbose=True))

    seperated_files['Patient']["fullUrl"] = seperated_files['Patient']["fullUrl"].str.replace("[urn::uuid:]", "")
    print(seperated_files['Patient'])
# resource.extension
    print(seperated_files['Patient']['resource.extension'])

    # unpack      = pd.json_normalize(seperated_files['Patient']['resource.extension'], max_level=20)
    seperated_files['Patient'] = seperated_files['Patient'].explode('resource.extension')
    # print(unpack)
    print(seperated_files['Patient'])
    # df["A"].str.replace("[ab]","")
    # for df in seperated_files:
    #     print(df.columns.tolist())


    # Idx=df.set_index(['COL1','COL2']).COL3.apply(pd.Series).stack().index

    # pd.DataFrame(df.set_index(['COL1','COL2']).COL3.apply(pd.Series).stack().values.tolist(),index=Idx).reset_index().drop('level_2',1)


    seperated_files['Patient'] = pd.DataFrame([x for x in seperated_files['Patient']['resource.extension']])
    print(seperated_files['Patient'])


    # print(seperated_files[0].columns.tolist())
    print(unpacked_files['resource.resourceType'].unique()[0])
    print(seperated_files['Patient'].to_string())


    seperated_files['Patient'] = file_transformer.flatten_df(seperated_files['Patient'])

    print(f"\n\n\n\n\n\n\n\n")
    print("new output")
    print(seperated_files['Patient'])
    print(f"\n\n\n\n\n\n\n\n")



    mySQL_connection("conn").post(unpacked_files['resource.resourceType'].unique()[0], seperated_files['Patient'])

    # print(unpacked_files)

    # mySQL_connection().run()
    #  mySQL_connection("conn").post("table_name", "data")
    # results = mySQL_connection("connection_details").get("table_name")
    # print(results)


if __name__ == '__main__':
    main()