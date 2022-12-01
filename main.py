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
        # print(current_path)
        self.files      = glob.glob(f"{current_path}\data\*.json")

    def getFiles(self):
        return self.files


from    multiprocessing import Pool, freeze_support
from    dataclasses import dataclass
from    functools import partial
from    itertools import repeat


from flatten_json import flatten_json
# amended from PyPi flatten_json
# from flatten_json_copy import flatten_json


# transform the files
class Transform:
    __slots__ = ('files')

    def __init__(self):#, files):
        # self.files = files
        pass

    def unpack_by_loop(self, files, normalise_on_column="entry", max_recurrsion_depth=20):
        file_count  = len(files)
        list_of_dfs = [0]*file_count 

        for index, file in enumerate(files):
            data    = pd.read_json(file)
            df      = pd.json_normalize(data[normalise_on_column], max_level=max_recurrsion_depth)
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
            # print(df_list[index])
            incr_row_count += len(df_list[index].index)
        
        # print(row_count)
        # print(incr_row_count)
        if row_count!=incr_row_count:
            print("data integrity issue")

        output_dict = dict(zip(unique_items, df_list))
        # print(output_dict)

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
        # print(f"mydata: {data}")
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
        # print(f"df: {type(df)}")
        # print(type(table_name))
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


# NOT SURE IF I NEED THIS OR IF SQLALCH IS SMART ENOUGH TO DO IT ON ITS OWN
# add in any missing columns as blank
def match_target_table_formatting():
    pass

def auto_convert_types(df):
    pass

# def load_all_tables():

# import csv



def exploder(df, df_name):
    df = df
    if df_name=='Patient':
        df = df.explode('resource.extension')
        df = df.explode('resource.identifier')
        df = df.explode('resource.address')
    elif df_name=='Encounter':
        df = df.explode('resource.participant')
    elif df_name=='Claim':
        df = df.explode('resource.item')
    elif df_name=='ExplanationOfBenefit':
        df = df.explode('resource.contained')
        df = df.explode('resource.item')
    elif df_name=='Provenance':
        df = df.explode('resource.target')
        df = df.explode('resource.agent')




    return df



def main():
    raw_files           = Extract(files=1).getFiles()
    # # unpacked_files  = Transform(raw_files).unpack_by_loop()

    file_transformer    = Transform()
    unpacked_files      = file_transformer.unpack_by_map(raw_files)
    seperated_files     = file_transformer.seperate_by_uniqueness_map(unpacked_files, 'resource.resourceType')

    for df in seperated_files:


        # seperated_files[df] = seperated_files[df].convert_dtypes()
        # print(f"\n\n\n\nprinting")
        # print(seperated_files[df].columns.tolist())
        # print(seperated_files[df]['fullUrl'])
        # seperated_files[df] = seperated_files[df].explode('resource.extension')
        # seperated_files[df] = seperated_files[df].explode('resource.identifier')
        # seperated_files[df] = seperated_files[df].explode('resource.address')
        # print(f"\n\n\n\n\n")
        print(df)
        print(seperated_files[df].columns.tolist())
        seperated_files[df] = exploder(seperated_files[df], df)


        seperated_files[df] = file_transformer.flatten_df(seperated_files[df])
        # print(seperated_files[df].columns.tolist())
        # if df=='ExplanationOfBenefit':
        #     stop
        #     seperated_files[df].to_csv('out.csv')    

        mySQL_connection("conn").post(df, seperated_files[df])



        # if df=='Patient':
        #     break



if __name__ == '__main__':
    main()