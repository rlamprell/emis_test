import pandas as pd
from    multiprocessing import Pool, freeze_support
from    dataclasses import dataclass
from    functools import partial
from    itertools import repeat


from flatten_json import flatten_json
from flatten_json import flatten
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
    def unpack_by_map(self, files, normalise_on_column="entry", workerCount=10):
        with Pool(workerCount) as p:
            dfs = p.map(self._file_unpacker, files)

        dfs = pd.concat(list(dfs))
        dfs = dfs.reset_index(drop=True)

        return dfs


    def _file_unpacker(self, file):
        data    = pd.read_json(file)
        df      = pd.json_normalize(data['entry'], max_level=20)
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
        print(df.columns.tolist())
        data = df.to_dict('records')
        # df = pd.DataFrame([flatten(data)])
        df = pd.DataFrame([flatten(d, ".") for d in data])
        print(df.columns.tolist())
        # print(data)
        # stop

        # # print(f"mydata: {data}")
        # df = pd.DataFrame([flatten_json(data)])    

        # df = pd.DataFrame([flatten_json(x) for x in data])
        # # df = pd.DataFrame([flatten_json(data[key]) for key in data])
        return df
        # return     


    def new_unpack_by_loop(self, files, normalise_on_column="entry", max_recurrsion_depth=20):
        file_count  = len(files)
        list_of_dfs = [0]*file_count 

        for index, file in enumerate(files):
            data    = pd.read_json(file)
            df      = pd.json_normalize(data[normalise_on_column], max_level=max_recurrsion_depth)
            list_of_dfs[index] = df

        combined_df = pd.concat(list_of_dfs)
        combined_df = combined_df.reset_index()

        return combined_df

    # @classmethod
    def exploder(self, df, df_name):
        df = df
        if df_name=='Patient':
            df = df.explode('resource.extension')
            df = df.explode('resource.identifier')
            df = df.explode('resource.address')
            df = df.explode('resource.name')
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
        elif df_name=='CarePlan':
            df = df.explode('resource.category')
        elif df_name=='DiagnosticReport':
            df = df.explode('resource.category')
        
        return df