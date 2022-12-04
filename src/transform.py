import  pandas as pd
from    multiprocessing import Pool, freeze_support
from    dataclasses import dataclass
from    functools import partial
from    itertools import repeat
from    flatten_json import flatten



# transform the files
class Transform:
    __slots__ = ('files')

    def __init__(self):
        pass

    def unpack_by_loop(self, files: list, normalise_on_column: str="entry", max_recurrsion_depth: int=20):
        file_count  = len(files)
        list_of_dfs = [0]*file_count 

        for index, file in enumerate(files):
            data    = pd.read_json(file)
            df      = pd.json_normalize(data[normalise_on_column], max_level=max_recurrsion_depth)
            list_of_dfs[index] = df

        combined_df = pd.concat(list_of_dfs)
        combined_df = combined_df.reset_index()

        return combined_df


    def seperate_by_uniqueness_loop(self, df, column):
        unique_items    = df[column].unique()
        unique_count    = len(unique_items)
        df_list         = [0]*unique_count
        row_count       = len(df.index)
        incr_row_count  = 0

        for index, item in enumerate(unique_items):
            item_filter     = df[column]==item
            df_list[index]  = df.where(item_filter, inplace=False)
            df_list[index]  = df_list[index].dropna(how='all')
            df_list[index]  = df_list[index].dropna(axis=1)
            incr_row_count += len(df_list[index].index)
        
        if row_count!=incr_row_count:
            print("data integrity issue")

        output_dict = dict(zip(unique_items, df_list))

        return output_dict


    # Might have trouble scaling this if:
    # -- the number of files is massive
    # -- the content of one or more files is massive
    def unpack_by_map(self, files: list, normalise_on_column: str="entry", workerCount: int=10)->list:
        with Pool(workerCount) as p:
            dfs = p.map(self._file_unpacker, files)

        dfs = pd.concat(list(dfs))
        dfs = dfs.reset_index(drop=True)
        
        return dfs


    def _file_unpacker(self, file: str)->pd.core.frame.DataFrame:
        data    = pd.read_json(file)
        df      = pd.json_normalize(data['entry'], max_level=20)

        return df


    def seperate_by_uniqueness_map(self, df, column: str, workerCount: int=20)->dict:
        unique_items    = df[column].unique()
        unique_count    = len(unique_items)
        row_count       = len(df.index)

        with Pool(workerCount) as p:
            df_list = p.starmap(self._df_seperator, zip(unique_items, repeat(column), repeat(df)))

        output_dict = dict(zip(unique_items, df_list))
        
        return output_dict

    
    def _df_seperator(self, item: str, column: str, df)->pd.core.frame.DataFrame:
        item_filter = df[column]==item
        df          = df.where(item_filter, inplace=False)
        df          = df.dropna(how='all')
        df          = df.dropna(axis=1)

        return df


    # it's dumb that we go json->df->dict->df
    def flatten_df(self, df)->pd.core.frame.DataFrame:
        data    = df.to_dict('records')
        df      = pd.DataFrame([flatten(d, ".") for d in data])

        return df


    def explode_nested_arrays(self, df, df_name, fields_to_explode):
        column_names = df.columns.tolist()
        for this_field in fields_to_explode:
            field_exists_in_df = column_names.count(this_field)
            if field_exists_in_df:
                df = df.explode(this_field)
        
        return df