import pandas as pd

import glob
import os

class File:
    def __init__(self, file_name):
        self.file_name = file_name
        self.file_path = file_path



# import the files
class Extract:
    def __init__(self, files):
        current_path    = os.getcwd()
        self.files      = glob.glob(f"{current_path}\data\*.json")

    def getFiles(self):
        return self.files


from    multiprocessing import Pool, freeze_support
from    dataclasses import dataclass


# transform the files
class Transform:
    def __init__(self, files):
        self.files = files

    def unpack(self):
        file_count  = len(self.files)
        list_of_dfs = [0]*file_count 

        for index, file in enumerate(self.files):
            data    = pd.read_json(file)
            df      = pd.json_normalize(data["entry"])
            list_of_dfs[index] = df

        combined_df = pd.concat(list_of_dfs)
        combined_df = combined_df.reset_index()

        return combined_df


    def _file_unpacker(self, file):
        data    = pd.read_json(file)
        df      = pd.json_normalize(data["entry"])

        return df


    def unpack_map(self, workerCount=10):
        with Pool(workerCount) as p:
            dfs = p.map(self._file_unpacker, self.files)

        dfs = pd.concat(list(dfs))
        dfs = dfs.reset_index()

        return dfs


# load the files into a db
class Load:
    def __init__(self, files):
        pass



def main():
    raw_files       = Extract(files=1).getFiles()
    # unpacked_files  = Transform(raw_files).unpack()

    unpacked_files = Transform(raw_files).unpack_map()

    print(unpacked_files)



if __name__ == '__main__':
    main()