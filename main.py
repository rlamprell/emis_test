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



# transform the files
class Transform:
    def __init__(self, files):
        self.files = files

    def unpack(self):
        # combined_df = pd.DataFrame()
        file_count  = len(self.files)
        list_of_dfs = [0]*file_count 

        for index, file in enumerate(self.files):
            data    = pd.read_json(file)
            df      = pd.json_normalize(data["entry"])
            # print(list_of_dfs)
            # print(index)?
            list_of_dfs[index] = df

        combined_df = pd.concat(list_of_dfs)
        print(combined_df)
            # combined_df = 
            # combined_df = [combined_df, df]
            # print(file)
            # print(type(file))
            # print(data)
            # print(combined_df)


        return 




# load the files into a db
class Load:
    def __init__(self, files):
        pass



def main():
    raw_files       = Extract(files=1).getFiles()
    unpacked_files  = Transform(raw_files).unpack()



if __name__ == '__main__':
    main()