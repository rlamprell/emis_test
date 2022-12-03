import glob
import os

# import the files
class Extract:
    __slots__ = ('folder_name', 'files')

    def __init__(self, folder_name: str):
        current_path    = os.getcwd()
        self.files      = glob.glob(f"{current_path}\{folder_name}\*.json")


    def getFiles(self)->list:
        return self.files