import glob
import os

# import the files
class Extract:
    __slots__ = ('folder_name', 'files')

    def __init__(self, folder_name="test_data"):
        current_path    = os.getcwd()
        # print(current_path)
        self.files      = glob.glob(f"{current_path}\{folder_name}\*.json")

    def getFiles(self):
        return self.files