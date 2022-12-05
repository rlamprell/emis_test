import unittest
import os
import sys
import pandas as pd
 
path_above = os.path.abspath(os.curdir)
sys.path.insert(0, f'{path_above}/src/')

from extract    import Extract
from transform  import Transform


# Tests only check the types atm, but should really check the contents too
# -- specifically, the contents of known quantities
class TestExtractMethods(unittest.TestCase):

    def test_extract_returns_list_for_single(self):
        files = Extract(folder_name="/tests/test_data/single_file").getFiles()
        self.assertIsInstance(files, list)


    def test_extract_returns_list_for_multi(self):
        files = Extract(folder_name="/tests/test_data/multi_file").getFiles()
        self.assertIsInstance(files, list)


    # maybe it should break or give some other feedback for these three instances
    def test_extract_returns_list_for_blank(self):
        files = Extract(folder_name="").getFiles()
        self.assertIsInstance(files, list)


    def test_extract_returns_list_for_None(self):
        files = Extract(folder_name=None).getFiles()
        self.assertIsInstance(files, list)


    def test_extract_returns_list_for_number(self):
        files = Extract(folder_name=1).getFiles()
        self.assertIsInstance(files, list)




class TestTransformMethods(unittest.TestCase):

    def test_map_unpack_is_dataframe_single_file(self):
        # should probably have this not rely on another method
        # -- dump the raw json in here instead
        raw_files               = Extract(folder_name="/tests/test_data/single_file").getFiles()
        file_transformer        = Transform()
        list_of_unpacked_dfs    = file_transformer.unpack_by_map(files=raw_files, normalise_on_column='entry')

        print(type(list_of_unpacked_dfs))

        self.assertIsInstance(list_of_unpacked_dfs , pd.core.frame.DataFrame)

    
    def test_map_unpack_is_dataframe_multi_file(self):
        # should probably have this not rely on another method
        # -- dump the raw json in here instead
        raw_files               = Extract(folder_name="/tests/test_data/multi_file").getFiles()
        file_transformer        = Transform()
        list_of_unpacked_dfs    = file_transformer.unpack_by_map(files=raw_files, normalise_on_column='entry')

        print(type(list_of_unpacked_dfs))

        self.assertIsInstance(list_of_unpacked_dfs , pd.core.frame.DataFrame)




if __name__ == '__main__':
    unittest.main() 