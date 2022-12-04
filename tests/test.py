import unittest
# from ...src.db_connections         import db_connection
# from ...src.db_connections.configs import db_connection_config
# from extract import Extract
# from db_connections import db_connection
import os

# importing sys

import sys
 
path_above = os.path.abspath(os.curdir)
print(path_above)
# adding Folder_2 to the system path
print("hi")
sys.path.insert(0, f'{path_above}/src/')
from extract import Extract

class TestExtractMethods(unittest.TestCase):

    def test_make_connection_to_db(self):
        # connection = mysql_conn(mySQL_connection_details())
        self.assertIsInstance(obj, cls)

    # # def test_upper(self):
    # #     self.assertEqual('foo'.upper(), 'FOO')

    # # def test_isupper(self):
    # #     self.assertTrue('FOO'.isupper())
    # #     self.assertFalse('Foo'.isupper())

    # def test_split(self):
    #     s = 'hello world'
    #     self.assertEqual(s.split(), ['hello', 'world'])
    #     # check that s.split fails when the separator is not a string
    #     with self.assertRaises(TypeError):
    #         s.split(2)

if __name__ == '__main__':
    unittest.main() 