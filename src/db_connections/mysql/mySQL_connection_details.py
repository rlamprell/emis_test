from dataclasses import dataclass

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
