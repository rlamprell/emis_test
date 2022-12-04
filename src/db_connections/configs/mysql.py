from dataclasses            import dataclass
from .db_connection_config  import db_connection_config


@dataclass
class mySQL_connection_details(db_connection_config):
    config = {
        'container':    'mysql',
        'host':         '10.5.0.5',
        'port':         3306,
        'user':         'root',
        'password':     'password',
        'database':     'emis_test_db'
    }