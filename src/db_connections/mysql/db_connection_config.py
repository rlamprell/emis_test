from dataclasses    import dataclass
from abc            import ABC

@dataclass
class db_connection_config(ABC):
    config = {
        "abstract-class": "blank"
    }


@dataclass
class mySQL_connection_details(db_connection_config):
    config = {
        'container':    'mysql',
        'host':         'localhost',
        'port':         3306,
        'user':         'root',
        'password':     'mypassword',
        'database':     'emis_test'
    }