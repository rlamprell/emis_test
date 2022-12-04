from dataclasses import dataclass
from abc import ABC, abstractmethod
import  sqlalchemy              as db
# from    sqlalchemy.dialects     import mysql
from    sqlalchemy              import select, insert
from    dataclasses             import dataclass
from    abc                     import ABC, abstractmethod
# from    .configs                import db_connection_config



@dataclass
class db_connection_config(ABC):
    config = {
        "abstract-class": "blank"
    }

@dataclass
class mySQL_connection_details():
    # config = {
    #     'container':    'mysql',
    #     'host':         'localhost',
    #     'port':         3306,
    #     'user':         'root',
    #     'password':     'mypassword',
    #     'database':     'emis_test'
    # }
    config = {
        'container':    'mysql',
        'host':         '172.20.0.2',
        'port':         3306,
        'user':         'root',
        'password':     'password',
        'database':     'emis_test_db'
    }


class db_connection(ABC):
    def __init__(self, connection_details):
        self.config = connection_details.config
        self._connectionString = self._connectionString()

    @abstractmethod
    def _connectionString(self):
        pass
    
    @abstractmethod
    def executeGet(self, table, columns):
        pass

    @abstractmethod
    def executePost(self, table_name, df):
        pass


class mysql_conn:
    def __init__(self, connection_details):
        self.config = connection_details.config
        self._connectionString = self._connectionString()


    def _connectionString(self):
        db_container   = self.config.get('container')
        db_user        = self.config.get('user')
        db_pwd         = self.config.get('password')
        db_host        = self.config.get('host')
        db_port        = self.config.get('port')
        db_name        = self.config.get('database')
        connection_str = f'{db_container}+pymysql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}'
        print(connection_str)
        return connection_str


    def executeGet(self, table, columns="*"):
        engine = db.create_engine(self._connectionString)
        with engine.connect() as connection:
            metadata = db.MetaData(bind=engine)
            metadata.reflect(only=[f'{table}'])

            test_table  = metadata.tables[f'{table}']
            stmt        = select(f"{columns}").select_from(test_table)
            results     = connection.execute(stmt).fetchall()

        return results


    def executePost(self, table_name, df):
        engine = db.create_engine(self._connectionString)
        with engine.connect() as connection:
            df.to_sql(table_name, connection, if_exists='replace', index=False)


conn = mysql_conn(mySQL_connection_details())
out = conn.executeGet('Patient')
print(out[0])