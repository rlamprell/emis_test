
from dataclasses import dataclass

# This should really be in a secrets manager or something to keep it secure
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


import sqlalchemy as db
from sqlalchemy import select, insert
from sqlalchemy.dialects import mysql


class db_connection:
    def __init__(self, connection_details):
        self.config             = mySQL_connection_details().config
        self._connectionString  = self._connectionString()
    

    def _connectionString(self):
        db_container    = self.config.get('container')
        db_user         = self.config.get('user')
        db_pwd          = self.config.get('password')
        db_host         = self.config.get('host')
        db_port         = self.config.get('port')
        db_name         = self.config.get('database')
        connection_str  = f'{db_container}+pymysql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}'
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


    # def createTable(self, table_name):
    #     engine = db.create_engine(self._connectionString)
    #     with engine.connect() as connection:
    #         pass

    #     return results


# NO DEFENSE AGAINST SHADOW READS AND CONCURENCY ISSUES
class mySQL_connection:
    def __init__(self):#, connection_details: mySQL_connection_details):
        # Coupling, but obsfucating the connection info
        self.connection_details = mySQL_connection_details()

    def get(self, table):
        # conn_details = mySQL_connection_details()
        db_conn = db_connection(self.connection_details)
        return db_conn.executeGet(table)

    def post(self, table, data):
        # conn_details = mySQL_connection_details()
        # db_conn = db_connection(conn_details)
        # self.connection_details.executePost(table, data)
        db_conn = db_connection(self.connection_details)
        return db_conn.executeGet(table)


    def put():
        pass

    def patch():
        pass

    def delete():
        pass