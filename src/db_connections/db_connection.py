import  sqlalchemy              as db
from    sqlalchemy              import select, insert
from    dataclasses             import dataclass
from    abc                     import ABC, abstractmethod
from    .configs                import db_connection_config


class db_connection(ABC):
    def __init__(self, connection_details: db_connection_config):
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




class mysql_conn(db_connection):
    def __init__(self, connection_details):
        super().__init__(connection_details)


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




class db_actions(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def get(self, table):
        pass

    @abstractmethod
    def post(self, table, data):
        pass

    @abstractmethod
    def put():
        pass
    
    @abstractmethod
    def patch():
        pass

    @abstractmethod
    def delete():
        pass




class mySQL_actions(db_actions):
    def __init__(self):
        self.connection_details = mySQL_connection_details()

    def get(self, table):
        return self.connection_details.executeGet(table)

    def post(self, table, data):
        db_conn = db_connection(self.connection_details)
        return db_conn.executeGet(table)

    def put():
        pass

    def patch():
        pass

    def delete():
        pass




class db_connection(db_connection):
    def __init__(self, connection_details):
        super().__init__(connection_details)


    def _connectionString(self):
        db_container   = self.config.get('container')
        db_user        = self.config.get('user')
        db_pwd         = self.config.get('password')
        db_host        = self.config.get('host')
        db_port        = self.config.get('port')
        db_name        = self.config.get('database')
        connection_str = f'{db_container}+pymysql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}'
        return connection_str


    def executeGet(self, table, columns="*"):
        engine = db.create_engine(self._connectionString)
        with engine.connect() as connection:
            metadata = db.MetaData(bind=engine)
            metadata.reflect(only=[f'{table}'])

            test_table = metadata.tables[f'{table}']
            stmt = select(f"{columns}").select_from(test_table)
            results = connection.execute(stmt).fetchall()

        return results


    def executePost(self, table_name, df):
        engine = db.create_engine(self._connectionString)
        with engine.connect() as connection:
            df.to_sql(table_name, connection, if_exists='replace', index=False)




if __name__ == "__main__":
    connection = mysql_conn(mySQL_connection_details())
    print(connection.executeGet('Patient'))