import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd

class DatabaseConnector:
    
    def __init__(self, credential) -> None:
        self.loaded_creds = self.read_db_creds(credential)
        self.engine = self.init_db_engine()


    def read_db_creds(self,credential):
        '''Load credentials file to access the database
        
        Args:
            credential: path of the credentials file to connect to the database

        Returns:
            credential: returns a dict containing the credentials to
            connect to the database.
        '''
        with open(credential, 'r') as file:
            credential = yaml.safe_load(file)
        #print(credential)
        return credential
    
    def init_db_engine(self):
        '''initialises a SQLAlchemy engine from the credentials provided from class'''
        creds = self.loaded_creds

        DATABASE_TYPE = 'postgresql'
        HOST = creds["RDS_HOST"]
        PASSWORD = creds["RDS_PASSWORD"]
        USER = creds["RDS_USER"]
        DATABASE = creds["RDS_DATABASE"]
        PORT = 5432

        engine = create_engine(f"{DATABASE_TYPE}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        return engine
    
    def list_db_tables(self):
        '''Retrieves the names of the tables'''
        inspector = inspect(self.engine)
        result = inspector.get_table_names()
        print(result)
        return result
    
if __name__ == '__main__':
    RDS_CONNECTOR = DatabaseConnector('db_creds.yaml')
    RDS_CONNECTOR.init_db_engine()
    RDS_CONNECTOR.list_db_tables()