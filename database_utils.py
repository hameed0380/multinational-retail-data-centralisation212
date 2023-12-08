import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd

class DatabaseConnector:
    
    def __init__(self, credential) -> None:
        self.loaded_creds = self.read_db_creds(credential)
        self.engine = self.init_db_engine()


    def read_db_creds(self,credential):

        with open(credential, 'r') as file:
            credential = yaml.safe_load(file)
        #print(credential)
        return credential
    
    def init_db_engine(self):
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
        inspector = inspect(self.engine)
        result = inspector.get_table_names()
        print(result)
        return result
    
if __name__ == '__main__':
    RDS_CONNECTOR = DatabaseConnector('db_creds.yaml')
    RDS_CONNECTOR.init_db_engine()
    RDS_CONNECTOR.list_db_tables()