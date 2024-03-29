import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd


class DatabaseConnector:
    
    def __init__(self) -> None:
        self.engine = self.init_db_engine()

    def read_db_creds(self):
        '''
        Load credentials file to access the database
        
        Args:
            credential: path of the credentials file to connect to the database

        Returns:
            credential: returns a dict containing the credentials to
            connect to the database.
        '''
        with open("db_creds.yaml", 'r') as file:
            credential = yaml.safe_load(file)
            return credential
    
    def init_db_engine(self):
        '''
        Initialises a SQLAlchemy engine from the credentials provided from class
        
        return:
            engine: the instance of the create_engine class
        '''
        engine = create_engine(f"postgresql://{self.read_db_creds()['RDS_USER']}:{self.read_db_creds()['RDS_PASSWORD']}@{self.read_db_creds()['RDS_HOST']}:{self.read_db_creds()['RDS_PORT']}/{self.read_db_creds()['RDS_DATABASE']}")        
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        return engine
    
    def list_db_tables(self):
        '''
        Retrieves the names of the tables

        return:
            retrieved_names: retrieved name of the tables from db
        '''
        inspector = inspect(self.engine)
        retrieved_names = inspector.get_table_names()
        print(retrieved_names)
        return retrieved_names
    
    def upload_to_db(self, df, tablename):
        '''
        Creates and uploads table to database

        args:
            df: the dataframe to be uploaded to the db
            tablename: establish name of table
        '''
        engine = create_engine(f"postgresql://{self.read_db_creds()['PS_USER']}:{self.read_db_creds()['PS_PASSWORD']}@{self.read_db_creds()['PS_HOST']}:{self.read_db_creds()['PS_PORT']}/{self.read_db_creds()['PS_DATABASE']}")        
        df.to_sql(tablename, engine, if_exists='replace', index=False)
        print("UPLOAD SUCCESSFUL")