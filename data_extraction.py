import pandas as pd
from database_utils import DatabaseConnector


class DataExtractor:

    def __init__(self) -> None:
        self.dbc = DatabaseConnector()
    
    def read_rds_table(self, table_name):
        '''Extracts the data from RDS database and returns it as a DataFrame.
        
        Args:
            table_name: name of the table we want to extract from
        Returns:
            DataFrame: returns the data we queried
        '''
        data = pd.read_sql_table(table_name, self.dbc.engine)
        df = pd.DataFrame(data)
        return df
    
