import pandas as pd
from database_utils import DatabaseConnector
import tabula


class DataExtractor:

    def __init__(self) -> None:
        self.dbconn = DatabaseConnector()
    
    def read_rds_table(self, table_name):
        '''Extracts the data from RDS database and returns it as a DataFrame.
        
        Args:
            table_name: name of the table we want to extract from
        Returns:
            DataFrame: returns the data we queried
        '''
        data = pd.read_sql_table(table_name, self.dbconn.engine)
        df = pd.DataFrame(data)
        return df
    
    def retrieve_pdf_data(self, link):
        '''
        Extract all pages from the pdf document
        '''
        dfs = tabula.read_pdf(link, pages='all')
        df = pd.concat(dfs)
        return df
    


RDS_CONNECTOR = DatabaseConnector()
RDS_CONNECTOR.init_db_engine()
data = RDS_CONNECTOR.init_db_engine()
RDS_CONNECTOR.list_db_tables()
data = RDS_CONNECTOR.init_db_engine()

lamb = DataExtractor()
# ['legacy_store_details', 'legacy_users', 'orders_table']
print(lamb.read_rds_table('legacy_users'))
k = lamb.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
print(k)
