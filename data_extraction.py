import pandas as pd
from database_utils import DatabaseConnector
import tabula
import boto3
import requests


class DataExtractor:

    def __init__(self) -> None:
        self.dbconn = DatabaseConnector()
        self.headers = {'x-api-key': 'ayFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMXi_key'}
    
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
    
    def list_number_of_stores(self, endpoint, headers):
        '''
        The function `retrieve_stores_data` retrieves data from a specified URL endpoint for a given number 
        of stores and returns a concatenated dataframe of all the retrieved data.

        Args:
            endpoint: URL endpoint where the store data is retrieved from
            headers: A dictionary that contains the headers to be included in the HTTP request
        Returns:
            DataFrame: returns the number of stores
        '''
        # Make a GET request to the specified API endpoint with the provided headers
        response = requests.get(endpoint, headers=headers)
        # Parse the JSON content of the response
        response_data = response.json()
        # Print the full API response for debugging
        print(f"Status Code: {response.status_code}")
        print("API Response:", response_data)
        return response.json()['number_stores']
    
    def retrieve_stores_data(self, endpoint, headers, num_stores):
        '''
        Function retrieves data from specified URL endpoint for a given number 
        of stores and returns a concatenated dataframe of all the retrieved data.

        Args:
            endpoint: URL endpoint where the store data is retrieved from
            headers: A dictionary that contains the headers to be included in the HTTP request
            num_stores: Total number of stores you want to retrieve data for
        Returns:
            full_df: returns the data we queried as a dataframe
        '''
        combined_df = []
        for store_num in range(0, num_stores):
            complete_endpoint = endpoint + str(store_num)
            # Make a GET request to the specified API endpoint with the provided headers
            response = requests.get(complete_endpoint, headers=headers)
            if response.status_code == 200:
                data = response.json()
                # add data to df (each row of data)
                combined_df.append(pd.DataFrame([data]))
            else:
                # If fail print status code
                print(f"Status Code: {response.status_code}") 
        full_df = pd.concat(combined_df, ignore_index=True)

        return full_df

    def extract_from_s3(self):
        pass
    

if __name__ == '__main__':
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
