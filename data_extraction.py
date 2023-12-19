import pandas as pd
import tabula
import boto3
import requests
import io
import re
from database_utils import DatabaseConnector


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
        Function makes a GET request to a specified URL endpoint with headers,
        retrieves the response data as JSON, and returns the number of stores from the response data

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
    
    def get_file_extension(self, s3_address):
        '''
        Retrieve anything trailling . in the url to retrieve the files extension rather then hard coding
        Args:
            s3 address: 
        '''
        delimiter = "."
        parts = s3_address.split(delimiter)
        extension_retrieved = parts[-1]
        return extension_retrieved
    
    def extract_url_without(self, s3_address):
        # regex pattern to match everything before the second forward slash
        pattern = re.compile(r'^(?:[^/]+://)?([^/]+)/(.*)$')

        # Match pattern of regex
        match_pattern = pattern.match(s3_address)
        bucket = match_pattern.group(1)
        key = match_pattern.group(2)
        print(bucket, key)
        return bucket, key

    
    def extract_from_s3(self, s3_address):
        print(f"Downloading file from {s3_address}...")
        # Initialize the S3 client
        s3 = boto3.client('s3')

        file_extension = self.get_file_extension(s3_address)
        # Splitting the S3 address to get the bucket and key

        #s3_address 
        bucket, key = self.extract_url_without(s3_address=s3_address)
        print(bucket, key)
        #bucket, key = s3_address.split('/', 1)

        if file_extension == 'csv':
        # # Download the file from S3
        #     with open('products.csv', 'wb') as file:
        #         s3.download_fileobj(bucket, key, file)

        #     print("File has sucessfully downloaded.")
        #     print("Reading the CSV file into DataFrame...")
        #     # Read the CSV file into a DataFrame
        #     df = pd.read_csv('products.csv')

            response = s3.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read()
    
            # Create a Pandas DataFrame from the CSV data
            df = pd.read_csv(io.BytesIO(content))
            return df
        

        elif file_extension == 'json':
            # Download the file from S3
            # with open('products.csv', 'wb') as file:
            #     s3.download_fileobj(bucket, key, file)

            # print("File has sucessfully downloaded.")
            # print("Reading the CSV file into DataFrame...")
            # # Read the CSV file into a DataFrame
            # df = pd.read_csv('products.csv')

            response = s3.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read()
            
            df = pd.read_json(io.BytesIO(content))
            return df
        else:
            print('Print error unsupported format')

        print("DataFrame created ")



    

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
