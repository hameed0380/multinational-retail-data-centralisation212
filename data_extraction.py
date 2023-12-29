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
    
    def read_rds_table(self, table_name):
        '''
        Extracts the data from RDS database and returns it as a DataFrame.
        
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

        Args:
            link: location of the data - stored in a pdf
        Returns:
            df: returns data as a dataframe
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
            response: returns the number of stores
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
            complete_endpoint = f"{endpoint}{store_num}"
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
            s3 address: URL endpoint where the data is retrieved from
        Return:
            extension_retrieved: extracted extension
        '''
        delimiter = "."
        parts = s3_address.split(delimiter) # separate at the full stop
        extension_retrieved = parts[-1] # get everything after full stop
        return extension_retrieved
    
    def extract_url_without(self, s3_address):
        '''
        Take the url and match it to a regex where the //: trailing is kept and split into the bucket and key

        Args:
            s3_address: URL endpoint where the data is retrieved from
        Return:
            bucket: name of the bucket
            key: name of the key
        '''
        # regex pattern to match everything before the second forward slash
        pattern = re.compile(r'^(?:[^/]+://)?([^/]+)/(.*)$')
        # Match pattern of regex
        match_pattern = pattern.match(s3_address)
        bucket = match_pattern.group(1) # contains the bucket section
        key = match_pattern.group(2) # contains the key section
        print(bucket, key)
        return bucket, key

    
    def extract_from_s3(self, s3_address):
        '''
        Method to extract data from an s3 bucket based on the file extension.
        Use regex to extract the bucket and key, and used split to get the extension and return dataframe

        Args:
            s3_adress: URL endpoint where the data is retrieved from
        Return:
            df: extracted data as a dataframe
        '''
        print(f"Downloading file from {s3_address}...")
        # Initialize the S3 client
        s3 = boto3.client('s3')
        # Get the file extension
        file_extension = self.get_file_extension(s3_address)
        # Splitting the S3 address to get the bucket and key
        bucket, key = self.extract_url_without(s3_address=s3_address)
        print(bucket, key)
        # case where the file is a csv
        if file_extension == 'csv':

            # Retrieve CSV File from S3 then reads the content of the object
            response = s3.get_object(Bucket=bucket, Key=key)
            print("File has sucessfully retrieved.")
            content = response['Body'].read()
            # Create a Pandas DataFrame from the CSV data
            print("Reading the CSV file into DataFrame...")
            df = pd.read_csv(io.BytesIO(content))
            return df
        # case where the file is a json
        elif file_extension == 'json':

            # Retrieve JSON File from S3 then reads the content of the object
            response = s3.get_object(Bucket=bucket, Key=key)
            print("File has sucessfully retrieved.")
            content = response['Body'].read()
            print("Reading the JSON file into DataFrame...")
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
    # ['legacy_store_details', 'legacy_users', 'orders_table'] reminder of names
    print(lamb.read_rds_table('legacy_users'))
    k = lamb.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    print(k)
