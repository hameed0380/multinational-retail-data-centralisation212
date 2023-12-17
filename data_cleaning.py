import pandas as pd
import re
from data_extraction import DataExtractor, DatabaseConnector
from database_utils import DatabaseConnector
class DataCleaning:
    
    def clean_user_data(self, user_df):
        '''
        Function to clean the user data retrieved and perform operations

        Args: 
            user_df: User data dataframe containing the user data
        Returns:
            cleaned_user_df: cleaned user dataframe 
        '''
        # Creating a copy of the dataframe which is best practice
        cleaned_user_df = user_df.copy()
        # cleaned_user_df = cleaned_user_df.drop('index', axis=1)
        # Dropping the null values and dups
        cleaned_user_df = cleaned_user_df.dropna()
        cleaned_user_df = cleaned_user_df.drop_duplicates()
        # Setting columns to the correct format '%Y-%m-%d'
        dates = ['date_of_birth', 'join_date']
        for date in dates: 
            cleaned_user_df[date] = pd.to_datetime(cleaned_user_df[date], format='%Y-%m-%d', errors='coerce').dt.date
        # Get all the digits and remove everything else
        cleaned_user_df['phone_number'] = cleaned_user_df['phone_number'].apply(self.get_digits)
        # Setting columns to the correct type as category
        cat_column = ['country', 'country_code']
        for cat in cat_column:
            cleaned_user_df[cat_column] = cleaned_user_df[cat_column].astype('category')
        # print(cleaned_user_df.dtypes) # checking changes
            
        return cleaned_user_df

        
    def get_digits(self, numbers):
        '''
        function to extract numbers

        Args: 
            numbers: The phone number being stripped
        '''
        return re.sub(r'\D', '', numbers)


    def clean_card_data(self, card_df):
        '''
        Function to clean the user card retrieved and perform operations
        
        Args: 
            card_df: card data dataframe containing the data about card
        Returns:
            cleaned_card_df: cleaned card dataframe 
        '''
        # Creating a copy of the dataframe which is best practice
        cleaned_card_df = card_df.copy()
        # Dropping the null values and dups
        cleaned_card_df = cleaned_card_df.dropna()

        # change to a string so I can perform operations
        cleaned_card_df['card_number'] = cleaned_card_df['card_number'].astype(str)
        cleaned_card_df['card_number'] = cleaned_card_df['card_number'].apply(self.get_digits)

        #print(cleaned_card_df.loc[cleaned_card_df['card_number'] == '3554954842403828'])

        cleaned_card_df['date_payment_confirmed'] = pd.to_datetime(cleaned_card_df['date_payment_confirmed'], format='%Y-%m-%d', errors='coerce').dt.date
        cleaned_card_df = cleaned_card_df.dropna(subset=['date_payment_confirmed'])

        cleaned_card_df = cleaned_card_df.drop_duplicates()
        return cleaned_card_df

    def clean_store_data(self, store_df):
        # Creating a copy of the dataframe which is best practice
        cleaned_store_df = store_df.copy()
        cleaned_store_df = cleaned_store_df.drop_duplicates()
        # Dropping the column `lat` as it carries all [null] values.
        cleaned_store_df.drop('lat', axis=1, inplace=True)
        # convert to correct d tyoe
        cleaned_store_df['longitude'] = pd.to_numeric(cleaned_store_df['longitude'], errors='coerce')
        cleaned_store_df['latitude'] = pd.to_numeric(cleaned_store_df['latitude'], errors='coerce')
        # change to make address on the same line
        cleaned_store_df['address'] = cleaned_store_df['address'].replace('\n',', ',regex=True)
        # Drop rows where 'country_code' is not 'GB', 'US', or 'DE' and convert to categories
        cleaned_store_df = cleaned_store_df.drop(cleaned_store_df[~cleaned_store_df['country_code'].isin(['GB', 'US', 'DE'])].index)
        cleaned_store_df['country_code'] = cleaned_store_df['country_code'].astype('category')
        # Remove ee from continent
        cleaned_store_df['continent'] = cleaned_store_df['continent'].str.replace('ee', '', regex=False)
        # Getting digits only for staff numbers
        cleaned_store_df['staff_numbers'] = cleaned_store_df['staff_numbers'].astype(str)
        cleaned_store_df['staff_numbers'] = cleaned_store_df['staff_numbers'].apply(self.get_digits)
        # Changing datatype of a column to the correct datatype.
        cleaned_store_df['longitude'] = cleaned_store_df['longitude'].astype(float)
        cleaned_store_df['latitude'] = cleaned_store_df['latitude'].astype(float)
        cleaned_store_df['staff_numbers'] = cleaned_store_df['staff_numbers'].astype('int32')
        cleaned_store_df['store_type'] = cleaned_store_df['store_type'].astype('category')
        cleaned_store_df['country_code'] = cleaned_store_df['country_code'].astype('category')
        # Setting opening_date column to be the correct datatype.
        cleaned_store_df['opening_date'] = pd.to_datetime(cleaned_store_df['opening_date'], format='%Y-%m-%d', errors='coerce').dt.date
        cleaned_store_df = cleaned_store_df.dropna(subset=['opening_date'])

        print(cleaned_store_df)
        print(cleaned_store_df.dtypes) # checking changes
        return cleaned_store_df
        



if __name__ == '__main__':
    RDS_CONNECTOR = DatabaseConnector()
    RDS_CONNECTOR.init_db_engine()
    RDS_CONNECTOR.list_db_tables()

    lamb = DataExtractor()
    # ['legacy_store_details', 'legacy_users', 'orders_table']
    # print(lamb.read_rds_table('legacy_users'))
    cleaned_user = lamb.read_rds_table('legacy_users')



    test_clean = DataCleaning()
    i = test_clean.clean_user_data(cleaned_user)
    #RDS_CONNECTOR.upload_to_db(i, 'dim_users')
    retrieved_data = lamb.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    #print(retrieved_data)
    cleaned = test_clean.clean_card_data(retrieved_data)
    RDS_CONNECTOR.upload_to_db(cleaned, 'dim_store_details')

    api_key = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    
    headers = {'x-api-key': api_key}

    number_of_stores = lamb.list_number_of_stores(endpoint, headers)

    other = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
    stores_data = lamb.retrieve_stores_data(other, headers, number_of_stores)
    cleaned1 = test_clean.clean_store_data(stores_data)
    #print()