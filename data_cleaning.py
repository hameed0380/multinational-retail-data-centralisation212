import pandas as pd
import re
from data_extraction import DataExtractor
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
        # Remove null firstnames
        cleaned_user_df = cleaned_user_df.loc[cleaned_user_df.first_name != 'NULL']
        # Setting columns to the correct format '%Y-%m-%d'
        dates = ['date_of_birth', 'join_date']
        for date in dates: 
            cleaned_user_df[date] = pd.to_datetime(cleaned_user_df[date], format='%Y-%m-%d', errors='coerce').dt.date
        # Get all the digits and remove everything else
        cleaned_user_df['phone_number'] = cleaned_user_df['phone_number'].apply(self.get_digits)
        cleaned_user_df = cleaned_user_df.dropna(subset=['phone_number'])
        # Make sure GB is the correct country_code
        cleaned_user_df['country_code'] = cleaned_user_df['country_code'].replace('GGB','GB', regex=True)
        # Setting columns to the correct type as category
        cat_column = ['country', 'country_code']
        for cat in cat_column:
            cleaned_user_df[cat_column] = cleaned_user_df[cat_column].astype('category')
        # print(cleaned_user_df.dtypes) # checking changes
        # Drop rows with wrong country codes
        incorrect_country_codes = ~cleaned_user_df['country_code'].isin(['US','GB','DE'])
        cleaned_user_df = cleaned_user_df[~incorrect_country_codes]

        print(len(cleaned_user_df))

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
        cleaned_card_df = cleaned_card_df.drop_duplicates()
        # change to a string so I can perform operations
        cleaned_card_df['card_number'] = cleaned_card_df['card_number'].astype(str)
        cleaned_card_df['card_number'] = cleaned_card_df['card_number'].apply(self.get_digits)
        # Convert to datetime d type
        cleaned_card_df['date_payment_confirmed'] = pd.to_datetime(cleaned_card_df['date_payment_confirmed'], format='%Y-%m-%d', errors='coerce').dt.date
        #cleaned_card_df = cleaned_card_df.dropna(subset=['date_payment_confirmed'])

        print(len(cleaned_card_df))
        return cleaned_card_df

    def clean_store_data(self, store_df):
        '''
        Function to clean the store data retrieved and perform operations
        
        Args: 
            store_df: store data dataframe containing the data about store
        Returns:
            cleaned_store_df: cleaned card dataframe 
        '''
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
        #cleaned_store_df = cleaned_store_df.dropna(subset=['opening_date'])

        print(cleaned_store_df)
        print(cleaned_store_df.dtypes) # checking changes
        return cleaned_store_df
    
    def convert_product_weights(self, weight):
        '''
        Converts product weight based on a dictionary 

        Args: 
            weight: The product weights being converted
        return:
            weight_change: converted weight
        '''
        # conversion ratio from dictionary key to kg - approximate oz
        conversion_dict = {'kg': 1, 'g': 0.001, 'ml': 0.001, 'oz': 0.035}

        # regex to iterate over rows and remove anything that isn't a letter from the end of the string
        weight = re.sub(r'[^a-zA-Z]+$', '', weight)

        for unit, factor in conversion_dict.items():
            if unit in weight:
                # check the current format
                if 'x' in weight:
                    # Remove the unit and evaluate expression
                    expression = weight.replace(unit, '').strip().replace('x', '*')
                    weight_change = eval(expression) * factor
                else:
                    weight = float(weight.strip(unit))
                    weight_change = weight * factor
                    return weight_change
        # No matching unit is found
        return None

    def clean_products_data(self, product_df):
        '''
        Function to clean the product dataframe retrieved and perform operations
        
        Args: 
            product_df: product data dataframe containing the data about product
        Returns:
            cleaned_products_df: cleaned product dataframe 
        '''
        # Creating a copy of the dataframe which is best practice
        cleaned_products_df = product_df.copy()
        # Dropping the null values and dups
        cleaned_products_df = cleaned_products_df.drop_duplicates().dropna()
        # Standardise dates
        cleaned_products_df['date_added'] = pd.to_datetime(cleaned_products_df['date_added'], format='%Y-%m-%d', errors='coerce').dt.date
        # Remove £ from the rows in the column
        cleaned_products_df['product_price'] = cleaned_products_df['product_price'].str.replace('£', '', regex=False)
        # Acts like a mask to get all the letters in the product price, then filters the DataFrame by selecting only the rows where the corresponding value in incorrect_rows_with_invalid_prices is False
        incorrect_rows_with_invalid_prices = cleaned_products_df['product_price'].str.contains(r'[a-zA-Z]+')
        cleaned_products_df = cleaned_products_df[~incorrect_rows_with_invalid_prices]
        # Cleaning the weight column to convert to kg & convert to dtype `float`
        cleaned_products_df['weight'] = cleaned_products_df['weight'].apply(self.convert_product_weights)
        # Convert to numeric and dropna 
        cleaned_products_df['weight'] = pd.to_numeric(cleaned_products_df['weight'], errors='coerce')
        cleaned_products_df = cleaned_products_df.dropna(subset=['weight'])
        # Converting columns to correct dtypes.
        cleaned_products_df['category'] = cleaned_products_df['category'].astype('category')
        cleaned_products_df['removed'] = cleaned_products_df['removed'].astype('category')
        cleaned_products_df['product_price'] = cleaned_products_df['product_price'].astype(float)
        cleaned_products_df['weight'] = cleaned_products_df['weight'].astype(float)

        return cleaned_products_df
    
    def clean_orders_data(self, orders_df):
        '''
        Function to clean the order dataframe retrieved and perform operations
        
        Args: 
            orders_df: order data dataframe containing the data about order
        Returns:
            cleaned_orders_df: cleaned order dataframe 
        '''
        # Creating a copy of the dataframe which is best practice
        cleaned_orders_df = orders_df.copy()
        # Dropping columns that are unnecessary or have the majority of rows with NULL in them.
        cleaned_orders_df = cleaned_orders_df.drop(['level_0', '1', 'first_name', 'last_name'], axis=1)
        # change to a string so I can perform operations
        cleaned_orders_df['card_number'] = cleaned_orders_df['card_number'].astype(str)
        cleaned_orders_df['card_number'] = cleaned_orders_df['card_number'].apply(self.get_digits)
        # Converting columns to correct dtypes.
        cleaned_orders_df['product_quantity'] = cleaned_orders_df['product_quantity'].astype('int32')
        # Remove dupes
        cleaned_orders_df = cleaned_orders_df.drop_duplicates()

        return cleaned_orders_df
    
    def clean_date_data(self, date_df):
        '''
        Function to clean the date_time dataframe retrieved and perform operations
        
        Args: 
            date_df: date_time data dataframe containing the data about date
        Returns:
            date_df: cleaned date_time dataframe 
        '''
        # Creating a copy of the dataframe which is best practice
        date_df = date_df.copy()
        # Drop dupes
        date_df = date_df.drop_duplicates()
        # change timestamp
        date_df['timestamp'] = pd.to_datetime(date_df['timestamp'], format='%H:%M:%S',errors='coerce')
        date_df = date_df.dropna(subset=['timestamp'])
        # Extract the time component only
        date_df['timestamp'] = date_df['timestamp'].dt.time
        return date_df



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