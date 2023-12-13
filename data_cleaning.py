import pandas as pd
import re
from data_extraction import DataExtractor, DatabaseConnector
from database_utils import DatabaseConnector

class DataCleaning:
    
    def clean_user_data(self, user_df):

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


        print(cleaned_user_df.dtypes)


        return cleaned_user_df

        
    # function to extract numbers
    def get_digits(self, numbers):
        return re.sub(r'\D', '', numbers)


    def clean_card_data(self):
        pass




if __name__ == '__main__':
    RDS_CONNECTOR = DatabaseConnector()
    RDS_CONNECTOR.init_db_engine()
    RDS_CONNECTOR.list_db_tables()

    lamb = DataExtractor()
    # ['legacy_store_details', 'legacy_users', 'orders_table']
    # print(lamb.read_rds_table('legacy_users'))
    cleani = lamb.read_rds_table('legacy_users')



    test_clean = DataCleaning()
    i = test_clean.clean_user_data(cleani)
    RDS_CONNECTOR.upload_to_db(i, 'dim_users')
    k = lamb.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    print(k)
