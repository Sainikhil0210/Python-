# Combining customer and order data from a SQL Server database for a unified customer view. (Joins, Mapping)
# Extract the country code from the Address field.
# Enrichment:
# Map the extracted country code to its respective international phone dialing code.
# Prepend this dialing code to the customer's Mobile Number to form a complete, internationally formatted phone number.

import pandas as pd
from sqlalchemy import create_engine, URL
import configparser
import phonenumbers


def get_sql_engine():
    config = configparser.ConfigParser()
    config.read(r'C:\Users\hampa\Downloads\TEST 2\TEST\config.config')

    UID = config['ssms']['UID']
    PWD = config['ssms']['PWD']
    SERVER = config['ssms']['SERVER']
    DATABASE = config['ssms']['DATABASE']
    DRIVER = config['ssms']['DRIVER']

    connection_url = URL.create(
        "mssql+pyodbc",
        username=UID,
        password=PWD,
        host=SERVER,
        database=DATABASE,
        query={'driver': DRIVER.strip('{}')}
    )

    print(f'Connecting to : {SERVER}/{DATABASE}')
    engine = create_engine(connection_url)
    return engine


def extract_data(engine):
    customers_df = pd.read_sql("select * from csv_customers", con=engine)
    orders_df = pd.read_sql("select * from csv_orders", con=engine)
    return customers_df, orders_df


def transform_data(customers_df, orders_df):
    df = pd.merge(customers_df,orders_df,on='customer_id',how='inner')
    df['phone'] = df['phone'].fillna('9999999999').astype(str)
    df['phone'] = df['phone'].str.split('x').str[0]
    df['phone'] = df['phone'].replace(r'[^\d]', '', regex=True).str[-10:]
 
    def get_dialing_code(region_code):

        code = phonenumbers.country_code_for_region(region_code)
        return f"+{code}"
    
    df['CountryCode'] = df['address'].str.findall(r'\b([A-Z]{2,3})\b').str[-1].fillna('NA')
    df['DialingCode'] = df['CountryCode'].apply(get_dialing_code)
    df['phone'] = df['DialingCode']+ '-' +df['phone']
    
    final_columns = ['customer_id',
        'order_id',
        'first_name',
        'last_name',
        'order_date',
        'email',
        'phone', # This is now the modified, international phone number
        'address',
        'CountryCode', 
        'loyalty_status']
    return df[final_columns]

def load_data(df, engine, table_name='unified_customer_orders'):
    df.to_sql(table_name,con=engine,if_exists='replace',index=False)
    print("Load complete.")


def main():
    engine = get_sql_engine()
    customers_df, orders_df = extract_data(engine)

    transformed_df = transform_data(customers_df, orders_df)

    print("\n--- Transformed Data Preview ---")
    print(transformed_df.head())
    print("\n------------------------------\n")

    load_data(transformed_df, engine)


if __name__ == "__main__":
    main()
