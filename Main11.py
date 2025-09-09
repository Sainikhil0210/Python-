# Processing semi-structured customer data from MongoDB for structured reporting.
# Split, Cleaning
'''MongoDB'''

import pandas as pd 
import configparser
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from pymongo import MongoClient

def get_connection_for_monogdb():
    config = configparser.ConfigParser()
    config.read(r'C:\Users\hampa\Downloads\TEST 2\TEST\config.config')

    URI = config['mongodb']['URI']
    DATABASE = config['mongodb']['DATABASE']

    client = MongoClient(URI)
    db = client[DATABASE]
    return db

def get_sql_engine():
    config = configparser.ConfigParser()
    config.read(r'C:\Users\hampa\Downloads\TEST 2\TEST\config.config')

    UID = quote_plus(config['ssms']['UID'])
    PWD = quote_plus(config['ssms']['PWD'])
    SERVER = config['ssms']['SERVER']
    DATABASE = config['ssms']['DATABASE']
    DRIVER = quote_plus(config['ssms']['DRIVER'])

    connection_string = f'mssql+pyodbc://{UID}:{PWD}@{SERVER}/{DATABASE}?driver={DRIVER}'
    return create_engine(connection_string)

def extract_mongoDB_data():
    db = get_connection_for_monogdb()
    
    collection = db['sales_dimensions 1']
    documents = list(collection.find({}))

    df = pd.json_normalize(documents)
    df['_id'] = df['_id'].astype(str)
    df.columns = df.columns.str.replace(r'\.', '_', regex=True).str.replace(r' ','_',regex=True)
    return df 

def transform_data(df):
    df['start_date'] = pd.to_datetime(df['start_date'])
    df['end_date'] = pd.to_datetime(df['end_date'])

    invalid_dates = df['end_date'] < df['start_date']
    df.loc[invalid_dates, ['end_date']] = df['start_date']
    return df

def load_data_to_sqlserver(tablename , df , sql_engine):
    df.to_sql(name = tablename , con = sql_engine , if_exists = 'replace' , index = False)

def main():
    sql_engine = get_sql_engine()

    df = extract_mongoDB_data()

    df = transform_data(df)
    print(df.head()) 
     
    load_data_to_sqlserver('sales_dimensions' , df , sql_engine)

if __name__ == '__main__':
    main()