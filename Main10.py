#Extracting customer profiles from a MongoDB database for analytics.
# Extraction, Mapping
'''MangoDB'''

import pandas as pd 
import configparser
from sqlalchemy import create_engine
from pymongo import MongoClient
from urllib.parse import quote_plus

def get_connection_for_mongodb():
    config =configparser.ConfigParser()
    config.read(r'C:\Users\hampa\Downloads\TEST 2\TEST\config.config')

    URI = config['mongodb']['URI']
    DATABASE = config['mongodb']['DATABASE']

    client = MongoClient(URI)
    db =client[DATABASE]
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

def extract_mongodb_data():
    db = get_connection_for_mongodb()

    collection = db['product_dimension 1']
    documents = list(collection.find())
    
    df =  pd.json_normalize(documents)
    df['_id'] = df['_id'].astype(str)
    df.columns = df.columns.str.replace(' ', '_').str.replace('.','_')
    return df 

def transform_data(df):
    categry_id = {'Accessories' : '1',
                  'Appliances' : '2',
                  'Books' : '3',
                  'Clothing' : '4',
                  'Electronics' : '5',
                  'Furniture' : '6',
                  'Toys' : '7'}
    
    df['categry_id'] = df['category'].map(categry_id) # Mapping the category_id with the category 
    return df 

def load_to_sqlserver(tablename,df,sql_engine):
    df.to_sql(name = tablename , con = sql_engine , if_exists = 'replace' , index = False)


def main():
    sql_engine = get_sql_engine()  # SQL engine storing into dataframe 
    
    df = extract_mongodb_data() # extracting monogdb files 

    df = transform_data(df)     # Calling the function trasnform_data
    print("DataFrame columns:", df.columns.tolist())  
    print(df.head())
    
    load_to_sqlserver('product_dimension',df,sql_engine) # load the data into the sql server 


if __name__ == '__main__':
    main()