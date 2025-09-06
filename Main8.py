#Aggregating sales data by customer from a MySQL database for performance tracking.
# Aggregration,Mapping

import pandas as pd 
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import configparser

def get_mysql_engine():
    config = configparser.ConfigParser()
    config.read(r'C:\Users\hampa\Downloads\TEST 2\TEST\config.config')

    UID = quote_plus(config['mysql']['UID'])
    PWD = quote_plus(config['mysql']['PWD'])
    HOST = config['mysql']['HOST']
    DATABASE = config['mysql']['DATABASE']

    connection_string = f'mysql+pymysql://{UID}:{PWD}@{HOST}/{DATABASE}'
    return create_engine(connection_string)

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

def extract_mysqldata(mysql_engine):
    customers_df = pd.read_sql('select * from mysql_customers' , con = mysql_engine)
    orders_df = pd.read_sql('select * from mysql_orders' , con = mysql_engine)
    return customers_df,orders_df

def transform_data(customers_df,orders_df):
    df = pd.merge(customers_df,orders_df,on='customer_id',how='inner')
    df.loc[df['email'].isna(), 'email'] = df['name'].str.replace(' ', '', regex=False).str.lower() + '@example.com'
    df['email'] = df['email'].str.replace(' ','').str.lower()
    df['phone'] = df['phone'].fillna('9999999999')
    df['phone'] = df['phone'].str.split('x').str[0]
    df['phone'] = df['phone'].str.replace(r'[^\d]', '', regex=True).str[-10:]

    df = df.groupby('customer_id')['transaction_id'].count().reset_index()
    df = df.sort_values(by='transaction_id' , ascending = False)
    df = df.rename(columns ={'transaction_id': 'order_id'})
    return(df)

def load_data_to_sql(sql_engine,df,tablename):
    df.to_sql(name = tablename , con = sql_engine , if_exists = 'replace' , index = False)

def main():
    mysql_engine = get_mysql_engine()
    sql_engine = get_sql_engine()

    customers_df , orders_df = extract_mysqldata(mysql_engine)

    df = transform_data(customers_df,orders_df)
    print(df.head())

    load_data_to_sql(sql_engine,df,'mysql_mergerd')    

if __name__ == '__main__':
    main()