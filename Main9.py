#Tracking historical changes to customer attributes in a MySQL database for audit purposes.
#SDC Type-II ,mapping

import pandas as pd 
from sqlalchemy import create_engine
from  urllib.parse import quote_plus
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

    connection_string = f'mssql+pyodbc://{UID}:{PWD}@{SERVER}/{DATABASE}?DRIVER={DRIVER}'
    return create_engine(connection_string) 

def extract_mysql_data(mysql_engine):
    df = pd.read_sql('select * from mysql_orders' , con=mysql_engine)
    return df

def transform_data(df):
    priority = {'Credit Card' : 'Very High',
                'Debit Card' : 'High',
                'PayPal' : 'Medium',
                'Cash' : 'Low'
                }
    df['priority'] = df['payment_method'].map(priority)
   
    def categorize(amount):
        if amount > 2500 :
            return 'High'
        elif amount > 1500 :
            return 'Medium'
        else:
            return 'Low'
    df['spendings'] = df['amount'].apply(categorize)
    return df



def load_data_to_sql(sql_engine,df,tablename):
    df.to_sql(name = tablename , con = sql_engine , if_exists = 'replace' , index = False)

def main():
    mysql_engine = get_mysql_engine()
    sql_engine = get_sql_engine()

    df = extract_mysql_data(mysql_engine)

    df = transform_data(df)
    print(df.head())

    load_data_to_sql(sql_engine , df , 'mysql_sdcmapped')

if __name__ == '__main__':
    main()