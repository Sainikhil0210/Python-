# Extracting and organizing customer activity data from DynamoDB for analysis.
# Extraction , Sorting 
'''DynamoDB'''

import pandas as pd 
import boto3
import configparser
from sqlalchemy import create_engine
from urllib.parse import quote_plus

def get_connection_for_DynamoDb():
    config = configparser.ConfigParser()
    config.read(r'C:\Users\hampa\Downloads\TEST 2\TEST\config.config')

    aws_access_key_id = config['aws']['aws_access_key_id']
    aws_secret_access_key = config['aws']['aws_secret_access_key']
    region = config['aws']['region']

    dynamodb = boto3.resource('dynamodb',
                              region_name = region,
                              aws_access_key_id = aws_access_key_id,
                              aws_secret_access_key = aws_secret_access_key)
    return dynamodb

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

def extract_data_from_Dynamodb():
    dynamodb = get_connection_for_DynamoDb()
    table = dynamodb.Table('Sales_Dimensions')

    items =[]
    response = table.scan()
    items.extend(response['Items'])
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])

    df = pd.DataFrame(items)
    return df 

def transform_data(df):
    df = df.sort_values(by='supplier_id' , ascending=True)
    final_columns = ['supplier_id',
                     'supplier_name',
                     'contact_email',
                     'supplier_country',
                     'reliability_score',
                     'discount_percentage',
                     'region_id',
                     'region_name',
                     'region_country',
                     'regional_manager',
                     'promotion_id',
                     'promotion_name',
                     'start_date',
                     'end_date']
    return df[final_columns]

def load_data_to_sql(tablename,df,sql_engine):
    df.to_sql(name = tablename , con = sql_engine , if_exists = 'replace' , index = False)


def main():
    sql_engine = get_sql_engine()
    
    df = extract_data_from_Dynamodb()

    df = transform_data(df)

    load_data_to_sql('sales_dimensions_1' , df , sql_engine)
    print(df.head())

if __name__ == '__main__':
    main()