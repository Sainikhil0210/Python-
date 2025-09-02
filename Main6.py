import pandas as pd
from sqlalchemy import create_engine,text
import configparser
from sqlalchemy.engine import URL

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
    df = pd.read_sql(r'select * from unified_customer_orders',con=engine)
    return df

def load_data(df, engine, table_name='cleaned_customer_orders'):
    df.to_sql(table_name,con=engine,if_exists='replace',index=False)
    

def main():
    engine = get_sql_engine()
    
    df = extract_data(engine)

    tier_map = {'Gold': 2, 'Silver': 1, 'Bronze': 0}
    df['customer_tier'] = df['loyalty_status'].map(tier_map)

    load_data(df, engine)


if __name__ == "__main__":
    main()
