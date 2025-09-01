# Extracting customer information from a SQL Server database for reporting purposes. (Extracting , Mapping )
# 🔄 Data Transformation Requirements
#1. Name Processing
#Split the Name column into two separate fields: (First Name,Last Name) Use the space as a separator for splitting.
#Additional Processing:
#Remove common prefixes: Examples: Mr., Mrs., Miss, Dr.
#Remove common suffixes: Examples: Jr., Sr., II, III



import pandas as pd
from sqlalchemy import create_engine
import configparser
from sqlalchemy.engine import URL

# --- Create the SQLAlchemy engine ---
def get_engine():
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
        query={"driver": DRIVER.strip('{}')}
    )

    print(f"Connecting to: {SERVER}/{DATABASE}")
    engine = create_engine(connection_url)
    return engine

# --- Read both CSVs ---
def read_csv_data():
    customers_cd = pd.read_csv('us_customer_data 3.csv')
    name_split = customers_cd['name'].str.split(' ', n=1, expand=True)
    customers_cd['first_name'] = name_split[0].str.strip() # Strip any extra spaces
    customers_cd['last_name'] = name_split[1].str.strip().fillna('') # Handle cases with only a first name and strip
    # Drop the original 'full_name' column
    customers_cd.drop(columns=['name'], inplace=True)

    orders_cd = pd.read_csv('trans_data.csv')
    # Rename columns including transaction_id → order_id
    orders_cd = orders_cd.rename(columns={'transaction_id': 'order_id', 'transaction_date': 'order_date'})
    return customers_cd, orders_cd


# --- Insert into SQL Server ---
def insert_data(engine, cd, table_name):
    cd.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    print(f"Data inserted into SQL Server table: {table_name}")

# --- Main workflow ---
def main():
    engine = get_engine()

    # Read both CSV files
    customers_cd, orders_cd = read_csv_data()

    # Insert both DataFrames into SQL Server
    insert_data(engine, customers_cd, 'csv_customers')
    insert_data(engine, orders_cd, 'csv_orders')

if __name__ == "__main__":
    main()
