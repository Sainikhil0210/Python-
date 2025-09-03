#3. Filtering high-value transactions from a CSV file for targeted promotions.
#Tasks involved: Filtering, Cleaning
#Filtering: Selecting only records that meet specific criteria — in this case, high-value transactions (e.g., transactions above $1000) or mean value.
#Cleaning: Ensuring the transaction data is accurate and usable — removing rows with missing amounts, fixing number formats, etc.
#3🟢 Goal: Identify and use only the most valuable customers or transactions for special promotions, such as VIP discounts or exclusive offers.

import pandas as pd
from sqlalchemy import create_engine, text
import configparser
from sqlalchemy.engine import URL

# --- Your existing get_engine function ---
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

# --- New function to read CSV data ---
def read_csv_data(engine):
    df = pd.read_csv('trans_data.csv')
    return df

# --- Your existing insert_data function ---
def insert_data(engine, df, table_name):
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

# --- Main workflow updated with cleaning steps ---
def main():
    engine = get_engine()

    # Extracting the .csv_file which we defined above the main() clause 
    df = read_csv_data(engine)
    
    df = df.dropna()
    df1= df.groupby('customer_id')['amount'].sum().reset_index()
    df2 = df1[df1['amount'] > 1000]
    df3 =df2['customer_id'].tolist()
    df4 = df[df['customer_id'].isin(df3)]
    
    print(df4)

    # 4. Inserting the destination table name in SQL Server
    insert_data(engine, df4, 'filtered_data' )
    

if __name__ == "__main__":
    main()
