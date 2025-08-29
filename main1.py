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
    df = pd.read_csv('us_customer_data 3.csv')
    return df

# --- Your existing insert_data function ---
def insert_data(engine, df, table_name):
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

# --- Main workflow updated with cleaning steps ---
def main():
    engine = get_engine()

    # Extracting the .csv_file which we defined above the main() clause 
    cd = read_csv_data(engine)

    # Step 1: Deduplication
    initial_rows = len(cd)
    cd.drop_duplicates(inplace=True)
   
    cd['name'] = cd['name'].str.replace(' ', '', regex=False)

    # Step 2: Clean 'customer_id' (ensure no nulls and reset index)
    cd = cd[cd["customer_id"].notnull()].reset_index(drop=True)
    

    # Step 3: Cleaning the email
    cd.loc[cd["email"].isna(), "email"] = (cd.loc[cd["email"].isna(), "name"] + "@example.com")
    cd['email'] = cd['email'].str.replace(' ','').str.lower()
   

    # Step 4: Cleaning the unwanted format in the phone
    cd['phone'] = cd['phone'].fillna('9999999999')
    cd['phone'] = cd['phone'].replace(r'[^\d]','',regex=True).str[-10:]
    

    # 4. Inserting the destination table name in SQL Server
    insert_data(engine, cd, 'csv_customer_data' )
    
   

if __name__ == "__main__":
    main()

