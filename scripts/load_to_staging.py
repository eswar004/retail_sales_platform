import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

# Load credentials from .env file
load_dotenv()

# Snowflake connection
def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

# Map CSV filenames to Snowflake staging table names
FILE_TABLE_MAPPING = {
    'olist_orders_dataset.csv': 'ORDERS',
    'olist_customers_dataset.csv': 'CUSTOMERS',
    'olist_order_items_dataset.csv': 'ORDER_ITEMS',
    'olist_products_dataset.csv': 'PRODUCTS',
    'olist_sellers_dataset.csv': 'SELLERS',
    'olist_order_payments_dataset.csv': 'PAYMENTS',
    'olist_order_reviews_dataset.csv': 'ORDER_REVIEWS',
    'product_category_name_translation.csv': 'PRODUCT_CATEGORY_TRANSLATION',
}

def load_csv_to_staging(conn, csv_file, table_name):
    data_path = os.path.join('data', 'raw', csv_file)
    
    print(f"Reading {csv_file}...")
    df = pd.read_csv(data_path)
    
    # Convert column names to uppercase (Snowflake standard)
    df.columns = [col.upper() for col in df.columns]
    
    print(f"Loading {len(df)} rows into STAGING.{table_name}...")
    
    success, num_chunks, num_rows, output = write_pandas(
        conn=conn,
        df=df,
        table_name=table_name,
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        overwrite=True  # truncate and reload each time
    )
    
    if success:
        print(f"✅ Successfully loaded {num_rows} rows into {table_name}")
    else:
        print(f"❌ Failed to load {table_name}")

def main():
    print("Connecting to Snowflake...")
    conn = get_snowflake_connection()
    print("✅ Connected successfully\n")
    
    for csv_file, table_name in FILE_TABLE_MAPPING.items():
        load_csv_to_staging(conn, csv_file, table_name)
        print()
    
    conn.close()
    print("All files loaded. Connection closed.")

if __name__ == "__main__":
    main()