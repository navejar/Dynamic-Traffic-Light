import requests
import pandas as pd
import time
import sqlite3
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# API endpoint URL
API_URL = "https://data.cityofchicago.org/resource/sxs8-h27x.json"
LIMIT = 500  # Increased limit for more records per request

def fetch_data(url, limit):
    
    all_data = []
    offset = 0

    while True:
        params = {"$limit": limit, "$offset": offset}
        logging.info(f"Fetching records {offset} to {offset + limit}...")
        
        try:
            response = requests.get(url, params)
            response.raise_for_status()  # Raise an error for bad responses
            
            data_chunk = response.json()
            if not data_chunk:  # No more data
                break
            
            all_data.extend(data_chunk)
            offset += limit
            
            if offset > 900:
              break
            time.sleep(1)  # Optional: Sleep to avoid hitting rate limits

        except requests.exceptions.RequestException as e: 
            logging.error(f"Error during request to {url}: {str(e)}")
            break

    return pd.DataFrame(all_data)

def clean_data(df):
    """Clean the DataFrame by handling missing values."""
    # Check for missing values
    logging.info("Checking for missing values...")
    missing_values = df.isnull().sum()
    logging.info(f"Missing values: {missing_values[missing_values > 0]}")

    # Forward fill missing values
    df.fillna(method='ffill', inplace=True)

    # Convert any dictionary-type columns to strings
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: str(x) if isinstance(x, dict) else x)

    return df

def filter_negative_values(df):
    """Filter out rows with negative values in specified columns."""
    # Specify the columns to check for negative values
    columns_to_check = df.select_dtypes(include=['float64','int']).columns

    # Create a mask for rows without negative values in the specified columns
    mask = (df[columns_to_check] >= 0).all(axis=1)

    # Filter the DataFrame
    filtered_df = df[(df[columns_to_check] >= 0).all(axis=1)]

    return filtered_df

def store_to_sqlite(df, db_name, table_name):
    #Store data in sql database
    try:
        conn = sqlite3.connect(db_name)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        logging.info(f"Data successfully stored in {db_name} under {table_name}.")
    except sqlite3.Error as e:
        logging.error(f"Database error: {str(e)}")
    finally:
        conn.close()

if __name__ == "__main__":
    df = fetch_data(API_URL, LIMIT)
    
    if not df.empty:
        logging.info("Data extracted successfully!")
        
        # Filter out rows with negative values
        filtered_df = filter_negative_values(clean_data(df))

        logging.info(f"Filtered DataFrame shape: {filtered_df.shape}")
        store_to_sqlite(filtered_df, 'traffic_data.db', 'traffic_table') #store in database
    else:
        logging.warning("No data fetched")

    # Display the filtered DataFrame
    print(filtered_df)
