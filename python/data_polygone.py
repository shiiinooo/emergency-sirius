import os
import requests
import pandas as pd
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the Polygon.io API key and endpoint
API_KEY = '0SkuNuiHXXyJhG777nPPYnYHhaOcQBuM'  # Replace with your actual API key
BASE_URL = 'https://api.polygon.io'

# Function to fetch stock data from Polygon.io (for example)
def fetch_stock_data(symbol='AAPL', start_date='2024-01-01', end_date='2024-10-30'):
    try:
        endpoint = f'{BASE_URL}/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}'
        params = {'apiKey': API_KEY}

        response = requests.get(endpoint, params=params)
        if response.status_code == 200:
            logging.info("Data fetched successfully!")
            data = response.json()
            if 'results' in data:
                results = data['results']
                df = pd.DataFrame(results)
                if 't' in df.columns:
                    df['date'] = pd.to_datetime(df['t'], unit='ms')
                return df
            else:
                logging.error("No results found in the API response.")
                return None
        else:
            logging.error(f"Error fetching data: {response.status_code}, {response.text}")
            return None
    except Exception as e:
        logging.error(f"An error occurred while fetching data: {str(e)}")
        return None
# Function to fetch data from Polygon.io and save it to a CSV file
def fetch_and_save_polygon_data():
    try:
        df = fetch_stock_data(symbol='AAPL', start_date='2024-01-01', end_date='2024-10-30')
        
        if df is not None:
            # Define the output directory and ensure it exists
            output_dir = '/home/airflow/data'
            os.makedirs(output_dir, exist_ok=True)
            
            # Define the output file path
            output_filename = f'{output_dir}/output_stock_data_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
            df.to_csv(output_filename, index=False)
            logging.info(f"Data has been exported successfully to '{output_filename}'")
        else:
            logging.error("Failed to fetch data. No CSV file was generated.")
    except Exception as e:
        logging.error(f"An error occurred while saving the data: {str(e)}")

# Run the script
if __name__ == "__main__":
    fetch_and_save_polygon_data()

