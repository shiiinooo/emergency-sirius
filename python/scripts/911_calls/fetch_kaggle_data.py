import os
import pandas as pd
from datetime import datetime
import logging
import kaggle

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kaggle dataset details
KAGGLE_DATASET = "ahmadrafiee/911-calls-for-service-metadata-1-million-record"
OUTPUT_DIR = "/home/airflow/data/bronze/911_calls"

def clear_output_dir(output_dir):
    """Remove all files in the specified directory."""
    try:
        if os.path.exists(output_dir):
            for file in os.listdir(output_dir):
                file_path = os.path.join(output_dir, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            logging.info(f"Cleared all files in {output_dir}")
    except Exception as e:
        logging.error(f"Error while clearing directory: {str(e)}")

def fetch_kaggle_data():
    try:
        # Ensure the output directory exists and clear its contents
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        clear_output_dir(OUTPUT_DIR)

        # Download the dataset using the Kaggle API
        logging.info("Downloading dataset from Kaggle...")
        kaggle.api.dataset_download_files(KAGGLE_DATASET, path=OUTPUT_DIR, unzip=True)
        logging.info("Dataset downloaded and extracted successfully!")

        # Load the dataset into a DataFrame
        csv_file = os.path.join(OUTPUT_DIR, "911_Calls_for_Service.csv")
        df = pd.read_csv(csv_file, delimiter='\t')

        # Save the processed data with a fixed filename
        output_filename = os.path.join(OUTPUT_DIR, "raw_911_calls.csv")
        df.to_csv(output_filename, index=False)
        logging.info(f"Raw data saved to {output_filename}")

        # Remove the original Kaggle dataset file
        os.remove(csv_file)
        logging.info(f"Removed the original file: {csv_file}")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

# Run the script
if __name__ == "__main__":
    fetch_kaggle_data()