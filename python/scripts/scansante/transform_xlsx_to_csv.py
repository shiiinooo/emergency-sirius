import os
import pandas as pd

# Function to transform XLSX to CSV using the second row as column names
def transform_xlsx_to_csv(source_dir, target_dir):
    # Ensure the target directory exists
    os.makedirs(target_dir, exist_ok=True)

    # Loop through each xlsx file in the source directory and convert to CSV
    for file_name in os.listdir(source_dir):
        if file_name.endswith('.xlsx'):
            # Construct full file path
            xlsx_path = os.path.join(source_dir, file_name)
            csv_file_name = file_name.replace('.xlsx', '.csv')
            csv_path = os.path.join(target_dir, csv_file_name)

            try:
                # Read the xlsx file, using the second row (index 1) as the header
                df = pd.read_excel(xlsx_path, header=1)  # Use the second row as header (index 1)

                # Drop completely empty rows if any
                df = df.dropna(how='all')

                # Optionally clean unwanted characters in columns (e.g., `_x001A_` or any other unwanted chars)
                df = df.replace({r'_x[0-9A-Fa-f]+_': ''}, regex=True)

                # Save the cleaned DataFrame as a CSV
                df.to_csv(csv_path, index=False, encoding="utf-8")
                print(f"Converted {xlsx_path} to {csv_path}")
            except Exception as e:
                print(f"Error processing {file_name}: {e}")

    return "Transformation completed"

# Task wrapper
def transform_xlsx_to_csv_task():
    source_dir = '/home/airflow/data/bronze/scansante/'
    target_dir = '/home/airflow/data/bronze/scansante/'

    # Call the function to perform the transformation
    transform_xlsx_to_csv(source_dir, target_dir)
    return "Transformation from XLSX to CSV completed"


if __name__ == "__main__":
    transform_xlsx_to_csv_task()