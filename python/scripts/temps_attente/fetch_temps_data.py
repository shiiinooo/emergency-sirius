import os
import kaggle

# Define the target directory to store datasets
AIRFLOW_DATA_DIR = os.path.expanduser("/home/airflow/data/bronze")

# Ensure the directory exists
os.makedirs(AIRFLOW_DATA_DIR, exist_ok=True)

# List of Kaggle datasets to download
datasets = {
    "nadianassiri/urgences-data": "urgences-data",
}

def download_kaggle_dataset(dataset_name, dataset_folder):
    """Download and extract a Kaggle dataset into a specific folder."""
    dataset_path = os.path.join(AIRFLOW_DATA_DIR, dataset_folder)
    os.makedirs(dataset_path, exist_ok=True)  # Create directory if not exists

    print(f"Downloading {dataset_name} into {dataset_path}...")
    kaggle.api.dataset_download_files(dataset_name, path=dataset_path, unzip=True)
    print(f"✅ {dataset_name} downloaded successfully!")

# Fetch each dataset
for dataset_name, folder_name in datasets.items():
    download_kaggle_dataset(dataset_name, folder_name)

print("🎉 All datasets have been downloaded successfully!")