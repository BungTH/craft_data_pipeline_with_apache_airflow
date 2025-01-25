import os
import zipfile
import requests

def fetch_greenery_data_from_kaggle():
    url = "https://www.kaggle.com/api/v1/datasets/download/napatsakornpianchana/greenary-data-for-airflow-class"
    
    reponse = requests.get(url, stream=True)
    
    if reponse.status_code != 200:
        raise Exception("Failed to fetch data from Kaggle, status code: {response.status_code}")
    zip_file_path = os.path.join("./downloads", "greenery_data.zip")
    
    with open(zip_file_path, "wb") as file:
        for chunk in reponse.iter_content(chunk_size=128):
            file.write(chunk)
        
    print(f"Save zip file to {zip_file_path}")
    
def extract_zip_file():
    zip_file_path = os.path.join("./downloads", "greenery_data.zip")
    extract_files_path = os.path.join("./greenery_data")
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(extract_files_path)
    
    print(f"Extracted zip file to {extract_files_path}")
fetch_greenery_data_from_kaggle()
extract_zip_file()