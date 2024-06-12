import os
import requests
import zipfile
import pandas as pd
import sqlite3

traffic_accidents_url = 'https://storage.googleapis.com/kaggle-data-sets/1581366/2602130/compressed/NYC%20Accidents%202020.csv.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20240612%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240612T211758Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=0d925610ff4efd7f899ec48c61ae12d0542ff21c977f158c782974ecde886a01235a8d3e6fdd0ab5e892cae6e59863c290595d719db775d71254fced349476dde7a9e7d8fcc2a44252396119cb716cef555ec30262167d9305bfdf90abe420111294ca72475e78ca9499b43a8c2c1ac6517d7ba36397873f4584cab0191f1812d5b56b3a756354be24e0bae45af0f07709eb1d2335aef7ea96976696a9767a339039dcc8c20fb39c6dfe26778b7d884de569ee6bf57feb6000c37bb2b5aeb12ade3782ba9f29206e8446b8f131dcad7bafb669eeb49b3b41e53a401672f74bc1d9232374f0a9170fb12b653165ccd2914f4f16180ff0b777b476643285f9e89b'
weather_data_url = 'https://storage.googleapis.com/kaggle-data-sets/2576340/4389349/compressed/NYC_Weather_2016_2022.csv.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20240612%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240612T211931Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=b79f0a9c907e8bc7858db51cc8b8e449b59a19f77a165846356daf7337915079ff0fcac70f095b23807899fc2bed28bdc10655e684699a30a0ecff071c8d9f9d6337fda0f9f51752e75f1f4542aeade370dff2f035b3da4ba2a5e5caa6c54690b3b5d9b79cef6010b93ab8b3f2a399edc35a4459de8b7bfe7a09e0afa7a0eed6ae38ffd1ff636e2adb8ae91662af2747665a2764eea5bdaa4bbe5f5518681f21bf9d441ec8506559940d7ebe3de74536db202bda8c35c006dd60048deabdc02c46007157250140d5f4f7ef2de9b3b9d68214599f8206ac61f1ce803631362ea2e175bbe3bf1ea8e723ed925b600c76cadc94093d39e9995eb754839d0df8f55e'

def download_data(url, filename):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": "https://www.kaggle.com/",
    }
    response = requests.get(url, headers=headers)
    with open(filename, 'wb') as file:
        file.write(response.content)

def extract_zip(file_path, extract_to):
    if zipfile.is_zipfile(file_path):
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
    else:
        raise zipfile.BadZipFile(f"File {file_path} is not a zip file")

def process_data():
    data_dir = './data'
    traffic_zip_path = os.path.join(data_dir, 'nyc_traffic_accidents.zip')
    weather_zip_path = os.path.join(data_dir, 'nyc_weather_data.zip')
    traffic_csv_path = os.path.join(data_dir, 'NYC Accidents 2020.csv')
    weather_csv_path = os.path.join(data_dir, 'NYC_Weather_2016_2022.csv')

    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    download_data(traffic_accidents_url, traffic_zip_path)
    download_data(weather_data_url, weather_zip_path)

    extract_zip(traffic_zip_path, data_dir)
    extract_zip(weather_zip_path, data_dir)

    accidents_df = pd.read_csv(traffic_csv_path)
    weather_df = pd.read_csv(weather_csv_path)

    # Filter weather data for the date range 01/2020 - 08/2020
    weather_df['time'] = pd.to_datetime(weather_df['time'])
    weather_df = weather_df[(weather_df['time'] >= '2020-01-01') & (weather_df['time'] < '2020-09-01')]

    accidents_df.fillna(0, inplace=True)
    weather_df.fillna(0, inplace=True)

    conn = sqlite3.connect(os.path.join(data_dir, 'nyc_climate_traffic1.db'))
    accidents_df.to_sql('traffic_accidents', conn, if_exists='replace', index=False)
    weather_df.to_sql('weather_data', conn, if_exists='replace', index=False)
    conn.close()

if __name__ == '__main__':
    try:
        process_data()
    except zipfile.BadZipFile as e:
        print(e)
