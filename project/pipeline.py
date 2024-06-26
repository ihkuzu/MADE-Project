import os
import requests
import zipfile
import pandas as pd
import sqlite3

traffic_accidents_url = 'https://storage.googleapis.com/kaggle-data-sets/2576340/4389349/compressed/NYC_Weather_2016_2022.csv.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20240626%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240626T130317Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=https://storage.googleapis.com/kaggle-data-sets/1581366/2602130/compressed/NYC%20Accidents%202020.csv.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20240626%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240626T130303Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=7691297a665780ceb293d227185d43e87e8966d5e3e1b4fe43783e9aecbc1505cb4fc3edf839d0a8a56ac1ac248f10d5ee2c36a1bb66b16019ff494624ab7d936832985181dadfdce4769b8b9839712dc3ad1f05b238a7fc5a373758923a3e0a423be79c153fe6e78bb35ed28043661e0aba873ff3b19af0bcc072a2343ce3bd390633025e26b14f42dcf293be1cd52876aba429fd00b3575317e2289723f379644d34ed30643582948c4e46aa3773383e5c022cbd8adfd2b63d42d412088d7f0582d7c863dbc476f92c4dd644268120424c9779c0c8d62f596299c4888cc24292bf36d370c3419018aab5da7fa41d122bedc8e661be70f5e654ab192d6fde70'
weather_data_url = 'https://storage.googleapis.com/kaggle-data-sets/2576340/4389349/compressed/NYC_Weather_2016_2022.csv.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20240626%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240626T130317Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=7f676170ec0b8fc527992edc42afa28030f29a4ab8e289545369bdc9b97a41bbcfbc660acd37f79bc6ae645708ba8be15d05659cb4706ded4ac4b43ffc1ef37ad0c1d0bb71b3dac9928d392acd9d8dcb7d0f3bc85df8f33501f5c779b6aa7636a7ac54ab485696a20d99592f935289bc07571bf3ab0b4b61b4c7573fb6cbd7fbe478438bb8dea43b45fafa524f211f07e3e130d2aab706fdf9f4b68f8ae4e9029548426a5624273acd706e8456251fef6e1cc6df2cc73e89691700a6d796b6908fa7b284f5fd4e4ea82a4611a95b0af0f6b175d12c1ef0259f65c2d322b8ec3ff1666bb45f4230a8ec469b14d2f7fe7f0330cb3ff01c46b7f5ed19465f9a1cdc'

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

    conn = sqlite3.connect(os.path.join(data_dir, 'nyc_climate_traffic.db'))
    accidents_df.to_sql('traffic_accidents', conn, if_exists='replace', index=False)
    weather_df.to_sql('weather_data', conn, if_exists='replace', index=False)
    conn.close()

if __name__ == '__main__':
    try:
        process_data()
    except zipfile.BadZipFile as e:
        print(e)
