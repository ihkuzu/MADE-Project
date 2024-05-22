import os
import requests
import zipfile
import pandas as pd
import sqlite3

traffic_accidents_url = 'https://storage.googleapis.com/kaggle-data-sets/1581366/2602130/compressed/NYC%20Accidents%202020.csv.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20240522%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240522T211503Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=39dcaac3ff083194cf999143a6d02cc017e5e499d6211f1279dac962cd88606f9c88cd1fa0806e5b995e9f6be8bbe05e18effcd9e0fae4b4f5bfa1c3b8ce1899c83d69d2c3744cf029231eb732620d4da41f9392e640f6dcc0ad147b35857c949f8575ccad52b81080ebee64bafd75540bb69df6e84ff2d23d8f55a4b0d120123227f5f3cb5fb904ac90468ab79f1575526daca91f679166abbb39a27c46267145992393442f4aacfe6dc102297a0dd04f75dda6b386c6940975c8aa3d5692b118f5a8315d2208cbb678264f954b3bd36ff3542a51b7c88cde289360ac48e58adce706ba52687fb1dee8b08305909c1e523e1847835c2ccf76bec52bef575b41'
weather_data_url = 'https://storage.googleapis.com/kaggle-data-sets/2576340/4389349/compressed/NYC_Weather_2016_2022.csv.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20240522%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240522T190336Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=317b6e477585fef9c6b23deec3186513dd1d9f7644bc31844267658738eca62223de77a0f37431384fc778fe7c0fd84b9e722baed9047b9ed466965f7a6fc0c364cf137e11f7e13766e0e5fa6eac5a8ca5982096dfedb38394304750f85b04443998fb05af44b15bae1a6a986b5c50fc76b98c11998f4b4141e05f56d89b6a29874f8859eff403e087499165c4040937cd40db2677058c67e8b085e3f292c1a35699ecf588689589f5109b12f0af1ee3f40a3d9c472313d11dd486a65efe31c74199d555d9c40d7aba1ed1a1c93770e43ee9eb4806202fe0adfbcaf94a0bb686075c6ae1df67111509b90d5505f574e93cfd46259b732557562e974a6fa646d4'

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
