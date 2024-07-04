import os
import requests
import zipfile
import pandas as pd
import sqlite3

traffic_accidents_url = 'https://drive.usercontent.google.com/uc?id=1j-kLPlCZVzLvpzyoFejnqVic3IEnYJTg&export=download'
weather_data_url = 'https://drive.usercontent.google.com/uc?id=1Wj7AhrgdvgImgZiV3y6W8XIqF_D4avIC&export=download'

def download_data(url, filename):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": "https://www.kaggle.com/",
    }
    response = requests.get(url, headers=headers)
    with open(filename, 'wb') as file:
        file.write(response.content)
    print(f"Downloaded data from {url} to {filename}")

def extract_zip(file_path, extract_to):
    if zipfile.is_zipfile(file_path):
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        print(f"Extracted {file_path} to {extract_to}")
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
        print(f"Created directory {data_dir}")

    print("Starting data download...")
    download_data(traffic_accidents_url, traffic_zip_path)
    download_data(weather_data_url, weather_zip_path)

    print("Starting data extraction...")
    extract_zip(traffic_zip_path, data_dir)
    extract_zip(weather_zip_path, data_dir)

    print("Reading CSV files into pandas dataframes...")
    accidents_df = pd.read_csv(traffic_csv_path)
    weather_df = pd.read_csv(weather_csv_path)

    print("Filtering and cleaning data...")
    weather_df['time'] = pd.to_datetime(weather_df['time'])
    weather_df = weather_df[(weather_df['time'] >= '2020-01-01') & (weather_df['time'] < '2020-09-01')]

    accidents_df.fillna(0, inplace=True)
    weather_df.fillna(0, inplace=True)

    print("Saving data to SQLite database...")
    conn = sqlite3.connect(os.path.join(data_dir, 'nyc_climate_traffic.db'))
    accidents_df.to_sql('traffic_accidents', conn, if_exists='replace', index=False)
    weather_df.to_sql('weather_data', conn, if_exists='replace', index=False)
    print("Joining and analyzing data...")
    query = '''
    WITH Traffic AS (
        SELECT 
            [CRASH DATE] as date, 
            COUNT(*) as accident_count 
        FROM 
            traffic_accidents 
        GROUP BY 
            [CRASH DATE]
    ),
    Weather AS (
        SELECT 
            DATE(time) as date,
            ROUND(AVG([temperature_2m (°C)]), 2) as avg_temperature,
            ROUND(SUM([precipitation (mm)]), 2) as total_precipitation,
            ROUND(SUM([rain (mm)]), 2) as total_rain,
            ROUND(AVG([cloudcover (%)]), 2) as avg_cloudcover,
            ROUND(AVG([cloudcover_low (%)]), 2) as avg_cloudcover_low,
            ROUND(AVG([cloudcover_mid (%)]), 2) as avg_cloudcover_mid,
            ROUND(AVG([cloudcover_high (%)]), 2) as avg_cloudcover_high,
            ROUND(AVG([windspeed_10m (km/h)]), 2) as avg_windspeed,
            ROUND(AVG([winddirection_10m (°)]), 2) as avg_winddirection
        FROM 
            weather_data
        GROUP BY 
            DATE(time)
    )
    SELECT 
        t.date,
        t.accident_count,
        w.avg_temperature,
        w.total_precipitation,
        w.total_rain,
        w.avg_cloudcover,
        w.avg_cloudcover_low,
        w.avg_cloudcover_mid,
        w.avg_cloudcover_high,
        w.avg_windspeed,
        w.avg_winddirection
    FROM 
        Traffic t
    JOIN 
        Weather w ON t.date = w.date
    ORDER BY 
        t.date;
    '''
    with sqlite3.connect(os.path.join(data_dir, 'nyc_climate_traffic.db')) as conn:
        result_df = pd.read_sql_query(query, conn)

    print("Saving analysis results to a new table in SQLite database...")
    with sqlite3.connect(os.path.join(data_dir, 'nyc_climate_traffic.db')) as conn:
        result_df.to_sql('traffic_weather_analysis', conn, if_exists='replace', index=False)

    print("Analysis complete and saved to SQLite database successfully.")
    conn.close()
    print("Data saved to SQLite database successfully.")

if __name__ == '__main__':
    try:
        process_data()
    except zipfile.BadZipFile as e:
        print(e)
