import pandas as pd
import sqlite3

url = "https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/stadt-neuss-herbstpflanzung-2023/exports/csv"
data = pd.read_csv(url, sep=';')

data = data[data['stadtteil'].str.startswith('Furth-')]

data[['geo-coordinate 1', 'geo-coordinate 2']] = data['id'].str.extract(r'(\d+\.\d+), (\d+\.\d+)')
data.drop(columns=['baumart_deutsch', 'id'], inplace=True)

data_types = {
    'Lfd. Nummer': 'INTEGER',
    'Name des Baumes': 'TEXT',
    'Pflanzung 2023': 'TEXT',
    'Stadtteil': 'TEXT',
    'Quelle': 'TEXT',
    'geo-coordinate 1': 'FLOAT',
    'geo-coordinate 2': 'FLOAT'
}

conn = sqlite3.connect('trees.sqlite')
data.to_sql('trees', conn, if_exists='replace', index=False, dtype=data_types)
conn.close()
