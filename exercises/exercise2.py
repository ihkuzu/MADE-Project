import pandas as pd
import sqlite3

# Download CSV data
url = "https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/stadt-neuss-herbstpflanzung-2023/exports/csv"
data = pd.read_csv(url, sep=';')

# Keep only rows with valid data
data = data[data['stadtteil'].str.startswith('Furth-')]

# Parse id column into geo-coordinate 1 and geo-coordinate 2
data[['geo-coordinate 1', 'geo-coordinate 2']] = data['id'].str.extract(r'(\d+\.\d+), (\d+\.\d+)')
data.drop(columns=['baumart_deutsch', 'id'], inplace=True)

# Assign fitting data types
data_types = {
    'Lfd. Nummer': 'INTEGER',
    'Name des Baumes': 'TEXT',
    'Pflanzung 2023': 'TEXT',
    'Stadtteil': 'TEXT',
    'Quelle': 'TEXT',
    'geo-coordinate 1': 'FLOAT',
    'geo-coordinate 2': 'FLOAT'
}

# Write data to SQLite database
conn = sqlite3.connect('trees.sqlite')
data.to_sql('trees', conn, if_exists='replace', index=False, dtype=data_types)
conn.close()
