import unittest
import os
import sqlite3
from project.pipeline import process_data

class TestPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Building test enviroment"""
        process_data()
        cls.db_path = './data/nyc_climate_traffic.db'
        cls.conn = sqlite3.connect(cls.db_path)
        cls.cursor = cls.conn.cursor()

    def testing_database_created(self):
        """Testing if the database created or not"""
        self.assertTrue(os.path.exists(self.db_path), "Database file was not created.")

    def testing_table_exist(self):
        """Testing if the necessary tables exist or not"""
        tables = ['traffic_accidents', 'weather_data']
        for table in tables:
            with self.subTest(table=table):
                self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';")
                self.assertIsNotNone(self.cursor.fetchone(), f"Table '{table}' was not created in the database.")

    def testing_traffic_accidents_datatable(self):
        """Testing to traffic_accidents table contains data"""
        self.cursor.execute("SELECT COUNT(*) FROM traffic_accidents;")
        self.assertGreater(self.cursor.fetchone()[0], 0, "Traffic accidents table does not contain any data.")

    def testing_weather_datatable(self):
        """Testing to weather_data table contains data"""
        self.cursor.execute("SELECT COUNT(*) FROM weather_data;")
        self.assertGreater(self.cursor.fetchone()[0], 0, "Weather data table does not contain any data.")

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests"""
        cls.conn.close()
        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)

if __name__ == "__main__":
    unittest.main()
