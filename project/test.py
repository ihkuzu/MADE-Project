import unittest
import os
import sqlite3
from pipeline import process_data

class TestPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up the test environment before running any tests."""
        # Run the data processing pipeline once for all tests
        process_data()

        # Set up the database connection and cursor for reuse in tests
        cls.db_path = './data/nyc_climate_traffic.db'
        cls.conn = sqlite3.connect(cls.db_path)
        cls.cursor = cls.conn.cursor()

    def test_database_creation(self):
        """Test if the database file is created."""
        self.assertTrue(os.path.exists(self.db_path), "Database file was not created.")

    def test_table_existence(self):
        """Test if the necessary tables exist."""
        tables = ['traffic_accidents', 'weather_data']
        for table in tables:
            with self.subTest(table=table):
                self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';")
                self.assertIsNotNone(self.cursor.fetchone(), f"Table '{table}' was not created in the database.")

    def test_traffic_accidents_data(self):
        """Test that the traffic_accidents table contains data."""
        self.cursor.execute("SELECT COUNT(*) FROM traffic_accidents;")
        self.assertGreater(self.cursor.fetchone()[0], 0, "Traffic accidents table does not contain any data.")

    def test_weather_data(self):
        """Test that the weather_data table contains data."""
        self.cursor.execute("SELECT COUNT(*) FROM weather_data;")
        self.assertGreater(self.cursor.fetchone()[0], 0, "Weather data table does not contain any data.")

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests."""
        cls.conn.close()
        # Remove the database file after all tests
        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)

if __name__ == "__main__":
    unittest.main()
