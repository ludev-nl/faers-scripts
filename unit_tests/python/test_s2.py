import unittest
import json
import os
import tempfile
from unittest.mock import patch, mock_open, MagicMock
import sys

# Use robust project root import pattern
project_root = os.getcwd()
sys.path.insert(0, project_root)

try:
    import s2
except ImportError as e:
    print(f"Error importing s2 module: {e}")
    print(f"Project root path: {project_root}")
    raise


class TestFAERSPipeline(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.sample_config = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "dbname": "test_db"
            },
            "bucket_name": "test-bucket",
            "gcs_directory": "ascii/",
            "root_dir": "/tmp"
        }
        
        self.sample_schema_config = {
            "DEMO": [
                {
                    "date_range": ["2020Q1", "2023Q4"],
                    "columns": ["id", "name", "age"]
                },
                {
                    "date_range": ["2024Q1", "9999Q4"],
                    "columns": ["id", "name", "age", "email"]
                }
            ]
        }

    def test_load_config_success(self):
        """Test successful config loading."""
        mock_config = json.dumps(self.sample_config)
        
        with patch("builtins.open", mock_open(read_data=mock_config)):
            result = s2.load_config()
            
        self.assertEqual(result, self.sample_config)

    def test_load_config_file_not_found(self):
        """Test config loading when file doesn't exist."""
        with patch("builtins.open", side_effect=FileNotFoundError()):
            with self.assertRaises(FileNotFoundError):
                s2.load_config()

    def test_get_schema_for_period_valid_period(self):
        """Test getting schema for a valid time period."""
        # Test period that falls within first date range
        result = s2.get_schema_for_period(
            self.sample_schema_config, "DEMO", 2022, 3
        )
        expected = ["id", "name", "age"]
        self.assertEqual(result, expected)
        
        # Test period that falls within second date range (newer schema)
        result = s2.get_schema_for_period(
            self.sample_schema_config, "DEMO", 2024, 2
        )
        expected = ["id", "name", "age", "email"]
        self.assertEqual(result, expected)

    def test_get_schema_for_period_invalid_table(self):
        """Test getting schema for non-existent table."""
        with self.assertRaises(ValueError) as context:
            s2.get_schema_for_period(
                self.sample_schema_config, "NONEXISTENT", 2022, 1
            )
        
        self.assertIn("No schema found for table NONEXISTENT", str(context.exception))

    def test_get_schema_for_period_invalid_period(self):
        """Test getting schema for period outside available date ranges."""
        with self.assertRaises(ValueError) as context:
            s2.get_schema_for_period(
                self.sample_schema_config, "DEMO", 2019, 4  # Before 2020Q1
            )
        
        self.assertIn("No schema available for table DEMO in period 2019Q4", str(context.exception))

if __name__ == "__main__":
    # Run the tests
    unittest.main(verbosity=2)