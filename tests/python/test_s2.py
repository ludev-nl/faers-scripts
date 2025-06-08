import unittest
from unittest.mock import Mock, patch, mock_open, MagicMock, call
import json
import tempfile
import os
import sys
import psycopg
from io import BytesIO
import re

# Use robust project root import pattern
project_root = os.getcwd()
sys.path.insert(0, project_root)

try:
    import s2
except ImportError as e:
    print(f"Error importing s2 module: {e}")
    print(f"Project root path: {project_root}")
    # Try alternative import paths
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    import s2


class TestS2Pipeline(unittest.TestCase):
    """Comprehensive test suite for s2.py FAERS pipeline functions."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.sample_config = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "dbname": "test_db",
                "user": "test_user",
                "password": "test_pass"
            },
            "bucket_name": "test-bucket",
            "gcs_directory": "ascii/",
            "root_dir": "/tmp"
        }
        
        self.sample_schema_config = {
            "DEMO": [
                {
                    "date_range": ["2004Q1", "2023Q4"],
                    "columns": {
                        "primaryid": "TEXT",
                        "caseid": "TEXT",
                        "caseversion": "INTEGER"
                    }
                },
                {
                    "date_range": ["2024Q1", "9999Q4"],
                    "columns": {
                        "primaryid": "TEXT",
                        "caseid": "TEXT",
                        "caseversion": "INTEGER",
                        "email": "TEXT"
                    }
                }
            ],
            "DRUG": [
                {
                    "date_range": ["2004Q1", "9999Q4"],
                    "columns": {
                        "primaryid": "TEXT",
                        "caseid": "TEXT",
                        "drug_seq": "INTEGER",
                        "drugname": "TEXT"
                    }
                }
            ]
        }

    # ============================================================================
    # VERSION CHECK TESTS
    # ============================================================================

    @patch('s2.psycopg.__version__', '3.1.0')
    @patch('s2.logger')
    def test_check_psycopg_version_valid(self, mock_logger):
        """Test psycopg version check with valid version."""
        s2.check_psycopg_version()
        mock_logger.info.assert_called_with("Using psycopg version: 3.1.0")

    @patch('s2.psycopg.__version__', '2.9.0')
    @patch('s2.logger')
    @patch('s2.sys.exit')
    def test_check_psycopg_version_invalid(self, mock_exit, mock_logger):
        """Test psycopg version check with invalid version."""
        s2.check_psycopg_version()
        mock_logger.error.assert_called_with("This script requires psycopg 3. Found version %s", "2.9.0")
        mock_exit.assert_called_with(1)

    # ============================================================================
    # CONFIG LOADING TESTS
    # ============================================================================

    @patch('builtins.open', new_callable=mock_open)
    @patch('s2.json.load')
    @patch('s2.logger')
    def test_load_config_success(self, mock_logger, mock_json_load, mock_file):
        """Test successful config loading."""
        mock_json_load.return_value = self.sample_config
        
        result = s2.load_config()
        
        self.assertEqual(result, self.sample_config)
        mock_file.assert_called_once_with("config.json", "r", encoding="utf-8")
        mock_logger.info.assert_called_with("Loaded configuration from config.json")

    @patch('builtins.open', side_effect=FileNotFoundError)
    @patch('s2.logger')
    def test_load_config_file_not_found(self, mock_logger, mock_file):
        """Test config loading when file not found."""
        with self.assertRaises(FileNotFoundError):
            s2.load_config()
        mock_logger.error.assert_called_with("Config file config.json not found")

    @patch('builtins.open', new_callable=mock_open)
    @patch('s2.json.load', side_effect=json.JSONDecodeError("test error", "doc", 0))
    @patch('s2.logger')
    def test_load_config_json_decode_error(self, mock_logger, mock_json_load, mock_file):
        """Test config loading with JSON decode error."""
        with self.assertRaises(json.JSONDecodeError):
            s2.load_config()
        self.assertTrue(mock_logger.error.called)

    @patch('builtins.open', new_callable=mock_open)
    @patch('s2.json.load')
    @patch('s2.logger')
    def test_load_schema_config_success(self, mock_logger, mock_json_load, mock_file):
        """Test successful schema config loading."""
        mock_json_load.return_value = self.sample_schema_config
        
        result = s2.load_schema_config()
        
        self.assertEqual(result, self.sample_schema_config)
        mock_file.assert_called_once_with("schema_config.json", "r", encoding="utf-8")
        mock_logger.info.assert_called_with("Loaded schema configuration from schema_config.json")

    # ============================================================================
    # GCS FILE OPERATIONS TESTS
    # ============================================================================

    @patch('s2.storage.Client')
    @patch('s2.logger')
    def test_check_file_exists_true(self, mock_logger, mock_storage_client):
        """Test file existence check when file exists."""
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_blob.exists.return_value = True
        
        mock_storage_client.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        result = s2.check_file_exists("test-bucket", "test-file.txt")
        
        self.assertTrue(result)
        mock_client.bucket.assert_called_once_with("test-bucket")
        mock_bucket.blob.assert_called_once_with("test-file.txt")
        mock_logger.info.assert_called_with("File test-file.txt exists: True")

    @patch('s2.storage.Client', side_effect=Exception("GCS error"))
    @patch('s2.logger')
    def test_check_file_exists_exception(self, mock_logger, mock_storage_client):
        """Test file existence check with exception."""
        result = s2.check_file_exists("test-bucket", "test-file.txt")
        
        self.assertFalse(result)
        mock_logger.error.assert_called_with("Error checking file existence: GCS error")

    @patch('s2.storage.Client')
    @patch('s2.logger')
    def test_download_gcs_file_success(self, mock_logger, mock_storage_client):
        """Test successful GCS file download."""
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        
        mock_storage_client.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        result = s2.download_gcs_file("test-bucket", "test-file.txt", "/tmp/test-file.txt")
        
        self.assertTrue(result)
        mock_blob.download_to_filename.assert_called_once_with("/tmp/test-file.txt")
        mock_logger.info.assert_called_with("Downloaded test-file.txt to /tmp/test-file.txt")

    @patch('s2.storage.Client')
    @patch('s2.logger')
    def test_list_files_in_gcs_directory_success(self, mock_logger, mock_storage_client):
        """Test successful GCS directory listing."""
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob1 = Mock()
        mock_blob1.name = "test1.txt"
        mock_blob2 = Mock()
        mock_blob2.name = "test2.TXT"
        mock_blob3 = Mock()
        mock_blob3.name = "test3.csv"  # Should be filtered out
        
        mock_storage_client.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3]
        
        result = s2.list_files_in_gcs_directory("test-bucket", "ascii/")
        
        expected = ["test1.txt", "test2.TXT"]
        self.assertEqual(result, expected)
        mock_bucket.list_blobs.assert_called_once_with(prefix="ascii/")

    @patch('s2.storage.Client', side_effect=Exception("GCS error"))
    @patch('s2.logger')
    def test_list_files_in_gcs_directory_exception(self, mock_logger, mock_storage_client):
        """Test GCS directory listing with exception."""
        result = s2.list_files_in_gcs_directory("test-bucket", "ascii/")
        
        self.assertEqual(result, [])
        mock_logger.error.assert_called_with("Error listing GCS files: GCS error")

    # ============================================================================
    # SCHEMA MANAGEMENT TESTS
    # ============================================================================

    def test_get_schema_for_period_found_early_period(self):
        """Test getting schema for a valid early period."""
        result = s2.get_schema_for_period(self.sample_schema_config, "DEMO", 2020, 2)
        
        expected = {
            "primaryid": "TEXT",
            "caseid": "TEXT",
            "caseversion": "INTEGER"
        }
        self.assertEqual(result, expected)

    def test_get_schema_for_period_found_later_period(self):
        """Test getting schema for a valid later period with updated schema."""
        result = s2.get_schema_for_period(self.sample_schema_config, "DEMO", 2024, 2)
        
        expected = {
            "primaryid": "TEXT",
            "caseid": "TEXT",
            "caseversion": "INTEGER",
            "email": "TEXT"
        }
        self.assertEqual(result, expected)

    def test_get_schema_for_period_not_found_table(self):
        """Test getting schema for non-existent table."""
        with self.assertRaises(ValueError) as context:
            s2.get_schema_for_period(self.sample_schema_config, "NONEXISTENT", 2020, 2)
        
        self.assertIn("No schema found for table NONEXISTENT", str(context.exception))

    def test_get_schema_for_period_not_found_period(self):
        """Test getting schema for period outside range."""
        with self.assertRaises(ValueError) as context:
            s2.get_schema_for_period(self.sample_schema_config, "DEMO", 2003, 1)
        
        self.assertIn("No schema available for table DEMO in period 2003Q1", str(context.exception))

    def test_get_schema_for_period_open_end_range(self):
        """Test getting schema for table with open-ended date range."""
        result = s2.get_schema_for_period(self.sample_schema_config, "DRUG", 2025, 1)
        
        expected = {
            "primaryid": "TEXT",
            "caseid": "TEXT",
            "drug_seq": "INTEGER",
            "drugname": "TEXT"
        }
        self.assertEqual(result, expected)

    # ============================================================================
    # DATABASE OPERATIONS TESTS
    # ============================================================================

    @patch('s2.logger')
    def test_create_table_if_not_exists_success(self, mock_logger):
        """Test successful table creation."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        schema = {"col1": "TEXT", "col2": "INTEGER"}
        
        s2.create_table_if_not_exists(mock_conn, "test_schema.test_table", schema)
        
        expected_calls = [
            call("CREATE SCHEMA IF NOT EXISTS test_schema"),
            call("CREATE TABLE IF NOT EXISTS test_schema.test_table (col1 TEXT, col2 INTEGER)")
        ]
        mock_cursor.execute.assert_has_calls(expected_calls)
        mock_conn.commit.assert_called_once()
        mock_logger.info.assert_called_with("Table test_schema.test_table created or already exists")

    @patch('s2.logger')
    def test_create_table_if_not_exists_exception(self, mock_logger):
        """Test table creation with exception."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Database error")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        schema = {"col1": "TEXT"}
        
        with self.assertRaises(Exception):
            s2.create_table_if_not_exists(mock_conn, "test_schema.test_table", schema)
        
        mock_conn.rollback.assert_called_once()
        mock_logger.error.assert_called_with("Error creating table test_schema.test_table: Database error")

    # ============================================================================
    # DATA VALIDATION TESTS
    # ============================================================================

    @patch('builtins.open', new_callable=mock_open, read_data="col1$col2$col3\nval1$val2$val3\n")
    @patch('s2.logger')
    def test_validate_data_file_success(self, mock_logger, mock_file):
        """Test successful data file validation."""
        schema = {"col1": "TEXT", "col2": "TEXT", "col3": "TEXT"}
        
        result = s2.validate_data_file("/tmp/test.txt", schema)
        
        self.assertTrue(result)
        mock_file.assert_called_once_with("/tmp/test.txt", "r", encoding="utf-8")

    @patch('builtins.open', new_callable=mock_open, read_data="col1$col2\nval1$val2\n")
    @patch('s2.logger')
    def test_validate_data_file_column_mismatch(self, mock_logger, mock_file):
        """Test data file validation with column count mismatch."""
        schema = {"col1": "TEXT", "col2": "TEXT", "col3": "TEXT"}
        
        result = s2.validate_data_file("/tmp/test.txt", schema)
        
        self.assertFalse(result)
        mock_logger.error.assert_called_with("Header in /tmp/test.txt has 2 columns, expected 3")

    @patch('builtins.open', side_effect=Exception("File error"))
    @patch('s2.logger')
    def test_validate_data_file_exception(self, mock_logger, mock_file):
        """Test data file validation with exception."""
        schema = {"col1": "TEXT"}
        
        result = s2.validate_data_file("/tmp/test.txt", schema)
        
        self.assertFalse(result)
        mock_logger.error.assert_called_with("Error validating /tmp/test.txt: File error")

    # ============================================================================
    # DATA IMPORT TESTS
    # ============================================================================

    @patch('s2.get_schema_for_period')
    @patch('s2.create_table_if_not_exists')
    @patch('s2.validate_data_file')
    @patch('builtins.open', new_callable=mock_open, read_data=b"test data")
    @patch('s2.logger')
    def test_import_data_file_success(self, mock_logger, mock_file, mock_validate, 
                                    mock_create_table, mock_get_schema):
        """Test successful data file import."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_copy = Mock()
        mock_cursor.copy.return_value.__enter__.return_value = mock_copy
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        mock_get_schema.return_value = {"col1": "TEXT", "col2": "TEXT"}
        mock_validate.return_value = True
        
        s2.import_data_file(mock_conn, "/tmp/test.txt", "test.table", "TEST", 2020, 1, {})
        
        mock_get_schema.assert_called_once_with({}, "TEST", 2020, 1)
        mock_create_table.assert_called_once()
        mock_validate.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_logger.info.assert_called_with("Imported /tmp/test.txt into test.table")

    @patch('s2.get_schema_for_period')
    @patch('s2.create_table_if_not_exists')
    @patch('s2.validate_data_file')
    @patch('s2.logger')
    def test_import_data_file_validation_failure(self, mock_logger, mock_validate, 
                                               mock_create_table, mock_get_schema):
        """Test data file import with validation failure."""
        mock_conn = Mock()
        mock_get_schema.return_value = {"col1": "TEXT"}
        mock_validate.return_value = False
        
        s2.import_data_file(mock_conn, "/tmp/test.txt", "test.table", "TEST", 2020, 1, {})
        
        mock_logger.error.assert_called_with("Validation failed for /tmp/test.txt")

    # ============================================================================
    # MAIN FUNCTION TESTS
    # ============================================================================

    @patch('s2.check_psycopg_version')
    @patch('s2.load_config')
    @patch('s2.load_schema_config')
    @patch('s2.list_files_in_gcs_directory')
    @patch('s2.download_gcs_file')
    @patch('s2.import_data_file')
    @patch('s2.psycopg.connect')
    @patch('s2.os.path.exists')
    @patch('s2.os.makedirs')
    @patch('s2.os.remove')
    @patch('s2.logger')
    def test_main_success(self, mock_logger, mock_remove, mock_makedirs, mock_exists,
                         mock_connect, mock_import, mock_download, mock_list_files,
                         mock_load_schema, mock_load_config, mock_check_version):
        """Test successful main function execution."""
        mock_load_config.return_value = self.sample_config
        mock_load_schema.return_value = self.sample_schema_config
        mock_exists.return_value = False
        mock_list_files.return_value = ["ascii/DEMO20Q1.txt", "ascii/DRUG21Q2.txt"]
        mock_download.return_value = True
        mock_conn = Mock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        s2.main()
        
        mock_check_version.assert_called_once()
        mock_load_config.assert_called_once()
        mock_load_schema.assert_called_once()
        mock_makedirs.assert_called_once_with("/tmp")
        self.assertEqual(mock_download.call_count, 2)
        self.assertEqual(mock_import.call_count, 2)
        self.assertEqual(mock_remove.call_count, 2)

    @patch('s2.check_psycopg_version')
    @patch('s2.load_config')
    @patch('s2.load_schema_config')
    @patch('s2.list_files_in_gcs_directory')
    @patch('s2.psycopg.connect')
    @patch('s2.os.path.exists')
    @patch('s2.logger')
    def test_main_no_files(self, mock_logger, mock_exists, mock_connect,
                          mock_list_files, mock_load_schema, mock_load_config, 
                          mock_check_version):
        """Test main function when no files are found."""
        mock_load_config.return_value = self.sample_config
        mock_load_schema.return_value = self.sample_schema_config
        mock_exists.return_value = True
        mock_list_files.return_value = []
        mock_conn = Mock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        s2.main()
        
        mock_logger.info.assert_called_with("No .txt files found in gs://test-bucket/ascii/")

    @patch('s2.check_psycopg_version')
    @patch('s2.load_config')
    @patch('s2.load_schema_config')
    @patch('s2.psycopg.connect', side_effect=psycopg.Error("Database connection failed"))
    @patch('s2.os.path.exists')
    @patch('s2.logger')
    def test_main_database_error(self, mock_logger, mock_exists, mock_connect,
                                mock_load_schema, mock_load_config, mock_check_version):
        """Test main function with database error."""
        mock_load_config.return_value = self.sample_config
        mock_load_schema.return_value = self.sample_schema_config
        mock_exists.return_value = True
        
        s2.main()
        
        mock_logger.error.assert_called_with("Database error: Database connection failed")


class TestFileNamePatternMatching(unittest.TestCase):
    """Test cases for file name pattern matching logic."""
    
    def test_valid_file_patterns(self):
        """Test various valid file name patterns."""
        pattern = r"([A-Z]+)(\d{2})Q(\d)\.txt"
        
        test_cases = [
            ("DEMO20Q1.txt", ("DEMO", "20", "1")),
            ("DRUG21Q2.txt", ("DRUG", "21", "2")),
            ("REAC22Q4.txt", ("REAC", "22", "4")),
            ("OUTC23Q3.txt", ("OUTC", "23", "3"))
        ]
        
        for filename, expected in test_cases:
            with self.subTest(filename=filename):
                match = re.match(pattern, filename, re.IGNORECASE)
                self.assertIsNotNone(match)
                self.assertEqual(match.groups(), expected)
    
    def test_invalid_file_patterns(self):
        """Test invalid file name patterns."""
        pattern = r"([A-Z]+)(\d{2})Q(\d)\.txt"
        
        invalid_files = [
            "DEMO2020Q1.txt",  # 4-digit year
            "DEMO20Q5.txt",    # Invalid quarter
            "DEMO20Q1.csv",    # Wrong extension
            "123demo20Q1.txt", # Starts with numbers
            "DEMO20Q.txt",     # Missing quarter
            "DEMO20Q1",        # Missing extension
        ]
        
        for filename in invalid_files:
            with self.subTest(filename=filename):
                match = re.match(pattern, filename, re.IGNORECASE)
                self.assertIsNone(match)

    def test_case_insensitive_matching(self):
        """Test that file pattern matching is case insensitive."""
        pattern = r"([A-Z]+)(\d{2})Q(\d)\.txt"
        
        case_variations = [
            "demo20q1.txt",
            "DEMO20Q1.TXT",
            "Demo20Q1.txt",
            "drug21q2.txt"
        ]
        
        for filename in case_variations:
            with self.subTest(filename=filename):
                match = re.match(pattern, filename, re.IGNORECASE)
                self.assertIsNotNone(match)


if __name__ == "__main__":
    # Create a test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestS2Pipeline))
    suite.addTests(loader.loadTestsFromTestCase(TestFileNamePatternMatching))
    
    # Run tests with verbose output
    runner = unittest.TextTestRunner(verbosity=2, buffer=True)
    result = runner.run(suite)
    
    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)