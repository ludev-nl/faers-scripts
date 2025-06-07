import unittest
import json
import sys
import os
from unittest.mock import patch, mock_open, MagicMock
from psycopg import errors as pg_errors

# Add the project root to the path to import s3_4
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

class TestS34Pipeline(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.sample_config = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "user": "testuser",
                "dbname": "test_db",
                "password": "testpass"
            },
            "bucket_name": "test-bucket",
            "gcs_directory": "ascii/",
            "root_dir": "/tmp/"
        }
        
        self.incomplete_config = {
            "database": {
                "host": "localhost",
                "port": 5432
            }
            # Missing other required fields
        }

    @patch('subprocess.run')
    @patch('psycopg.connect')
    @patch('builtins.exit')
    def test_config_loading_success(self, mock_exit, mock_connect, mock_subprocess):
        """Test successful config loading and script execution."""
        mock_config = json.dumps(self.sample_config)
        
        # Mock database connection and successful table check
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [True]  # Table exists
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.info.host = "localhost"
        mock_conn.info.port = 5432
        mock_conn.info.user = "testuser"
        mock_conn.info.dbname = "test_db"
        mock_conn.info.password = "testpass"
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        # Mock successful subprocess runs
        mock_subprocess.return_value.returncode = 0
        mock_subprocess.return_value.stdout = "Success"
        mock_subprocess.return_value.stderr = ""
        
        with patch("builtins.open", mock_open(read_data=mock_config)):
            import s3_4
            
        # Check that exit was not called (would indicate an error)
        mock_exit.assert_not_called()

    @patch('builtins.exit')
    def test_config_file_not_found(self, mock_exit):
        """Test behavior when config.json is missing."""
        with patch("builtins.open", side_effect=FileNotFoundError()):
            with patch('logging.error') as mock_log:
                import s3_4
                
        # Should log error and exit
        mock_log.assert_called()
        mock_exit.assert_called_with(1)

    @patch('builtins.exit')
    def test_config_invalid_json(self, mock_exit):
        """Test behavior when config.json contains invalid JSON."""
        invalid_json = '{"database": invalid json}'
        
        with patch("builtins.open", mock_open(read_data=invalid_json)):
            with patch('logging.error') as mock_log:
                import s3_4
                
        # Should log JSON decode error and exit
        mock_log.assert_called()
        mock_exit.assert_called_with(1)

    @patch('builtins.exit')
    def test_missing_config_parameters(self, mock_exit):
        """Test behavior when required config parameters are missing."""
        mock_config = json.dumps(self.incomplete_config)
        
        with patch("builtins.open", mock_open(read_data=mock_config)):
            with patch('logging.error') as mock_log:
                import s3_4
                
        # Should log missing parameters error and exit
        mock_log.assert_called()
        mock_exit.assert_called_with(1)

    @patch('subprocess.run')
    @patch('psycopg.connect')
    @patch('builtins.exit')
    def test_table_existence_check_table_missing(self, mock_exit, mock_connect, mock_subprocess):
        """Test behavior when DEMO_Combined table doesn't exist."""
        mock_config = json.dumps(self.sample_config)
        
        # Mock database connection with table not existing
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [False]  # Table doesn't exist
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        with patch("builtins.open", mock_open(read_data=mock_config)):
            with patch('logging.error') as mock_log:
                import s3_4
                
        # Should log error about missing table and exit
        mock_log.assert_called()
        mock_exit.assert_called_with(1)

if __name__ == "__main__":
    # Run the tests with verbose output
    unittest.main(verbosity=2)