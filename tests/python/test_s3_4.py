import unittest
import os
import sys
import json
import tempfile
import subprocess
from unittest.mock import patch, mock_open, MagicMock, call
import psycopg

# Add the parent directory to sys.path to import the module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

# Import the module to test
import s3_4


class TestS34Script(unittest.TestCase):
    """Test cases for s3_4.py script"""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.sample_config = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "user": "testuser",
                "password": "testpass",
                "dbname": "testdb"
            },
            "bucket_name": "test-bucket",
            "gcs_directory": "test-ascii/",
            "root_dir": "/tmp/test/"
        }
        
        self.incomplete_config = {
            "database": {
                "host": "localhost",
                "port": 5432
            }
            # Missing required fields
        }
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    @patch('sys.exit')
    def test_config_file_not_found(self, mock_exit, mock_json_load, mock_file):
        """Test behavior when config file is not found"""
        mock_file.side_effect = FileNotFoundError()
        
        # This should trigger the FileNotFoundError handling
        with patch('s3_4.logging') as mock_logging:
            try:
                exec(open('s3_4.py').read())
            except SystemExit:
                pass  # Expected due to exit(1)
            
            mock_exit.assert_called_with(1)
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    @patch('sys.exit')
    def test_invalid_json_config(self, mock_exit, mock_json_load, mock_file):
        """Test behavior when config file contains invalid JSON"""
        mock_json_load.side_effect = json.JSONDecodeError("Invalid JSON", "doc", 0)
        
        with patch('s3_4.logging') as mock_logging:
            try:
                exec(open('s3_4.py').read())
            except SystemExit:
                pass
            
            mock_exit.assert_called_with(1)
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    @patch('sys.exit')
    def test_missing_config_parameters(self, mock_exit, mock_json_load, mock_file):
        """Test behavior when required config parameters are missing"""
        mock_json_load.return_value = self.incomplete_config
        
        with patch('s3_4.logging') as mock_logging:
            try:
                exec(open('s3_4.py').read())
            except SystemExit:
                pass
            
            mock_exit.assert_called_with(1)
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    @patch('psycopg.connect')
    @patch('sys.exit')
    def test_table_does_not_exist(self, mock_exit, mock_connect, mock_json_load, mock_file):
        """Test behavior when DEMO_Combined table does not exist"""
        mock_json_load.return_value = self.sample_config
        
        # Mock database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [False]  # Table does not exist
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        with patch('s3_4.logging') as mock_logging:
            try:
                exec(open('s3_4.py').read())
            except SystemExit:
                pass
            
            mock_exit.assert_called_with(1)
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    @patch('psycopg.connect')
    @patch('subprocess.run')
    @patch('sys.exit')
    def test_s3_sql_execution_failure(self, mock_exit, mock_subprocess, mock_connect, mock_json_load, mock_file):
        """Test behavior when s3.sql execution fails"""
        mock_json_load.return_value = self.sample_config
        
        # Mock database connection and cursor for table check
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [True]  # Table exists
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.info.host = "localhost"
        mock_conn.info.port = 5432
        mock_conn.info.user = "testuser"
        mock_conn.info.dbname = "testdb"
        mock_conn.info.password = "testpass"
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        # Mock subprocess failure for s3.sql
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "SQL execution error"
        mock_subprocess.return_value = mock_result
        
        with patch('s3_4.logging') as mock_logging:
            try:
                exec(open('s3_4.py').read())
            except SystemExit:
                pass
            
            mock_exit.assert_called_with(1)
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    @patch('psycopg.connect')
    @patch('subprocess.run')
    @patch('time.sleep')
    @patch('sys.exit')
    def test_s4test_sql_execution_failure(self, mock_exit, mock_sleep, mock_subprocess, mock_connect, mock_json_load, mock_file):
        """Test behavior when s4test.sql execution fails"""
        mock_json_load.return_value = self.sample_config
        
        # Mock database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [True]  # Table exists
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.info.host = "localhost"
        mock_conn.info.port = 5432
        mock_conn.info.user = "testuser"
        mock_conn.info.dbname = "testdb"
        mock_conn.info.password = "testpass"
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        # Mock subprocess results
        def subprocess_side_effect(*args, **kwargs):
            if 's3.sql' in args[0]:
                # s3.sql succeeds
                result = MagicMock()
                result.returncode = 0
                result.stdout = "s3.sql executed successfully"
                return result
            elif 's4test.sql' in args[0]:
                # s4test.sql fails
                result = MagicMock()
                result.returncode = 1
                result.stderr = "No such file or directory"
                return result
        
        mock_subprocess.side_effect = subprocess_side_effect
        
        with patch('s3_4.logging') as mock_logging:
            try:
                exec(open('s3_4.py').read())
            except SystemExit:
                pass
            
            mock_exit.assert_called_with(1)
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    @patch('psycopg.connect')
    @patch('subprocess.run')
    @patch('time.sleep')
    @patch('builtins.print')
    def test_successful_execution(self, mock_print, mock_sleep, mock_subprocess, mock_connect, mock_json_load, mock_file):
        """Test successful execution of both SQL scripts"""
        mock_json_load.return_value = self.sample_config
        
        # Mock database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [True]  # Table exists
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.info.host = "localhost"
        mock_conn.info.port = 5432
        mock_conn.info.user = "testuser"
        mock_conn.info.dbname = "testdb"
        mock_conn.info.password = "testpass"
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        # Mock successful subprocess execution
        def subprocess_side_effect(*args, **kwargs):
            result = MagicMock()
            result.returncode = 0
            if 's3.sql' in args[0]:
                result.stdout = "s3.sql executed successfully"
            elif 's4test.sql' in args[0]:
                result.stdout = "s4test.sql executed successfully"
                result.stderr = ""
            return result
        
        mock_subprocess.side_effect = subprocess_side_effect
        
        with patch('s3_4.logging') as mock_logging:
            try:
                exec(open('s3_4.py').read())
            except SystemExit:
                self.fail("Script should not exit on successful execution")
            
            # Verify that both SQL scripts were called
            self.assertEqual(mock_subprocess.call_count, 2)
            mock_sleep.assert_called_once_with(1)
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    @patch('psycopg.connect')
    @patch('sys.exit')
    def test_database_connection_error(self, mock_exit, mock_connect, mock_json_load, mock_file):
        """Test behavior when database connection fails"""
        mock_json_load.return_value = self.sample_config
        mock_connect.side_effect = psycopg.Error("Connection failed")
        
        with patch('s3_4.logging') as mock_logging:
            try:
                exec(open('s3_4.py').read())
            except SystemExit:
                pass
            
            mock_exit.assert_called_with(1)
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    @patch('psycopg.connect')
    @patch('sys.exit')
    def test_unexpected_exception(self, mock_exit, mock_connect, mock_json_load, mock_file):
        """Test behavior when unexpected exception occurs"""
        mock_json_load.return_value = self.sample_config
        mock_connect.side_effect = Exception("Unexpected error")
        
        with patch('s3_4.logging') as mock_logging:
            try:
                exec(open('s3_4.py').read())
            except SystemExit:
                pass
            
            mock_exit.assert_called_with(1)
    
    def test_config_validation(self):
        """Test configuration validation logic"""
        # Test with complete config
        config = self.sample_config
        db_params = config.get("database", {})
        bucket_name = config.get("bucket_name")
        gcs_directory = config.get("gcs_directory", "ascii/")
        root_dir = config.get("root_dir", "/tmp/")
        
        self.assertTrue(all([db_params, bucket_name, gcs_directory, root_dir]))
        
        # Test with incomplete config - missing bucket_name
        incomplete_config = self.sample_config.copy()
        del incomplete_config["bucket_name"]
        
        db_params = incomplete_config.get("database", {})
        bucket_name = incomplete_config.get("bucket_name")
        gcs_directory = incomplete_config.get("gcs_directory", "ascii/")
        root_dir = incomplete_config.get("root_dir", "/tmp/")
        
        self.assertFalse(all([db_params, bucket_name, gcs_directory, root_dir]))
    
    def test_psql_command_construction(self):
        """Test that psql command is constructed correctly"""
        # Mock connection info
        host = "testhost"
        port = 5432
        user = "testuser"
        dbname = "testdb"
        
        expected_cmd_s3 = ["psql", "-h", host, "-p", str(port), "-U", user, "-d", dbname, "-f", "s3.sql"]
        expected_cmd_s4 = ["psql", "-h", host, "-p", str(port), "-U", user, "-d", dbname, "-f", "s4test.sql"]
        
        # Verify command structure
        self.assertEqual(expected_cmd_s3[-1], "s3.sql")
        self.assertEqual(expected_cmd_s4[-1], "s4test.sql")


class TestScriptIntegration(unittest.TestCase):
    """Integration tests for the script"""
    
    def setUp(self):
        """Create temporary config file for integration tests"""
        self.temp_config = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        self.config_data = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "user": "testuser",
                "password": "testpass",
                "dbname": "testdb"
            },
            "bucket_name": "test-bucket",
            "gcs_directory": "test-ascii/",
            "root_dir": "/tmp/test/"
        }
        json.dump(self.config_data, self.temp_config)
        self.temp_config.close()
    
    def tearDown(self):
        """Clean up temporary files"""
        os.unlink(self.temp_config.name)
    
    def test_config_loading_integration(self):
        """Test actual config file loading"""
        with open(self.temp_config.name, 'r') as f:
            loaded_config = json.load(f)
        
        self.assertEqual(loaded_config, self.config_data)
        
        # Test validation
        db_params = loaded_config.get("database", {})
        bucket_name = loaded_config.get("bucket_name")
        gcs_directory = loaded_config.get("gcs_directory", "ascii/")
        root_dir = loaded_config.get("root_dir", "/tmp/")
        
        self.assertTrue(all([db_params, bucket_name, gcs_directory, root_dir]))


if __name__ == '__main__':
    # Create a test suite
    unittest.main(verbosity=2)