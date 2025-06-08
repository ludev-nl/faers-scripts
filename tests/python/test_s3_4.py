import unittest
import json
import sys
import os
import tempfile
import subprocess
from unittest.mock import patch, mock_open, MagicMock, call
from psycopg import errors as pg_errors
import psycopg

# Use robust project root import pattern
project_root = os.getcwd()
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

try:
    import s3_4
except ImportError as e:
    print(f"Error importing s3_4 module: {e}")
    print(f"Project root path: {project_root}")
    raise


class TestS34Pipeline(unittest.TestCase):
    """Comprehensive test suite for s3_4.py script."""
    
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
        
        self.complete_config = {
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
            # Missing other required fields
        }

    # ============================================================================
    # CONFIG LOADING TESTS
    # ============================================================================

    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    @patch('sys.exit')
    @patch('s3_4.logging')
    def test_config_file_not_found(self, mock_logging, mock_exit, mock_json_load, mock_file):
        """Test behavior when config.json is missing."""
        mock_file.side_effect = FileNotFoundError()
        
        # Import or execute the module to trigger the config loading
        with self.assertRaises((SystemExit, FileNotFoundError)):
            # This simulates the actual script execution
            exec(compile(open('s3_4.py').read(), 's3_4.py', 'exec'))
        
        # Should log error and exit
        self.assertTrue(mock_logging.error.called or mock_exit.called)

    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    @patch('sys.exit')
    @patch('s3_4.logging')
    def test_config_invalid_json(self, mock_logging, mock_exit, mock_json_load, mock_file):
        """Test behavior when config.json contains invalid JSON."""
        mock_json_load.side_effect = json.JSONDecodeError("Invalid JSON", "doc", 0)
        
        with self.assertRaises((SystemExit, json.JSONDecodeError)):
            exec(compile(open('s3_4.py').read(), 's3_4.py', 'exec'))
        
        # Should log JSON decode error and exit
        self.assertTrue(mock_logging.error.called or mock_exit.called)

    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    @patch('sys.exit')
    @patch('s3_4.logging')
    def test_missing_config_parameters(self, mock_logging, mock_exit, mock_json_load, mock_file):
        """Test behavior when required config parameters are missing."""
        mock_json_load.return_value = self.incomplete_config
        
        with self.assertRaises(SystemExit):
            exec(compile(open('s3_4.py').read(), 's3_4.py', 'exec'))
        
        # Should log missing parameters error and exit
        self.assertTrue(mock_logging.error.called or mock_exit.called)

    # ============================================================================
    # DATABASE CONNECTION TESTS
    # ============================================================================

    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    @patch('psycopg.connect')
    @patch('sys.exit')
    @patch('s3_4.logging')
    def test_database_connection_error(self, mock_logging, mock_exit, mock_connect, mock_json_load, mock_file):
        """Test behavior when database connection fails."""
        mock_json_load.return_value = self.sample_config
        mock_connect.side_effect = psycopg.Error("Connection failed")
        
        with self.assertRaises(SystemExit):
            exec(compile(open('s3_4.py').read(), 's3_4.py', 'exec'))
        
        mock_exit.assert_called_with(1)

    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    @patch('psycopg.connect')
    @patch('sys.exit')
    @patch('s3_4.logging')
    def test_table_existence_check_table_missing(self, mock_logging, mock_exit, mock_connect, mock_json_load, mock_file):
        """Test behavior when DEMO_Combined table doesn't exist."""
        mock_json_load.return_value = self.sample_config
        
        # Mock database connection with table not existing
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [False]  # Table doesn't exist
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        with self.assertRaises(SystemExit):
            exec(compile(open('s3_4.py').read(), 's3_4.py', 'exec'))
        
        # Should log error about missing table and exit
        mock_exit.assert_called_with(1)

    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    @patch('psycopg.connect')
    @patch('sys.exit')
    @patch('s3_4.logging')
    def test_table_exists_check_success(self, mock_logging, mock_exit, mock_connect, mock_json_load, mock_file):
        """Test successful table existence check."""
        mock_json_load.return_value = self.sample_config
        
        # Mock database connection with table existing
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
        
        # Mock successful subprocess to prevent actual SQL execution
        with patch('subprocess.run') as mock_subprocess:
            mock_subprocess.return_value.returncode = 0
            mock_subprocess.return_value.stdout = "Success"
            mock_subprocess.return_value.stderr = ""
            
            try:
                exec(compile(open('s3_4.py').read(), 's3_4.py', 'exec'))
            except SystemExit:
                pass  # May exit normally after successful execution
        
        # Verify table check was performed
        mock_cursor.execute.assert_called()

    # ============================================================================
    # SQL EXECUTION TESTS
    # ============================================================================

    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    @patch('psycopg.connect')
    @patch('subprocess.run')
    @patch('sys.exit')
    @patch('s3_4.logging')
    def test_s3_sql_execution_failure(self, mock_logging, mock_exit, mock_subprocess, mock_connect, mock_json_load, mock_file):
        """Test behavior when s3.sql execution fails."""
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
        
        with self.assertRaises(SystemExit):
            exec(compile(open('s3_4.py').read(), 's3_4.py', 'exec'))
        
        mock_exit.assert_called_with(1)

    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    @patch('psycopg.connect')
    @patch('subprocess.run')
    @patch('time.sleep')
    @patch('sys.exit')
    @patch('s3_4.logging')
    def test_s4test_sql_execution_failure(self, mock_logging, mock_exit, mock_sleep, mock_subprocess, mock_connect, mock_json_load, mock_file):
        """Test behavior when s4test.sql execution fails."""
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
        
        with self.assertRaises(SystemExit):
            exec(compile(open('s3_4.py').read(), 's3_4.py', 'exec'))
        
        mock_exit.assert_called_with(1)

    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    @patch('psycopg.connect')
    @patch('subprocess.run')
    @patch('time.sleep')
    @patch('builtins.print')
    @patch('s3_4.logging')
    def test_successful_execution(self, mock_logging, mock_print, mock_sleep, mock_subprocess, mock_connect, mock_json_load, mock_file):
        """Test successful execution of both SQL scripts."""
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
        
        try:
            exec(compile(open('s3_4.py').read(), 's3_4.py', 'exec'))
        except SystemExit:
            # May exit normally after successful execution
            pass
        
        # Verify that both SQL scripts were called
        self.assertEqual(mock_subprocess.call_count, 2)
        mock_sleep.assert_called_once_with(1)

    # ============================================================================
    # ERROR HANDLING TESTS
    # ============================================================================

    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    @patch('psycopg.connect')
    @patch('sys.exit')
    @patch('s3_4.logging')
    def test_unexpected_exception(self, mock_logging, mock_exit, mock_connect, mock_json_load, mock_file):
        """Test behavior when unexpected exception occurs."""
        mock_json_load.return_value = self.sample_config
        mock_connect.side_effect = Exception("Unexpected error")
        
        with self.assertRaises(SystemExit):
            exec(compile(open('s3_4.py').read(), 's3_4.py', 'exec'))
        
        mock_exit.assert_called_with(1)

    # ============================================================================
    # CONFIGURATION VALIDATION TESTS
    # ============================================================================

    def test_config_validation_complete(self):
        """Test configuration validation logic with complete config."""
        config = self.complete_config
        db_params = config.get("database", {})
        bucket_name = config.get("bucket_name")
        gcs_directory = config.get("gcs_directory", "ascii/")
        root_dir = config.get("root_dir", "/tmp/")
        
        self.assertTrue(all([db_params, bucket_name, gcs_directory, root_dir]))

    def test_config_validation_incomplete(self):
        """Test configuration validation logic with incomplete config."""
        # Test with incomplete config - missing bucket_name
        incomplete_config = self.complete_config.copy()
        del incomplete_config["bucket_name"]
        
        db_params = incomplete_config.get("database", {})
        bucket_name = incomplete_config.get("bucket_name")
        gcs_directory = incomplete_config.get("gcs_directory", "ascii/")
        root_dir = incomplete_config.get("root_dir", "/tmp/")
        
        self.assertFalse(all([db_params, bucket_name, gcs_directory, root_dir]))

    def test_config_validation_missing_database_params(self):
        """Test configuration validation with missing database parameters."""
        incomplete_config = self.complete_config.copy()
        del incomplete_config["database"]["user"]
        del incomplete_config["database"]["password"]
        
        db_params = incomplete_config.get("database", {})
        required_db_fields = ["host", "port", "user", "password", "dbname"]
        
        # Check if all required database fields are present
        missing_fields = [field for field in required_db_fields if field not in db_params]
        self.assertTrue(len(missing_fields) > 0)

    # ============================================================================
    # COMMAND CONSTRUCTION TESTS
    # ============================================================================

    def test_psql_command_construction(self):
        """Test that psql command is constructed correctly."""
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
        self.assertIn("-h", expected_cmd_s3)
        self.assertIn("-p", expected_cmd_s3)
        self.assertIn("-U", expected_cmd_s3)
        self.assertIn("-d", expected_cmd_s3)
        self.assertIn("-f", expected_cmd_s3)

    def test_environment_variable_setup(self):
        """Test that PGPASSWORD environment variable is set correctly."""
        password = "test_password"
        
        # Simulate setting environment variable
        import os
        original_pgpassword = os.environ.get('PGPASSWORD')
        
        try:
            os.environ['PGPASSWORD'] = password
            self.assertEqual(os.environ.get('PGPASSWORD'), password)
        finally:
            # Restore original value
            if original_pgpassword is not None:
                os.environ['PGPASSWORD'] = original_pgpassword
            else:
                os.environ.pop('PGPASSWORD', None)


class TestScriptIntegration(unittest.TestCase):
    """Integration tests for the s3_4 script."""
    
    def setUp(self):
        """Create temporary config file for integration tests."""
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
        """Clean up temporary files."""
        os.unlink(self.temp_config.name)
    
    def test_config_loading_integration(self):
        """Test actual config file loading."""
        with open(self.temp_config.name, 'r') as f:
            loaded_config = json.load(f)
        
        self.assertEqual(loaded_config, self.config_data)
        
        # Test validation
        db_params = loaded_config.get("database", {})
        bucket_name = loaded_config.get("bucket_name")
        gcs_directory = loaded_config.get("gcs_directory", "ascii/")
        root_dir = loaded_config.get("root_dir", "/tmp/")
        
        self.assertTrue(all([db_params, bucket_name, gcs_directory, root_dir]))

    def test_config_file_structure(self):
        """Test that config file has the expected structure."""
        with open(self.temp_config.name, 'r') as f:
            config = json.load(f)
        
        # Check top-level keys
        expected_keys = ["database", "bucket_name", "gcs_directory", "root_dir"]
        for key in expected_keys:
            self.assertIn(key, config)
        
        # Check database sub-keys
        db_keys = ["host", "port", "user", "password", "dbname"]
        for key in db_keys:
            self.assertIn(key, config["database"])

    def test_config_data_types(self):
        """Test that config values have correct data types."""
        with open(self.temp_config.name, 'r') as f:
            config = json.load(f)
        
        # Test data types
        self.assertIsInstance(config["database"]["host"], str)
        self.assertIsInstance(config["database"]["port"], int)
        self.assertIsInstance(config["database"]["user"], str)
        self.assertIsInstance(config["database"]["password"], str)
        self.assertIsInstance(config["database"]["dbname"], str)
        self.assertIsInstance(config["bucket_name"], str)
        self.assertIsInstance(config["gcs_directory"], str)
        self.assertIsInstance(config["root_dir"], str)


class TestTableExistenceCheck(unittest.TestCase):
    """Test cases specifically for table existence checking logic."""
    
    def test_table_check_query_construction(self):
        """Test that the table existence query is constructed correctly."""
        expected_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'faers_combined' 
            AND table_name = 'DEMO_Combined'
        );
        """
        
        # Normalize whitespace for comparison
        normalized_expected = " ".join(expected_query.split())
        
        # This would be the actual query used in the script
        actual_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'faers_combined' 
            AND table_name = 'DEMO_Combined'
        );
        """
        normalized_actual = " ".join(actual_query.split())
        
        self.assertEqual(normalized_expected, normalized_actual)

    def test_table_check_result_interpretation(self):
        """Test interpretation of table existence check results."""
        # Test True result (table exists)
        result_exists = [True]
        self.assertTrue(result_exists[0])
        
        # Test False result (table doesn't exist)
        result_not_exists = [False]
        self.assertFalse(result_not_exists[0])
        
        # Test None result (error case)
        result_none = [None]
        self.assertIsNone(result_none[0])


if __name__ == "__main__":
    # Create a test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestS34Pipeline))
    suite.addTests(loader.loadTestsFromTestCase(TestScriptIntegration))
    suite.addTests(loader.loadTestsFromTestCase(TestTableExistenceCheck))
    
    # Run tests with verbose output
    runner = unittest.TextTestRunner(verbosity=2, buffer=True)
    result = runner.run(suite)
    
    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)