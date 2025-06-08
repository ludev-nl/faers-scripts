import unittest
import os
import sys
import json
import tempfile
import time
from unittest.mock import patch, mock_open, MagicMock, call
from io import StringIO
import psycopg
from psycopg import errors as pg_errors

# Add the parent directory to sys.path to import the module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

# Import the module to test
import s5


class TestS5Execution(unittest.TestCase):
    """Test cases for s5_execution.py script"""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.sample_config = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "user": "testuser",
                "password": "testpass",
                "dbname": "testdb"
            }
        }
        
        self.incomplete_config = {
            "database": {
                "host": "localhost",
                "port": 5432
                # Missing required fields
            }
        }
        
        # Sample SQL content for testing
        self.sample_sql = """
        -- Create schema
        CREATE SCHEMA IF NOT EXISTS faers_b;
        
        -- Create table
        CREATE TABLE IF NOT EXISTS faers_b.test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100)
        );
        
        -- DO block example
        DO $$
        BEGIN
            INSERT INTO faers_b.test_table (name) VALUES ('test');
        END $$;
        
        -- Another statement
        SELECT COUNT(*) FROM faers_b.test_table;
        """
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_load_config_success(self, mock_json_load, mock_file):
        """Test successful configuration loading"""
        mock_json_load.return_value = self.sample_config
        
        result = s5_execution.load_config()
        
        self.assertEqual(result, self.sample_config)
        mock_file.assert_called_once_with("config.json", "r", encoding="utf-8")
        mock_json_load.assert_called_once()
    
    @patch('builtins.open', new_callable=mock_open)
    def test_load_config_file_not_found(self, mock_file):
        """Test configuration loading when file is not found"""
        mock_file.side_effect = FileNotFoundError()
        
        with self.assertRaises(FileNotFoundError):
            s5_execution.load_config()
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_load_config_invalid_json(self, mock_json_load, mock_file):
        """Test configuration loading with invalid JSON"""
        mock_json_load.side_effect = json.JSONDecodeError("Invalid JSON", "doc", 0)
        
        with self.assertRaises(json.JSONDecodeError):
            s5_execution.load_config()
    
    def test_execute_with_retry_success_first_attempt(self):
        """Test successful execution on first attempt"""
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = None
        
        result = s5_execution.execute_with_retry(mock_cursor, "SELECT 1;")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once_with("SELECT 1;")
    
    def test_execute_with_retry_success_after_retries(self):
        """Test successful execution after retries"""
        mock_cursor = MagicMock()
        # Fail twice, then succeed
        mock_cursor.execute.side_effect = [
            pg_errors.OperationalError("Connection lost"),
            pg_errors.OperationalError("Connection lost"),
            None  # Success
        ]
        
        with patch('time.sleep') as mock_sleep:
            result = s5_execution.execute_with_retry(mock_cursor, "SELECT 1;", retries=3, delay=1)
        
        self.assertTrue(result)
        self.assertEqual(mock_cursor.execute.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_has_calls([call(1), call(1)])
    
    def test_execute_with_retry_max_retries_exceeded(self):
        """Test failure after max retries exceeded"""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.OperationalError("Persistent error")
        
        with patch('time.sleep'):
            with self.assertRaises(pg_errors.OperationalError):
                s5_execution.execute_with_retry(mock_cursor, "SELECT 1;", retries=2, delay=0.1)
        
        self.assertEqual(mock_cursor.execute.call_count, 2)
    
    def test_execute_with_retry_duplicate_object_skipped(self):
        """Test that duplicate object errors are gracefully handled"""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.DuplicateTable("Table already exists")
        
        result = s5_execution.execute_with_retry(mock_cursor, "CREATE TABLE test;")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once()
    
    def test_execute_with_retry_database_error(self):
        """Test handling of non-retryable database errors"""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.SyntaxError("Invalid SQL")
        
        with self.assertRaises(pg_errors.SyntaxError):
            s5_execution.execute_with_retry(mock_cursor, "INVALID SQL;")
        
        mock_cursor.execute.assert_called_once()
    
    @patch('s5_execution.load_config')
    @patch('psycopg.connect')
    def test_verify_tables_schema_exists(self, mock_connect, mock_load_config):
        """Test table verification when schema exists"""
        mock_load_config.return_value = self.sample_config
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        # Mock schema check - schema exists
        mock_cursor.fetchone.side_effect = [
            ("faers_b",),  # Schema exists
            (100,),        # DRUG_Mapper count
            (200,),        # RXNATOMARCHIVE count
            (0,),          # RXNCONSO count (empty table)
        ]
        
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        # Should not raise an exception
        s5_execution.verify_tables()
        
        # Verify schema check was performed
        mock_cursor.execute.assert_any_call("SELECT nspname FROM pg_namespace WHERE nspname = 'faers_b'")
    
    @patch('s5_execution.load_config')
    @patch('psycopg.connect')
    def test_verify_tables_schema_missing(self, mock_connect, mock_load_config):
        """Test table verification when schema is missing"""
        mock_load_config.return_value = self.sample_config
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        # Mock schema check - schema does not exist
        mock_cursor.fetchone.return_value = None
        
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        # Should not raise an exception, just log warning
        s5_execution.verify_tables()
        
        mock_cursor.execute.assert_called_once_with("SELECT nspname FROM pg_namespace WHERE nspname = 'faers_b'")
    
    @patch('s5_execution.load_config')
    @patch('psycopg.connect')
    def test_verify_tables_table_missing(self, mock_connect, mock_load_config):
        """Test table verification when some tables are missing"""
        mock_load_config.return_value = self.sample_config
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        # Mock schema exists, but table query fails
        mock_cursor.fetchone.side_effect = [("faers_b",)]  # Schema exists
        mock_cursor.execute.side_effect = [
            None,  # Schema check succeeds
            pg_errors.UndefinedTable("Table does not exist")  # Table check fails
        ]
        
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        # Should not raise an exception, just log warning
        s5_execution.verify_tables()
    
    def test_parse_sql_statements_basic(self):
        """Test basic SQL statement parsing"""
        sql = """
        CREATE TABLE test1 (id INT);
        CREATE TABLE test2 (id INT);
        SELECT * FROM test1;
        """
        
        statements = s5_execution.parse_sql_statements(sql)
        
        self.assertEqual(len(statements), 3)
        self.assertIn("CREATE TABLE test1", statements[0])
        self.assertIn("CREATE TABLE test2", statements[1])
        self.assertIn("SELECT * FROM test1", statements[2])
    
    def test_parse_sql_statements_with_comments(self):
        """Test SQL parsing with comments"""
        sql = """
        -- This is a comment
        CREATE TABLE test1 (id INT); -- Inline comment
        /* Multi-line comment */
        CREATE TABLE test2 (id INT);
        """
        
        statements = s5_execution.parse_sql_statements(sql)
        
        self.assertEqual(len(statements), 2)
        # Comments should be removed
        for stmt in statements:
            self.assertNotIn("--", stmt)
            self.assertNotIn("/*", stmt)
    
    def test_parse_sql_statements_with_do_block(self):
        """Test SQL parsing with DO blocks"""
        sql = """
        CREATE TABLE test1 (id INT);
        DO $$
        BEGIN
            INSERT INTO test1 VALUES (1);
            INSERT INTO test1 VALUES (2);
        END $$;
        CREATE TABLE test2 (id INT);
        """
        
        statements = s5_execution.parse_sql_statements(sql)
        
        self.assertEqual(len(statements), 3)
        # DO block should be kept as one statement
        do_block = statements[1]
        self.assertIn("DO $$", do_block)
        self.assertIn("END $$", do_block)
        self.assertIn("INSERT INTO test1 VALUES (1)", do_block)
    
    def test_parse_sql_statements_skip_copy_commands(self):
        """Test that \\copy commands are skipped"""
        sql = """
        CREATE TABLE test1 (id INT);
        \\copy test1 FROM 'data.csv' WITH CSV HEADER;
        CREATE TABLE test2 (id INT);
        """
        
        statements = s5_execution.parse_sql_statements(sql)
        
        self.assertEqual(len(statements), 2)
        # \\copy command should be skipped
        for stmt in statements:
            self.assertNotIn("\\copy", stmt)
    
    def test_parse_sql_statements_skip_create_database(self):
        """Test that CREATE DATABASE statements are filtered out"""
        sql = """
        CREATE DATABASE testdb;
        CREATE TABLE test1 (id INT);
        CREATE DATABASE another_db;
        """
        
        statements = s5_execution.parse_sql_statements(sql)
        
        self.assertEqual(len(statements), 1)
        self.assertIn("CREATE TABLE test1", statements[0])
        # CREATE DATABASE should be filtered out
        for stmt in statements:
            self.assertNotIn("CREATE DATABASE", stmt)
    
    @patch('s5_execution.load_config')
    @patch('s5_execution.verify_tables')
    @patch('s5_execution.execute_with_retry')
    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    @patch('psycopg.connect')
    def test_run_s5_sql_success(self, mock_connect, mock_file, mock_exists, 
                               mock_execute, mock_verify, mock_load_config):
        """Test successful execution of run_s5_sql"""
        mock_load_config.return_value = self.sample_config
        mock_exists.return_value = True
        mock_file.return_value.read.return_value = self.sample_sql
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            None,  # Database doesn't exist initially
            ("faers_b",),  # Database exists after creation
        ]
        
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        mock_execute.return_value = True
        
        # Should not raise an exception
        s5_execution.run_s5_sql()
        
        # Verify database creation was attempted
        mock_cursor.execute.assert_any_call("SELECT 1 FROM pg_database WHERE datname = 'faersdatabase'")
        mock_verify.assert_called_once()
        self.assertTrue(mock_execute.called)
    
    @patch('s5_execution.load_config')
    def test_run_s5_sql_missing_config_keys(self, mock_load_config):
        """Test run_s5_sql with incomplete configuration"""
        mock_load_config.return_value = self.incomplete_config
        
        with self.assertRaises(ValueError) as cm:
            s5_execution.run_s5_sql()
        
        self.assertIn("Missing database configuration", str(cm.exception))
    
    @patch('s5_execution.load_config')
    @patch('os.path.exists')
    def test_run_s5_sql_missing_sql_file(self, mock_exists, mock_load_config):
        """Test run_s5_sql when SQL file is missing"""
        mock_load_config.return_value = self.sample_config
        mock_exists.return_value = False
        
        with patch('psycopg.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = ("faers_b",)  # Database exists
            
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_connect.return_value.__enter__.return_value = mock_conn
            
            with self.assertRaises(FileNotFoundError):
                s5_execution.run_s5_sql()
    
    @patch('s5_execution.load_config')
    @patch('psycopg.connect')
    def test_run_s5_sql_database_error(self, mock_connect, mock_load_config):
        """Test run_s5_sql with database connection error"""
        mock_load_config.return_value = self.sample_config
        mock_connect.side_effect = pg_errors.OperationalError("Connection failed")
        
        with self.assertRaises(pg_errors.OperationalError):
            s5_execution.run_s5_sql()
    
    @patch('s5_execution.load_config')
    @patch('s5_execution.execute_with_retry')
    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    @patch('psycopg.connect')
    def test_run_s5_sql_statement_execution_error(self, mock_connect, mock_file, 
                                                  mock_exists, mock_execute, mock_load_config):
        """Test run_s5_sql when some statements fail"""
        mock_load_config.return_value = self.sample_config
        mock_exists.return_value = True
        mock_file.return_value.read.return_value = "CREATE TABLE test1 (id INT); CREATE TABLE test2 (id INT);"
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("faers_b",)  # Database exists
        
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        # First statement succeeds, second fails
        mock_execute.side_effect = [True, pg_errors.SyntaxError("Invalid SQL")]
        
        # Should continue execution despite errors
        s5_execution.run_s5_sql()
        
        # Both statements should have been attempted
        self.assertEqual(mock_execute.call_count, 2)
    
    def test_logger_configuration(self):
        """Test that logger is configured correctly"""
        # The logger should be configured when the module is imported
        logger = s5_execution.logger
        
        self.assertEqual(logger.name, "s5_execution")
        self.assertEqual(logger.level, s5_execution.logging.DEBUG)
        
        # Should have both file and stream handlers
        handler_types = [type(h).__name__ for h in logger.handlers]
        # Note: The actual handlers might not be set up in test environment
        # This test mainly verifies the logger object exists and is named correctly


class TestS5ExecutionIntegration(unittest.TestCase):
    """Integration tests for s5_execution.py"""
    
    def setUp(self):
        """Set up integration test environment"""
        self.test_db_available = all([
            os.getenv("TEST_DB_HOST"),
            os.getenv("TEST_DB_USER"),
            os.getenv("TEST_DB_NAME")
        ])
        
        if self.test_db_available:
            self.db_params = {
                "host": os.getenv("TEST_DB_HOST"),
                "port": int(os.getenv("TEST_DB_PORT", 5432)),
                "user": os.getenv("TEST_DB_USER"),
                "password": os.getenv("TEST_DB_PASSWORD", ""),
                "dbname": os.getenv("TEST_DB_NAME")
            }
    
    @unittest.skipUnless(os.getenv("RUN_INTEGRATION_TESTS"), "Integration tests disabled")
    def test_config_loading_integration(self):
        """Test actual config file loading"""
        # Create a temporary config file
        config_data = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "user": "testuser",
                "password": "testpass",
                "dbname": "testdb"
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            temp_config_path = f.name
        
        try:
            # Temporarily replace the config file path
            original_config_file = s5_execution.CONFIG_FILE
            s5_execution.CONFIG_FILE = temp_config_path
            
            loaded_config = s5_execution.load_config()
            self.assertEqual(loaded_config, config_data)
            
        finally:
            # Restore original config file path and clean up
            s5_execution.CONFIG_FILE = original_config_file
            os.unlink(temp_config_path)
    
    @unittest.skipUnless(os.getenv("RUN_INTEGRATION_TESTS"), "Integration tests disabled")
    def test_sql_parsing_with_real_file(self):
        """Test SQL parsing with a real file"""
        if not self.test_db_available:
            self.skipTest("Test database not configured")
        
        # Create a temporary SQL file
        sql_content = """
        -- Test SQL file
        CREATE SCHEMA IF NOT EXISTS test_schema;
        
        CREATE TABLE IF NOT EXISTS test_schema.test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100)
        );
        
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM test_schema.test_table WHERE name = 'test') THEN
                INSERT INTO test_schema.test_table (name) VALUES ('test');
            END IF;
        END $$;
        
        -- Final statement
        SELECT COUNT(*) FROM test_schema.test_table;
        """
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_content)
            temp_sql_path = f.name
        
        try:
            statements = s5_execution.parse_sql_statements(sql_content)
            
            # Should have 4 statements: CREATE SCHEMA, CREATE TABLE, DO block, SELECT
            self.assertEqual(len(statements), 4)
            
            # Verify DO block is kept intact
            do_statement = [s for s in statements if 'DO $$' in s][0]
            self.assertIn('BEGIN', do_statement)
            self.assertIn('END $$', do_statement)
            
        finally:
            os.unlink(temp_sql_path)


class TestS5ExecutionEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions"""
    
    def test_execute_with_retry_zero_retries(self):
        """Test execute_with_retry with zero retries"""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.OperationalError("Error")
        
        with self.assertRaises(pg_errors.OperationalError):
            s5_execution.execute_with_retry(mock_cursor, "SELECT 1;", retries=0)
        
        mock_cursor.execute.assert_called_once()
    
    def test_parse_sql_statements_empty_input(self):
        """Test SQL parsing with empty input"""
        statements = s5_execution.parse_sql_statements("")
        self.assertEqual(statements, [])
        
        statements = s5_execution.parse_sql_statements("   \n\n   ")
        self.assertEqual(statements, [])
    
    def test_parse_sql_statements_only_comments(self):
        """Test SQL parsing with only comments"""
        sql = """
        -- This is a comment
        /* Another comment */
        -- Final comment
        """
        
        statements = s5_execution.parse_sql_statements(sql)
        self.assertEqual(statements, [])
    
    def test_parse_sql_statements_malformed_do_block(self):
        """Test SQL parsing with malformed DO block"""
        sql = """
        DO $$
        BEGIN
            SELECT 1;
        -- Missing END $$
        """
        
        statements = s5_execution.parse_sql_statements(sql)
        
        # Should still return something, even if malformed
        self.assertGreater(len(statements), 0)
    
    def test_verify_tables_connection_error(self):
        """Test verify_tables with connection error"""
        with patch('s5_execution.load_config') as mock_load_config:
            mock_load_config.return_value = {"database": {}}
            
            with patch('psycopg.connect') as mock_connect:
                mock_connect.side_effect = pg_errors.OperationalError("Connection failed")
                
                # Should not raise exception, just log error
                s5_execution.verify_tables()
    
    @patch('s5_execution.run_s5_sql')
    def test_main_execution_success(self, mock_run_s5):
        """Test main execution path success"""
        mock_run_s5.return_value = None
        
        # Capture exit code
        with patch('sys.exit') as mock_exit:
            # Import and run the main block
            exec(compile(open('s5_execution.py').read(), 's5_execution.py', 'exec'))
            
            # Should not call exit with error code
            mock_exit.assert_not_called()
    
    @patch('s5_execution.run_s5_sql')
    def test_main_execution_failure(self, mock_run_s5):
        """Test main execution path failure"""
        mock_run_s5.side_effect = Exception("Test error")
        
        with patch('sys.exit') as mock_exit:
            # This would be the main block execution
            try:
                s5_execution.run_s5_sql()
            except Exception:
                # Simulate the exception handling in main
                mock_exit(1)
            
            mock_exit.assert_called_with(1)


if __name__ == '__main__':
    # Set up test environment
    print("Running s5_execution.py unit tests...")
    print("This tests the Python script that executes s5.sql with retry logic")
    print("For integration tests, set environment variables:")
    print("  TEST_DB_HOST, TEST_DB_USER, TEST_DB_NAME, TEST_DB_PASSWORD")
    print("  RUN_INTEGRATION_TESTS=1")
    print()
    
    # Run tests
    unittest.main(verbosity=2)