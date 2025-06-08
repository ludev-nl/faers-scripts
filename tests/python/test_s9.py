import unittest
from unittest.mock import Mock, patch, mock_open, MagicMock
import json
import tempfile
import os
import sys
from io import StringIO
import logging

project_root = os.getcwd()
sys.path.insert(0, project_root)

# Import the module under test
try:
    import s9
except ImportError as e:
    print(f"Error importing s9 module: {e}")
    print(f"Project root path: {project_root}")
    print(f"Python path: {sys.path}")
    raise

from psycopg import errors as pg_errors


class TestS9Execution(unittest.TestCase):
    
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
        
        self.sample_sql = """
        -- This is a comment
        CREATE SCHEMA IF NOT EXISTS faers_b;
        
        CREATE TABLE IF NOT EXISTS faers_b.DRUG_Mapper (
            id SERIAL PRIMARY KEY,
            drug_name VARCHAR(255)
        );
        
        INSERT INTO faers_b.DRUG_Mapper (drug_name) VALUES ('aspirin');
        
        DO $$
        BEGIN
            UPDATE faers_b.DRUG_Mapper SET drug_name = 'ASPIRIN' WHERE drug_name = 'aspirin';
        END
        $$;
        
        UPDATE faers_b.DRUG_Mapper 
        SET drug_name = UPPER(drug_name) 
        WHERE drug_name IS NOT NULL;
        """

    def tearDown(self):
        """Clean up after each test."""
        # Reset any module-level variables if needed
        pass

    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_load_config_success(self, mock_json_load, mock_file):
        """Test successful configuration loading."""
        mock_json_load.return_value = self.sample_config
        
        result = s9.load_config()
        
        self.assertEqual(result, self.sample_config)
        mock_file.assert_called_once_with(s9.CONFIG_FILE, "r", encoding="utf-8")
        mock_json_load.assert_called_once()

    @patch('builtins.open', side_effect=FileNotFoundError)
    def test_load_config_file_not_found(self, mock_file):
        """Test configuration loading when file doesn't exist."""
        with self.assertRaises(FileNotFoundError):
            s9.load_config()

    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load', side_effect=json.JSONDecodeError("Invalid JSON", "", 0))
    def test_load_config_invalid_json(self, mock_json_load, mock_file):
        """Test configuration loading with invalid JSON."""
        with self.assertRaises(json.JSONDecodeError):
            s9.load_config()

    def test_execute_with_retry_success_first_attempt(self):
        """Test successful execution on first attempt."""
        mock_cursor = Mock()
        mock_cursor.execute.return_value = None
        
        result = s9.execute_with_retry(mock_cursor, "SELECT 1")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once_with("SELECT 1")

    @patch('time.sleep')
    def test_execute_with_retry_success_after_retries(self, mock_sleep):
        """Test successful execution after initial failures."""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = [
            pg_errors.OperationalError("Connection failed"),
            pg_errors.OperationalError("Connection failed"),
            None  # Success on third attempt
        ]
        
        result = s9.execute_with_retry(mock_cursor, "SELECT 1", retries=3, delay=1)
        
        self.assertTrue(result)
        self.assertEqual(mock_cursor.execute.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_called_with(1)

    @patch('time.sleep')
    def test_execute_with_retry_max_retries_exceeded(self, mock_sleep):
        """Test failure after max retries exceeded."""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = pg_errors.OperationalError("Connection failed")
        
        with self.assertRaises(pg_errors.OperationalError):
            s9.execute_with_retry(mock_cursor, "SELECT 1", retries=2, delay=1)
        
        self.assertEqual(mock_cursor.execute.call_count, 2)
        self.assertEqual(mock_sleep.call_count, 1)

    def test_execute_with_retry_duplicate_object_skip(self):
        """Test that duplicate object errors are handled gracefully."""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = pg_errors.DuplicateTable("Table already exists")
        
        result = s9.execute_with_retry(mock_cursor, "CREATE TABLE test")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once()

    def test_execute_with_retry_database_error(self):
        """Test that non-retryable database errors are raised immediately."""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = pg_errors.SyntaxError("Invalid SQL")
        
        with self.assertRaises(pg_errors.SyntaxError):
            s9.execute_with_retry(mock_cursor, "INVALID SQL")
        
        mock_cursor.execute.assert_called_once()

    def test_parse_sql_statements_basic(self):
        """Test parsing of basic SQL statements."""
        sql = """
        CREATE TABLE test (id INT);
        INSERT INTO test VALUES (1);
        SELECT * FROM test;
        """
        
        statements = s9.parse_sql_statements(sql)
        
        expected = [
            "CREATE TABLE test (id INT);",
            "INSERT INTO test VALUES (1);",
            "SELECT * FROM test;"
        ]
        self.assertEqual(statements, expected)

    def test_parse_sql_statements_with_comments(self):
        """Test parsing SQL with comments removed."""
        sql = """
        -- This is a comment
        CREATE TABLE test (id INT); -- Inline comment
        /* Block comment */
        INSERT INTO test VALUES (1);
        """
        
        statements = s9.parse_sql_statements(sql)
        
        expected = [
            "CREATE TABLE test (id INT);",
            "INSERT INTO test VALUES (1);"
        ]
        self.assertEqual(statements, expected)

    def test_parse_sql_statements_do_block(self):
        """Test parsing of DO blocks."""
        sql = """
        CREATE TABLE test (id INT);
        DO $
        BEGIN
            INSERT INTO test VALUES (1);
        END
        $;
        SELECT * FROM test;
        """
        
        statements = s9.parse_sql_statements(sql)
        
        self.assertEqual(len(statements), 3)
        self.assertIn("DO $$", statements[1])
        self.assertIn("END", statements[1])

    def test_parse_sql_statements_copy_command_skipped(self):
        """Test that COPY commands are skipped."""
        sql = """
        CREATE TABLE test (id INT);
        \\copy test FROM 'data.csv';
        SELECT * FROM test;
        """
        
        statements = s9.parse_sql_statements(sql)
        
        expected = [
            "CREATE TABLE test (id INT);",
            "SELECT * FROM test;"
        ]
        self.assertEqual(statements, expected)

    def test_parse_sql_statements_bom_removal(self):
        """Test that BOM is removed from SQL."""
        sql = "\ufeffCREATE TABLE test (id INT);"
        
        statements = s9.parse_sql_statements(sql)
        
        self.assertEqual(statements, ["CREATE TABLE test (id INT);"])

    @patch('s9.load_config')
    @patch('psycopg.connect')
    def test_verify_tables_success(self, mock_connect, mock_load_config):
        """Test successful table verification."""
        mock_load_config.return_value = self.sample_config
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.side_effect = [("faers_b",), (100,)]  # Schema exists, table has 100 rows
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        # Should not raise any exceptions
        s9.verify_tables()
        
        mock_connect.assert_called_once()
        self.assertEqual(mock_cursor.execute.call_count, 2)

    @patch('s9.load_config')
    @patch('psycopg.connect')
    def test_verify_tables_schema_missing(self, mock_connect, mock_load_config):
        """Test table verification when schema is missing."""
        mock_load_config.return_value = self.sample_config
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None  # Schema doesn't exist
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        # Should not raise any exceptions
        s9.verify_tables()
        
        mock_cursor.execute.assert_called_once()

    @patch('s9.load_config')
    @patch('s9.verify_tables')
    @patch('s9.execute_with_retry')
    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    @patch('psycopg.connect')
    def test_run_s9_sql_success(self, mock_connect, mock_file, mock_exists, 
                               mock_execute, mock_verify, mock_load_config):
        """Test successful execution of run_s9_sql."""
        mock_load_config.return_value = self.sample_config
        mock_exists.return_value = True
        mock_file.return_value.read.return_value = self.sample_sql
        mock_execute.return_value = True
        
        # Mock database connections
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.side_effect = [None, ("PostgreSQL 14.0",)]  # DB doesn't exist, then version
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        s9.run_s9_sql()
        
        # Verify database creation and connection
        self.assertEqual(mock_connect.call_count, 2)  # Once for initial check, once for faersdatabase
        mock_verify.assert_called_once()

    @patch('s9.load_config')
    @patch('os.path.exists')
    def test_run_s9_sql_missing_sql_file(self, mock_exists, mock_load_config):
        """Test run_s9_sql when SQL file is missing."""
        mock_load_config.return_value = self.sample_config
        mock_exists.return_value = False
        
        with self.assertRaises(FileNotFoundError):
            s9.run_s9_sql()

    def test_run_s9_sql_missing_database_config(self):
        """Test run_s9_sql with incomplete database configuration."""
        incomplete_config = {
            "database": {
                "host": "localhost",
                "port": 5432
                # Missing user, password, dbname
            }
        }
        
        with patch('s9.load_config', return_value=incomplete_config):
            with self.assertRaises(ValueError):
                s9.run_s9_sql()

    @patch('s9.load_config')
    @patch('psycopg.connect', side_effect=pg_errors.OperationalError("Connection failed"))
    def test_run_s9_sql_connection_error(self, mock_connect, mock_load_config):
        """Test run_s9_sql with database connection error."""
        mock_load_config.return_value = self.sample_config
        
        with self.assertRaises(pg_errors.OperationalError):
            s9.run_s9_sql()

    @patch('s9.run_s9_sql')
    def test_main_success(self, mock_run_s9):
        """Test successful main execution."""
        mock_run_s9.return_value = None
        
        # Capture exit code - Note: testing __main__ execution is complex
        # In practice, you might want to refactor main logic into a separate function
        pass

    @patch('s9.run_s9_sql', side_effect=Exception("Test error"))
    @patch('sys.exit')
    def test_main_failure(self, mock_exit, mock_run_s9):
        """Test main execution with failure."""
        # This would test the main block, but it's tricky to test directly
        # In a real scenario, you might refactor to have a testable main function
        pass


class TestLoggingConfiguration(unittest.TestCase):
    """Test logging configuration and behavior."""
    
    def setUp(self):
        """Set up logging test fixtures."""
        self.log_stream = StringIO()
        self.test_handler = logging.StreamHandler(self.log_stream)
        self.test_logger = logging.getLogger('test_s9')
        self.test_logger.addHandler(self.test_handler)
        self.test_logger.setLevel(logging.DEBUG)

    def test_logging_levels(self):
        """Test that different logging levels work correctly."""
        self.test_logger.debug("Debug message")
        self.test_logger.info("Info message")
        self.test_logger.warning("Warning message")
        self.test_logger.error("Error message")
        
        log_output = self.log_stream.getvalue()
        self.assertIn("Debug message", log_output)
        self.assertIn("Info message", log_output)
        self.assertIn("Warning message", log_output)
        self.assertIn("Error message", log_output)


class TestIntegrationScenarios(unittest.TestCase):
    """Test integration scenarios with more complex setups."""
    
    @patch('s9.load_config')
    @patch('s9.parse_sql_statements')
    @patch('s9.execute_with_retry')
    @patch('psycopg.connect')
    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    def test_full_workflow_simulation(self, mock_file, mock_exists, mock_connect,
                                    mock_execute, mock_parse, mock_load_config):
        """Test a complete workflow simulation."""
        # Setup mocks
        mock_load_config.return_value = {
            "database": {
                "host": "localhost", "port": 5432, "user": "test", 
                "password": "test", "dbname": "test"
            }
        }
        mock_exists.return_value = True
        mock_file.return_value.read.return_value = "SELECT 1;"
        mock_parse.return_value = ["CREATE SCHEMA test;", "CREATE TABLE test.table1 (id INT);"]
        mock_execute.return_value = True
        
        # Setup database mocks
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.side_effect = [("test_db",), ("PostgreSQL 14.0",)]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        # Execute
        s9.run_s9_sql()
        
        # Verify workflow
        mock_load_config.assert_called()
        mock_exists.assert_called_with(s9.SQL_FILE_PATH)
        mock_parse.assert_called_once()
        self.assertEqual(mock_execute.call_count, 2)  # Two parsed statements


if __name__ == '__main__':
    # Configure test runner
    unittest.main(verbosity=2, buffer=True)