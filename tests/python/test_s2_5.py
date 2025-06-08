import unittest
from unittest.mock import Mock, patch, mock_open, MagicMock
import json
import tempfile
import os
import psycopg
from psycopg import errors as pg_errors
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

# Import the module under test (assuming it's saved as db_executor.py)
# You'll need to adjust this import based on your actual module name
from s2_5 import (
    load_config, execute_with_retry, verify_tables, 
    parse_sql_statements, run_s2_5_sql
)


class TestLoadConfig(unittest.TestCase):
    """Test cases for load_config function."""
    
    def test_load_config_success(self):
        """Test successful config loading."""
        config_data = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "user": "testuser",
                "dbname": "testdb",
                "password": "testpass"
            }
        }
        
        with patch("builtins.open", mock_open(read_data=json.dumps(config_data))):
            result = load_config()
            self.assertEqual(result, config_data)
    
    def test_load_config_file_not_found(self):
        """Test config loading when file doesn't exist."""
        with patch("builtins.open", side_effect=FileNotFoundError):
            with self.assertRaises(FileNotFoundError):
                load_config()
    
    def test_load_config_invalid_json(self):
        """Test config loading with invalid JSON."""
        with patch("builtins.open", mock_open(read_data="invalid json")):
            with self.assertRaises(json.JSONDecodeError):
                load_config()


class TestExecuteWithRetry(unittest.TestCase):
    """Test cases for execute_with_retry function."""
    
    def setUp(self):
        self.mock_cursor = Mock()
    
    def test_execute_success_first_attempt(self):
        """Test successful execution on first attempt."""
        statement = "SELECT * FROM test_table"
        result = execute_with_retry(self.mock_cursor, statement)
        
        self.assertTrue(result)
        self.mock_cursor.execute.assert_called_once_with(statement)
    
    def test_execute_success_after_retry(self):
        """Test successful execution after one retry."""
        statement = "SELECT * FROM test_table"
        self.mock_cursor.execute.side_effect = [
            pg_errors.OperationalError("Connection lost"),
            None  # Success on second attempt
        ]
        
        with patch('time.sleep'):  # Mock sleep to speed up test
            result = execute_with_retry(self.mock_cursor, statement, retries=2)
        
        self.assertTrue(result)
        self.assertEqual(self.mock_cursor.execute.call_count, 2)
    
    def test_execute_duplicate_table_handled(self):
        """Test handling of duplicate table errors."""
        statement = "CREATE TABLE test_table"
        self.mock_cursor.execute.side_effect = pg_errors.DuplicateTable("Table exists")
        
        result = execute_with_retry(self.mock_cursor, statement)
        self.assertTrue(result)
    
    def test_execute_syntax_error_handled(self):
        """Test handling of syntax errors."""
        statement = "INVALID SQL"
        self.mock_cursor.execute.side_effect = pg_errors.SyntaxError("Bad syntax")
        
        result = execute_with_retry(self.mock_cursor, statement)
        self.assertTrue(result)
    
    def test_execute_fails_after_retries(self):
        """Test failure after maximum retries."""
        statement = "SELECT * FROM test_table"
        self.mock_cursor.execute.side_effect = pg_errors.OperationalError("Persistent error")
        
        with patch('time.sleep'):
            with self.assertRaises(pg_errors.OperationalError):
                execute_with_retry(self.mock_cursor, statement, retries=2)
    
    def test_execute_non_retryable_error(self):
        """Test non-retryable errors are raised immediately."""
        statement = "SELECT * FROM test_table"
        self.mock_cursor.execute.side_effect = pg_errors.IntegrityError("Constraint violation")
        
        with self.assertRaises(pg_errors.IntegrityError):
            execute_with_retry(self.mock_cursor, statement)


class TestVerifyTables(unittest.TestCase):
    """Test cases for verify_tables function."""
    
    def setUp(self):
        self.mock_cursor = Mock()
    
    def test_verify_tables_success(self):
        """Test successful table verification."""
        tables = ["table1", "table2"]
        self.mock_cursor.fetchone.side_effect = [(100,), (50,)]
        
        # Should not raise any exception
        verify_tables(self.mock_cursor, tables)
        
        expected_calls = [
            unittest.mock.call('SELECT COUNT(*) FROM faers_combined."table1"'),
            unittest.mock.call('SELECT COUNT(*) FROM faers_combined."table2"')
        ]
        self.mock_cursor.execute.assert_has_calls(expected_calls)
    
    def test_verify_tables_empty_table_warning(self):
        """Test warning for empty tables."""
        tables = ["empty_table"]
        self.mock_cursor.fetchone.return_value = (0,)
        
        with patch('paste.logger') as mock_logger:
            verify_tables(self.mock_cursor, tables)
            mock_logger.warning.assert_called()
    
    def test_verify_tables_nonexistent_table(self):
        """Test error when table doesn't exist."""
        tables = ["nonexistent_table"]
        self.mock_cursor.execute.side_effect = pg_errors.UndefinedTable("Table not found")
        
        with self.assertRaises(pg_errors.UndefinedTable):
            verify_tables(self.mock_cursor, tables)


class TestParseSqlStatements(unittest.TestCase):
    """Test cases for parse_sql_statements function."""
    
    def test_parse_simple_statements(self):
        """Test parsing simple SQL statements."""
        sql_script = """
        CREATE TABLE test1 (id INT);
        INSERT INTO test1 VALUES (1);
        -- This is a comment
        SELECT * FROM test1;
        """
        
        statements = parse_sql_statements(sql_script)
        expected = [
            "CREATE TABLE test1 (id INT)",
            "INSERT INTO test1 VALUES (1)",
            "SELECT * FROM test1"
        ]
        
        self.assertEqual(statements, expected)
    
    def test_parse_do_block(self):
        """Test parsing DO blocks."""
        sql_script = """
        CREATE TABLE test1 (id INT);
        DO $$
        BEGIN
            INSERT INTO test1 VALUES (1);
            INSERT INTO test1 VALUES (2);
        END
        $$;
        SELECT * FROM test1;
        """
        
        statements = parse_sql_statements(sql_script)
        self.assertEqual(len(statements), 3)
        self.assertIn("DO $$", statements[1])
        self.assertIn("END", statements[1])
        self.assertIn("$$", statements[1])
    
    def test_parse_empty_script(self):
        """Test parsing empty or comment-only script."""
        sql_script = """
        -- Just comments
        -- More comments
        """
        
        statements = parse_sql_statements(sql_script)
        self.assertEqual(statements, [])
    
    def test_parse_multiline_statement(self):
        """Test parsing multiline statements."""
        sql_script = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100),
            created_at TIMESTAMP DEFAULT NOW()
        );
        """
        
        statements = parse_sql_statements(sql_script)
        self.assertEqual(len(statements), 1)
        self.assertIn("CREATE TABLE test_table", statements[0])
        self.assertIn("created_at TIMESTAMP", statements[0])


class TestRunS25Sql(unittest.TestCase):
    """Test cases for run_s2_5_sql function."""
    
    def setUp(self):
        self.config_data = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "user": "testuser",
                "dbname": "testdb",
                "password": "testpass"
            }
        }
        self.sql_content = """
        CREATE SCHEMA IF NOT EXISTS faers_combined;
        CREATE TABLE faers_combined.DEMO_Combined (id INT);
        INSERT INTO faers_combined.DEMO_Combined VALUES (1);
        """
    
    @patch('paste.load_config')
    @patch('paste.os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    @patch('paste.psycopg.connect')
    def test_run_s2_5_sql_success(self, mock_connect, mock_file, mock_exists, mock_load_config):
        """Test successful execution of run_s2_5_sql."""
        # Setup mocks
        mock_load_config.return_value = self.config_data
        mock_exists.return_value = True
        mock_file.return_value.read.return_value = self.sql_content
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (1,)  # For table verification
        
        # Execute function
        run_s2_5_sql()
        
        # Verify calls
        mock_connect.assert_called_once_with(**self.config_data["database"])
        mock_cursor.execute.assert_called()  # Should have executed SQL statements
    
    @patch('paste.load_config')
    def test_run_s2_5_sql_missing_config_keys(self, mock_load_config):
        """Test error when required config keys are missing."""
        incomplete_config = {"database": {"host": "localhost"}}
        mock_load_config.return_value = incomplete_config
        
        with self.assertRaises(ValueError):
            run_s2_5_sql()
    
    @patch('paste.load_config')
    @patch('paste.os.path.exists')
    def test_run_s2_5_sql_file_not_found(self, mock_exists, mock_load_config):
        """Test error when SQL file doesn't exist."""
        mock_load_config.return_value = self.config_data
        mock_exists.return_value = False
        
        with self.assertRaises(FileNotFoundError):
            run_s2_5_sql()
    
    @patch('paste.load_config')
    @patch('paste.os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    @patch('paste.psycopg.connect')
    def test_run_s2_5_sql_database_error(self, mock_connect, mock_file, mock_exists, mock_load_config):
        """Test handling of database connection errors."""
        mock_load_config.return_value = self.config_data
        mock_exists.return_value = True
        mock_file.return_value.read.return_value = self.sql_content
        mock_connect.side_effect = psycopg.OperationalError("Connection failed")
        
        with self.assertRaises(psycopg.OperationalError):
            run_s2_5_sql()


class TestIntegration(unittest.TestCase):
    """Integration tests for the entire workflow."""
    
    def setUp(self):
        # Create temporary files for testing
        self.temp_dir = tempfile.mkdtemp()
        self.config_file = os.path.join(self.temp_dir, "config.json")
        self.sql_file = os.path.join(self.temp_dir, "s2-5.sql")
        
        # Sample config data
        config_data = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "user": "testuser",
                "dbname": "testdb",
                "password": "testpass"
            }
        }
        
        # Sample SQL content
        sql_content = """
        CREATE SCHEMA IF NOT EXISTS faers_combined;
        
        CREATE TABLE IF NOT EXISTS faers_combined.DEMO_Combined (
            primaryid VARCHAR(20),
            caseid VARCHAR(20),
            case_version INTEGER
        );
        
        INSERT INTO faers_combined.DEMO_Combined 
        SELECT 'test1', 'case1', 1;
        """
        
        # Write test files
        with open(self.config_file, 'w') as f:
            json.dump(config_data, f)
        
        with open(self.sql_file, 'w') as f:
            f.write(sql_content)
    
    def tearDown(self):
        # Clean up temporary files
        import shutil
        shutil.rmtree(self.temp_dir)
    
    @patch('paste.CONFIG_FILE')
    @patch('paste.SQL_FILE_PATH')
    @patch('paste.psycopg.connect')
    def test_full_workflow_simulation(self, mock_connect, mock_sql_path, mock_config_path):
        """Test the complete workflow with mocked database."""
        # Set paths to our temporary files
        mock_config_path = self.config_file
        mock_sql_path = self.sql_file
        
        # Mock database connection
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (1,)  # Table has 1 row
        
        # This would test the full workflow if we could properly patch the constants
        # In practice, you might want to make these configurable parameters
        pass


if __name__ == '__main__':
    # Create a test suite
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    
    # Run tests with verbose output
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Exit with error code if tests failed
    sys.exit(0 if result.wasSuccessful() else 1)