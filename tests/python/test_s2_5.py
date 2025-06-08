import unittest
import json
import sys
import os
import tempfile
import shutil
from unittest.mock import patch, mock_open, MagicMock, Mock
import psycopg
from psycopg import errors as pg_errors

# Add the project root to the path to import s2_5
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
import s2_5

class TestS25Pipeline(unittest.TestCase):
    """Comprehensive test suite for s2_5 module."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.sample_config = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "user": "testuser",
                "dbname": "test_db",
                "password": "testpass"
            }
        }
        
        self.sample_sql_script = """
        -- This is a comment
        CREATE SCHEMA IF NOT EXISTS faers_combined;
        
        CREATE TABLE faers_combined.test_table (
            id INTEGER,
            name VARCHAR(100)
        );
        
        DO $$
        BEGIN
            INSERT INTO faers_combined.test_table VALUES (1, 'test');
            RAISE NOTICE 'Data inserted successfully';
        END
        $$;
        
        INSERT INTO faers_combined.test_table VALUES (2, 'another test');
        """

    # ============================================================================
    # CONFIG LOADING TESTS
    # ============================================================================
    
    def test_load_config_success(self):
        """Test successful config loading."""
        mock_config = json.dumps(self.sample_config)
        
        with patch("builtins.open", mock_open(read_data=mock_config)):
            result = s2_5.load_config()
            
        self.assertEqual(result, self.sample_config)

    def test_load_config_file_not_found(self):
        """Test config loading when file doesn't exist."""
        with patch("builtins.open", side_effect=FileNotFoundError):
            with self.assertRaises(FileNotFoundError):
                s2_5.load_config()

    def test_load_config_invalid_json(self):
        """Test config loading with invalid JSON."""
        invalid_json = '{"database": invalid json}'
        
        with patch("builtins.open", mock_open(read_data=invalid_json)):
            with self.assertRaises(json.JSONDecodeError):
                s2_5.load_config()

    # ============================================================================
    # SQL PARSING TESTS
    # ============================================================================
    
    def test_parse_sql_statements_basic(self):
        """Test parsing SQL statements including DO blocks and comments."""
        statements = s2_5.parse_sql_statements(self.sample_sql_script)
        
        # Should have 4 statements: CREATE SCHEMA, CREATE TABLE, DO block, INSERT
        self.assertEqual(len(statements), 4)
        
        # Check that comments are filtered out
        for stmt in statements:
            self.assertNotIn('--', stmt)
        
        # Check that DO block is preserved as one statement
        do_block = [stmt for stmt in statements if stmt.strip().startswith('DO $$')]
        self.assertEqual(len(do_block), 1)
        self.assertIn('BEGIN', do_block[0])
        self.assertIn('END', do_block[0])

    def test_parse_sql_statements_empty_input(self):
        """Test parsing empty or comment-only SQL."""
        empty_sql = """
        -- Just comments
        -- Another comment
        """
        
        statements = s2_5.parse_sql_statements(empty_sql)
        self.assertEqual(len(statements), 0)

    def test_parse_sql_statements_simple(self):
        """Test parsing simple SQL statements."""
        sql_script = """
        CREATE TABLE test1 (id INT);
        INSERT INTO test1 VALUES (1);
        -- This is a comment
        SELECT * FROM test1;
        """
        
        statements = s2_5.parse_sql_statements(sql_script)
        expected = [
            "CREATE TABLE test1 (id INT)",
            "INSERT INTO test1 VALUES (1)",
            "SELECT * FROM test1"
        ]
        
        self.assertEqual(statements, expected)

    def test_parse_sql_statements_multiline(self):
        """Test parsing multiline statements."""
        sql_script = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100),
            created_at TIMESTAMP DEFAULT NOW()
        );
        """
        
        statements = s2_5.parse_sql_statements(sql_script)
        self.assertEqual(len(statements), 1)
        self.assertIn("CREATE TABLE test_table", statements[0])
        self.assertIn("created_at TIMESTAMP", statements[0])

    # ============================================================================
    # EXECUTE WITH RETRY TESTS
    # ============================================================================

    @patch('s2_5.time.sleep')
    def test_execute_with_retry_success_on_first_attempt(self, mock_sleep):
        """Test successful execution on first attempt."""
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = None
        
        result = s2_5.execute_with_retry(mock_cursor, "SELECT 1")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once_with("SELECT 1")
        mock_sleep.assert_not_called()

    @patch('s2_5.time.sleep')
    def test_execute_with_retry_success_after_retry(self, mock_sleep):
        """Test successful execution after one retry."""
        mock_cursor = MagicMock()
        statement = "SELECT * FROM test_table"
        mock_cursor.execute.side_effect = [
            pg_errors.OperationalError("Connection lost"),
            None  # Success on second attempt
        ]
        
        result = s2_5.execute_with_retry(mock_cursor, statement, retries=2)
        
        self.assertTrue(result)
        self.assertEqual(mock_cursor.execute.call_count, 2)

    @patch('s2_5.time.sleep')
    def test_execute_with_retry_duplicate_table_handling(self, mock_sleep):
        """Test handling of duplicate table errors (should not retry)."""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.DuplicateTable("Table already exists")
        
        result = s2_5.execute_with_retry(mock_cursor, "CREATE TABLE test")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once()
        mock_sleep.assert_not_called()

    @patch('s2_5.time.sleep')  
    def test_execute_with_retry_syntax_error_handled(self, mock_sleep):
        """Test handling of syntax errors."""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.SyntaxError("Bad syntax")
        
        result = s2_5.execute_with_retry(mock_cursor, "INVALID SQL")
        
        self.assertTrue(result)
        mock_sleep.assert_not_called()

    @patch('s2_5.time.sleep')
    def test_execute_with_retry_fails_after_retries(self, mock_sleep):
        """Test failure after maximum retries."""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.OperationalError("Persistent error")
        
        with self.assertRaises(pg_errors.OperationalError):
            s2_5.execute_with_retry(mock_cursor, "SELECT * FROM test_table", retries=2)

    def test_execute_with_retry_non_retryable_error(self):
        """Test non-retryable errors are raised immediately."""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.IntegrityError("Constraint violation")
        
        with self.assertRaises(pg_errors.IntegrityError):
            s2_5.execute_with_retry(mock_cursor, "SELECT * FROM test_table")

    # ============================================================================
    # TABLE VERIFICATION TESTS
    # ============================================================================

    def test_verify_tables_success(self):
        """Test successful table verification."""
        mock_cursor = MagicMock()
        tables = ["table1", "table2"]
        mock_cursor.fetchone.side_effect = [(100,), (50,)]
        
        # Should not raise any exception
        s2_5.verify_tables(mock_cursor, tables)
        
        expected_calls = [
            unittest.mock.call('SELECT COUNT(*) FROM faers_combined."table1"'),
            unittest.mock.call('SELECT COUNT(*) FROM faers_combined."table2"')
        ]
        mock_cursor.execute.assert_has_calls(expected_calls)

    @patch('s2_5.logger')
    def test_verify_tables_empty_table_warning(self, mock_logger):
        """Test warning for empty tables."""
        mock_cursor = MagicMock()
        tables = ["empty_table"]
        mock_cursor.fetchone.return_value = (0,)
        
        s2_5.verify_tables(mock_cursor, tables)
        mock_logger.warning.assert_called()

    def test_verify_tables_nonexistent_table(self):
        """Test error when table doesn't exist."""
        mock_cursor = MagicMock()
        tables = ["nonexistent_table"]
        mock_cursor.execute.side_effect = pg_errors.UndefinedTable("Table not found")
        
        with self.assertRaises(pg_errors.UndefinedTable):
            s2_5.verify_tables(mock_cursor, tables)

    # ============================================================================
    # MAIN FUNCTION TESTS
    # ============================================================================

    @patch('s2_5.load_config')
    @patch('s2_5.os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    @patch('s2_5.psycopg.connect')
    def test_run_s2_5_sql_success(self, mock_connect, mock_file, mock_exists, mock_load_config):
        """Test successful execution of run_s2_5_sql."""
        # Setup mocks
        mock_load_config.return_value = self.sample_config
        mock_exists.return_value = True
        mock_file.return_value.read.return_value = self.sample_sql_script
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (1,)  # For table verification
        
        # Execute function
        s2_5.run_s2_5_sql()
        
        # Verify calls
        mock_connect.assert_called_once_with(**self.sample_config["database"])
        mock_cursor.execute.assert_called()  # Should have executed SQL statements

    @patch('s2_5.load_config')
    def test_run_s2_5_sql_missing_config_keys(self, mock_load_config):
        """Test error when required config keys are missing."""
        incomplete_config = {"database": {"host": "localhost"}}
        mock_load_config.return_value = incomplete_config
        
        with self.assertRaises((KeyError, ValueError)):
            s2_5.run_s2_5_sql()

    @patch('s2_5.load_config')
    @patch('s2_5.os.path.exists')
    def test_run_s2_5_sql_file_not_found(self, mock_exists, mock_load_config):
        """Test error when SQL file doesn't exist."""
        mock_load_config.return_value = self.sample_config
        mock_exists.return_value = False
        
        with self.assertRaises(FileNotFoundError):
            s2_5.run_s2_5_sql()

    @patch('s2_5.load_config')
    @patch('s2_5.os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    @patch('s2_5.psycopg.connect')
    def test_run_s2_5_sql_database_error(self, mock_connect, mock_file, mock_exists, mock_load_config):
        """Test handling of database connection errors."""
        mock_load_config.return_value = self.sample_config
        mock_exists.return_value = True
        mock_file.return_value.read.return_value = self.sample_sql_script
        mock_connect.side_effect = psycopg.OperationalError("Connection failed")
        
        with self.assertRaises(psycopg.OperationalError):
            s2_5.run_s2_5_sql()

    # ============================================================================
    # INTEGRATION TESTS
    # ============================================================================

    def setUp_integration(self):
        """Set up temporary files for integration testing."""
        self.temp_dir = tempfile.mkdtemp()
        self.config_file = os.path.join(self.temp_dir, "config.json")
        self.sql_file = os.path.join(self.temp_dir, "s2-5.sql")
        
        # Write test files
        with open(self.config_file, 'w') as f:
            json.dump(self.sample_config, f)
        
        with open(self.sql_file, 'w') as f:
            f.write(self.sample_sql_script)

    def tearDown_integration(self):
        """Clean up temporary files."""
        if hasattr(self, 'temp_dir') and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_integration_workflow_simulation(self):
        """Test a simulated complete workflow."""
        # This is a placeholder for integration tests that would require
        # actual file system setup and database connections
        # In practice, you might want to use Docker containers or 
        # in-memory databases for true integration testing
        pass


if __name__ == "__main__":
    # Run the tests with verbose output
    unittest.main(verbosity=2)