import unittest
import json
import sys
import os
import tempfile
import time
from unittest.mock import patch, mock_open, MagicMock, call
import psycopg
from psycopg import errors as pg_errors

# Use robust project root import pattern
project_root = os.getcwd()
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

try:
    import s6
except ImportError as e:
    print(f"Error importing s6 module: {e}")
    print(f"Project root path: {project_root}")
    raise


class TestS6Pipeline(unittest.TestCase):
    """Comprehensive test suite for s6.py script."""
    
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
        
        self.complete_config = {
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
        
        self.sample_sql_script = """
        -- Create schema
        CREATE SCHEMA IF NOT EXISTS faers_b;
        
        -- Create table
        CREATE TABLE faers_b.test_table (
            id INTEGER,
            name VARCHAR(100)
        );
        
        CREATE OR REPLACE FUNCTION faers_b.test_function()
        RETURNS void AS $$
        BEGIN
            INSERT INTO faers_b.test_table VALUES (1, 'test');
            RAISE NOTICE 'Function executed';
        END
        $$ LANGUAGE plpgsql;
        
        DO $$
        BEGIN
            INSERT INTO faers_b.test_table VALUES (2, 'from_do_block');
        END
        $$;
        
        \\copy faers_b.test_table FROM 'data.csv' WITH CSV HEADER;
        
        INSERT INTO faers_b.test_table VALUES (3, 'regular insert');
        """
        
        # SQL script with BOM (Byte Order Mark)
        self.bom_sql_script = "\ufeff" + self.sample_sql_script

    # ============================================================================
    # CONFIG LOADING TESTS
    # ============================================================================

    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_load_config_success(self, mock_json_load, mock_file):
        """Test successful configuration loading."""
        mock_json_load.return_value = self.sample_config
        
        result = s6.load_config()
        
        self.assertEqual(result, self.sample_config)
        mock_file.assert_called_once_with("config.json", "r", encoding="utf-8")

    @patch('builtins.open', new_callable=mock_open)
    def test_load_config_file_not_found(self, mock_file):
        """Test configuration loading when file is not found."""
        mock_file.side_effect = FileNotFoundError()
        
        with self.assertRaises(FileNotFoundError):
            s6.load_config()

    def test_load_config_invalid_json(self):
        """Test config loading with malformed JSON."""
        invalid_json = '{"database": invalid json}'
        
        with patch("builtins.open", mock_open(read_data=invalid_json)):
            with self.assertRaises(json.JSONDecodeError):
                s6.load_config()

    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_load_config_invalid_json_detailed(self, mock_json_load, mock_file):
        """Test configuration loading with invalid JSON (detailed)."""
        mock_json_load.side_effect = json.JSONDecodeError("Invalid JSON", "doc", 0)
        
        with self.assertRaises(json.JSONDecodeError):
            s6.load_config()

    # ============================================================================
    # SQL PARSING TESTS
    # ============================================================================

    def test_parse_sql_statements_with_functions_and_do_blocks(self):
        """Test parsing SQL statements with both functions and DO blocks."""
        statements = s6.parse_sql_statements(self.sample_sql_script)
        
        # Should have 5 statements: CREATE SCHEMA, CREATE TABLE, CREATE FUNCTION, DO block, INSERT
        # The \copy command should be filtered out
        self.assertEqual(len(statements), 5)
        
        # Check that comments are filtered out
        for stmt in statements:
            self.assertNotIn('--', stmt)
        
        # Check that \copy commands are filtered out
        for stmt in statements:
            self.assertNotIn('\\copy', stmt.lower())
        
        # Check that function is preserved as one statement
        function_statements = [stmt for stmt in statements if 'CREATE OR REPLACE FUNCTION' in stmt]
        self.assertEqual(len(function_statements), 1)
        self.assertIn('LANGUAGE plpgsql', function_statements[0])
        
        # Check that DO block is preserved as one statement
        do_block = [stmt for stmt in statements if stmt.strip().startswith('DO $$')]
        self.assertEqual(len(do_block), 1)
        self.assertIn('BEGIN', do_block[0])
        self.assertIn('END', do_block[0])

    def test_parse_sql_statements_with_bom(self):
        """Test parsing SQL statements that start with BOM character."""
        statements = s6.parse_sql_statements(self.bom_sql_script)
        
        # Should handle BOM gracefully and produce same results
        self.assertEqual(len(statements), 5)
        
        # First statement should not contain BOM
        first_stmt = statements[0]
        self.assertNotIn('\ufeff', first_stmt)

    def test_parse_sql_statements_empty_input(self):
        """Test parsing empty or comment-only SQL."""
        empty_sql = """
        -- Just comments
        -- Another comment
        
        
        """
        
        statements = s6.parse_sql_statements(empty_sql)
        self.assertEqual(len(statements), 0)

    def test_parse_sql_statements_nested_functions(self):
        """Test parsing nested function definitions."""
        nested_function_sql = """
        CREATE OR REPLACE FUNCTION faers_b.outer_function()
        RETURNS void AS $$
        DECLARE
            inner_var INTEGER;
        BEGIN
            -- This contains $$ inside the function
            EXECUTE 'CREATE TEMP TABLE test AS SELECT $$ || ''hello'' || $$';
            RAISE NOTICE 'Executed with $$';
        END
        $$ LANGUAGE plpgsql;
        """
        
        statements = s6.parse_sql_statements(nested_function_sql)
        self.assertEqual(len(statements), 1)
        self.assertIn('LANGUAGE plpgsql', statements[0])

    def test_parse_sql_statements_basic(self):
        """Test basic SQL statement parsing."""
        sql = """
        CREATE TABLE test1 (id INT);
        CREATE TABLE test2 (id INT);
        SELECT * FROM test1;
        """
        
        statements = s6.parse_sql_statements(sql)
        
        self.assertEqual(len(statements), 3)
        self.assertIn("CREATE TABLE test1", statements[0])
        self.assertIn("CREATE TABLE test2", statements[1])
        self.assertIn("SELECT * FROM test1", statements[2])

    def test_parse_sql_statements_with_do_block(self):
        """Test SQL parsing with DO blocks."""
        sql = """
        CREATE TABLE test1 (id INT);
        DO $$
        BEGIN
            INSERT INTO test1 VALUES (1);
        END $$;
        CREATE TABLE test2 (id INT);
        """
        
        statements = s6.parse_sql_statements(sql)
        
        self.assertEqual(len(statements), 3)
        do_block = statements[1]
        self.assertIn("DO $$", do_block)
        self.assertIn("END $$", do_block)

    def test_parse_sql_statements_with_function(self):
        """Test SQL parsing with function definitions."""
        sql = """
        CREATE OR REPLACE FUNCTION test_func()
        RETURNS INTEGER AS $$
        BEGIN
            RETURN 1;
        END;
        $$ LANGUAGE plpgsql;
        """
        
        statements = s6.parse_sql_statements(sql)
        
        self.assertEqual(len(statements), 1)
        function_stmt = statements[0]
        self.assertIn("CREATE OR REPLACE FUNCTION", function_stmt)
        self.assertIn("LANGUAGE plpgsql", function_stmt)

    def test_parse_sql_statements_skip_copy_commands(self):
        """Test that \\copy commands are skipped."""
        sql = """
        CREATE TABLE test1 (id INT);
        \\copy test1 FROM 'data.csv' WITH CSV HEADER;
        CREATE TABLE test2 (id INT);
        """
        
        statements = s6.parse_sql_statements(sql)
        
        self.assertEqual(len(statements), 2)
        for stmt in statements:
            self.assertNotIn("\\copy", stmt)

    def test_parse_sql_statements_complex_function(self):
        """Test SQL parsing with complex function."""
        sql = """
        CREATE OR REPLACE FUNCTION complex_func()
        RETURNS TEXT AS $$
        DECLARE
            result TEXT;
        BEGIN
            SELECT 'test' INTO result;
            RETURN result;
        END;
        $$ LANGUAGE plpgsql;
        """
        
        statements = s6.parse_sql_statements(sql)
        
        self.assertEqual(len(statements), 1)
        function_stmt = statements[0]
        self.assertIn("CREATE OR REPLACE FUNCTION", function_stmt)
        self.assertIn("DECLARE", function_stmt)
        self.assertIn("LANGUAGE plpgsql", function_stmt)

    # ============================================================================
    # EXECUTE WITH RETRY TESTS
    # ============================================================================

    @patch('s6.time.sleep')
    def test_execute_with_retry_success_first_attempt(self, mock_sleep):
        """Test successful execution on first attempt."""
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = None
        
        result = s6.execute_with_retry(mock_cursor, "SELECT 1")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once_with("SELECT 1")
        mock_sleep.assert_not_called()

    @patch('s6.time.sleep')
    def test_execute_with_retry_database_error_then_success(self, mock_sleep):
        """Test retry logic with database error followed by success."""
        mock_cursor = MagicMock()
        # First call fails with DatabaseError, second succeeds
        mock_cursor.execute.side_effect = [
            pg_errors.DatabaseError("Database temporarily unavailable"),
            None  # Success on second attempt
        ]
        
        result = s6.execute_with_retry(mock_cursor, "SELECT 1", retries=2, delay=1)
        
        self.assertTrue(result)
        self.assertEqual(mock_cursor.execute.call_count, 2)
        mock_sleep.assert_called_once_with(1)

    @patch('s6.time.sleep')
    def test_execute_with_retry_duplicate_index_no_retry(self, mock_sleep):
        """Test that duplicate index errors don't trigger retries."""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.DuplicateIndex("Index already exists")
        
        result = s6.execute_with_retry(mock_cursor, "CREATE INDEX test_idx ON test(id)", retries=3)
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once()
        mock_sleep.assert_not_called()

    def test_execute_with_retry_success_after_retries(self):
        """Test successful execution after retries."""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = [
            pg_errors.OperationalError("Connection lost"),
            pg_errors.OperationalError("Connection lost"),
            None  # Success
        ]
        
        with patch('time.sleep') as mock_sleep:
            result = s6.execute_with_retry(mock_cursor, "SELECT 1;", retries=3, delay=1)
        
        self.assertTrue(result)
        self.assertEqual(mock_cursor.execute.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    def test_execute_with_retry_max_retries_exceeded(self):
        """Test failure after max retries exceeded."""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.OperationalError("Persistent error")
        
        with patch('time.sleep'):
            with self.assertRaises(pg_errors.OperationalError):
                s6.execute_with_retry(mock_cursor, "SELECT 1;", retries=2, delay=0.1)
        
        self.assertEqual(mock_cursor.execute.call_count, 2)

    def test_execute_with_retry_duplicate_object_skipped(self):
        """Test that duplicate object errors are gracefully handled."""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.DuplicateTable("Table already exists")
        
        result = s6.execute_with_retry(mock_cursor, "CREATE TABLE test;")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once()

    # ============================================================================
    # TABLE VERIFICATION TESTS
    # ============================================================================

    @patch('s6.load_config')
    @patch('psycopg.connect')
    def test_verify_tables_schema_exists(self, mock_connect, mock_load_config):
        """Test table verification when schema exists."""
        mock_load_config.return_value = self.sample_config
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        # Mock schema check and table counts
        mock_cursor.fetchone.side_effect = [
            ("faers_b",),  # Schema exists
            (100,),        # DRUG_Mapper count
            (200,),        # products_at_fda count
            (0,),          # IDD count (empty)
            (50,),         # manual_mapping count
        ]
        
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        s6.verify_tables()
        
        mock_cursor.execute.assert_any_call("SELECT nspname FROM pg_namespace WHERE nspname = 'faers_b'")

    @patch('s6.load_config')
    @patch('psycopg.connect')
    def test_verify_tables_schema_missing(self, mock_connect, mock_load_config):
        """Test table verification when schema is missing."""
        mock_load_config.return_value = self.sample_config
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None  # Schema doesn't exist
        
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        s6.verify_tables()  # Should not raise exception

    @patch('s6.load_config')
    @patch('psycopg.connect')
    def test_verify_tables_connection_error(self, mock_connect, mock_load_config):
        """Test verify_tables with connection error."""
        mock_load_config.return_value = {"database": {}}
        mock_connect.side_effect = pg_errors.OperationalError("Connection failed")
        
        # Should not raise exception, just log error
        s6.verify_tables()

    # ============================================================================
    # MAIN FUNCTION TESTS
    # ============================================================================

    @patch('s6.load_config')
    @patch('s6.verify_tables')
    @patch('s6.execute_with_retry')
    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    @patch('psycopg.connect')
    def test_run_s6_sql_success(self, mock_connect, mock_file, mock_exists, 
                               mock_execute, mock_verify, mock_load_config):
        """Test successful execution of run_s6_sql."""
        mock_load_config.return_value = self.sample_config
        mock_exists.return_value = True
        mock_file.return_value.read.return_value = "CREATE TABLE test (id INT);"
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            None,          # Database doesn't exist initially
            ("faers_b",),  # Database exists after creation
        ]
        
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_execute.return_value = True
        
        s6.run_s6_sql()
        
        mock_cursor.execute.assert_any_call("SELECT 1 FROM pg_database WHERE datname = 'faersdatabase'")
        mock_verify.assert_called_once()
        self.assertTrue(mock_execute.called)

    @patch('s6.load_config')
    def test_run_s6_sql_missing_config_keys(self, mock_load_config):
        """Test run_s6_sql with incomplete configuration."""
        mock_load_config.return_value = self.incomplete_config
        
        with self.assertRaises(ValueError) as cm:
            s6.run_s6_sql()
        
        self.assertIn("Missing database configuration", str(cm.exception))

    @patch('s6.load_config')
    @patch('os.path.exists')
    def test_run_s6_sql_missing_sql_file(self, mock_exists, mock_load_config):
        """Test run_s6_sql when SQL file is missing."""
        mock_load_config.return_value = self.sample_config
        mock_exists.return_value = False
        
        with patch('psycopg.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = ("faers_b",)
            
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_connect.return_value.__enter__.return_value = mock_conn
            
            with self.assertRaises(FileNotFoundError):
                s6.run_s6_sql()

    @patch('s6.load_config')
    @patch('psycopg.connect')
    def test_run_s6_sql_database_error(self, mock_connect, mock_load_config):
        """Test run_s6_sql with database connection error."""
        mock_load_config.return_value = self.sample_config
        mock_connect.side_effect = pg_errors.OperationalError("Connection failed")
        
        with self.assertRaises(pg_errors.OperationalError):
            s6.run_s6_sql()

    # ============================================================================
    # CONSTANTS AND CONFIGURATION TESTS
    # ============================================================================

    def test_constants(self):
        """Test that s6.py has the expected constants."""
        self.assertEqual(s6.SQL_FILE_PATH, "s6.sql")
        self.assertEqual(s6.CONFIG_FILE, "config.json")
        self.assertEqual(s6.MAX_RETRIES, 3)
        self.assertEqual(s6.RETRY_DELAY, 5)

    def test_logger_exists(self):
        """Test that logger is configured."""
        self.assertTrue(hasattr(s6, 'logger'))
        self.assertEqual(s6.logger.name, "s6_execution")

    # ============================================================================
    # EDGE CASES AND ERROR CONDITIONS
    # ============================================================================

    def test_execute_with_retry_zero_retries(self):
        """Test execute_with_retry with zero retries."""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.OperationalError("Error")
        
        with self.assertRaises(pg_errors.OperationalError):
            s6.execute_with_retry(mock_cursor, "SELECT 1;", retries=0)

    def test_parse_sql_statements_empty_input_detailed(self):
        """Test SQL parsing with empty input (detailed)."""
        statements = s6.parse_sql_statements("")
        self.assertEqual(statements, [])
        
        statements = s6.parse_sql_statements("   \n\n   ")
        self.assertEqual(statements, [])

    def test_parse_sql_statements_only_comments(self):
        """Test SQL parsing with only comments."""
        sql = """
        -- This is a comment
        /* Another comment */
        -- Final comment
        """
        
        statements = s6.parse_sql_statements(sql)
        self.assertEqual(statements, [])


class TestS6Integration(unittest.TestCase):
    """Integration tests for s6.py."""
    
    def setUp(self):
        """Set up integration test environment."""
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
        """Test actual config file loading."""
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
            original_config_file = s6.CONFIG_FILE
            s6.CONFIG_FILE = temp_config_path
            
            loaded_config = s6.load_config()
            self.assertEqual(loaded_config, config_data)
            
        finally:
            s6.CONFIG_FILE = original_config_file
            os.unlink(temp_config_path)

    @unittest.skipUnless(os.getenv("RUN_INTEGRATION_TESTS"), "Integration tests disabled")
    def test_sql_parsing_with_real_file(self):
        """Test SQL parsing with a real file."""
        sql_content = """
        CREATE SCHEMA IF NOT EXISTS faers_b;
        
        CREATE OR REPLACE FUNCTION faers_b.test_function()
        RETURNS TEXT AS $$
        BEGIN
            RETURN 'test';
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE TABLE IF NOT EXISTS faers_b.test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100)
        );
        
        DO $$
        BEGIN
            INSERT INTO faers_b.test_table (name) VALUES ('test');
        END $$;
        """
        
        statements = s6.parse_sql_statements(sql_content)
        
        self.assertEqual(len(statements), 4)
        
        # Verify function is parsed correctly
        function_statement = [s for s in statements if 'CREATE OR REPLACE FUNCTION' in s][0]
        self.assertIn('LANGUAGE plpgsql', function_statement)
        
        # Verify DO block is parsed correctly
        do_statement = [s for s in statements if 'DO $$' in s][0]
        self.assertIn('BEGIN', do_statement)
        self.assertIn('END $$', do_statement)


if __name__ == '__main__':
    print("Running s6.py unit tests...")
    print("This tests the s6.py Python script for FAERS pipeline step 6")
    print()
    print("For integration tests, set environment variables:")
    print("  TEST_DB_HOST, TEST_DB_USER, TEST_DB_NAME, TEST_DB_PASSWORD")
    print("  RUN_INTEGRATION_TESTS=1")
    print()
    
    # Create a test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestS6Pipeline))
    suite.addTests(loader.loadTestsFromTestCase(TestS6Integration))
    
    # Run tests with verbose output
    runner = unittest.TextTestRunner(verbosity=2, buffer=True)
    result = runner.run(suite)
    
    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)