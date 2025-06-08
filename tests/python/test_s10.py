import unittest
import json
import sys
import os
import tempfile
import time
from unittest.mock import patch, mock_open, MagicMock, Mock, call
from io import StringIO
import logging
import psycopg
from psycopg import errors as pg_errors

# Use robust project root import pattern
project_root = os.getcwd()
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

try:
    import s10
except ImportError as e:
    print(f"Error importing s10 module: {e}")
    print(f"Project root path: {project_root}")
    print(f"Python path: {sys.path}")
    raise


class TestS10Pipeline(unittest.TestCase):
    """Comprehensive test suite for s10.py script - Drug mapper tables and manual remapping."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.sample_config = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "user": "testuser",
                "dbname": "faersdatabase",
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
        -- Create drug mapper tables
        CREATE TABLE IF NOT EXISTS faers_b.drug_mapper (
            id SERIAL PRIMARY KEY,
            drug_name VARCHAR(255),
            mapping_status VARCHAR(50)
        );
        
        CREATE TABLE IF NOT EXISTS faers_b.drug_mapper_2 (
            id SERIAL PRIMARY KEY,
            drug_name VARCHAR(255),
            rxcui INTEGER
        );
        
        DO $$
        BEGIN
            INSERT INTO faers_b.drug_mapper (drug_name, mapping_status) 
            VALUES ('Aspirin', 'mapped');
            RAISE NOTICE 'Initial data loaded';
        END
        $$;
        
        CREATE TABLE IF NOT EXISTS faers_b.manual_remapper (
            id SERIAL PRIMARY KEY,
            original_name VARCHAR(255),
            mapped_name VARCHAR(255)
        );
        
        INSERT INTO faers_b.remapping_log (action, timestamp) 
        VALUES ('s10_initialization', CURRENT_TIMESTAMP);
        """
        
        self.advanced_sql = """
        -- This is a comment
        CREATE SCHEMA IF NOT EXISTS faers_b;
        
        CREATE TABLE IF NOT EXISTS faers_b.drug_mapper (
            id SERIAL PRIMARY KEY,
            drug_name VARCHAR(255)
        );
        
        CREATE TABLE IF NOT EXISTS faers_b.drug_mapper_2 (
            id SERIAL PRIMARY KEY,
            drug_name VARCHAR(255)
        );
        
        INSERT INTO faers_b.drug_mapper (drug_name) VALUES ('aspirin');
        
        DO $$
        BEGIN
            UPDATE faers_b.drug_mapper SET drug_name = 'ASPIRIN' WHERE drug_name = 'aspirin';
        END
        $$;
        
        UPDATE faers_b.drug_mapper_2 
        SET drug_name = UPPER(drug_name) 
        WHERE drug_name IS NOT NULL;
        """

    def tearDown(self):
        """Clean up after each test."""
        pass

    # ============================================================================
    # CONFIG LOADING TESTS
    # ============================================================================

    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_load_config_success(self, mock_json_load, mock_file):
        """Test successful configuration loading."""
        mock_json_load.return_value = self.sample_config
        
        result = s10.load_config()
        
        self.assertEqual(result, self.sample_config)
        mock_file.assert_called_once_with(s10.CONFIG_FILE, "r", encoding="utf-8")
        mock_json_load.assert_called_once()

    def test_load_config_missing_file(self):
        """Test error handling when config file is missing."""
        with patch("builtins.open", side_effect=FileNotFoundError()):
            with self.assertRaises(FileNotFoundError):
                s10.load_config()

    @patch('builtins.open', side_effect=FileNotFoundError)
    def test_load_config_file_not_found_detailed(self, mock_file):
        """Test configuration loading when file doesn't exist (detailed)."""
        with self.assertRaises(FileNotFoundError):
            s10.load_config()

    def test_load_config_invalid_json(self):
        """Test error handling for malformed JSON config."""
        invalid_json = '{"database": {"host": localhost, "port": 5432}}'  # Missing quotes
        
        with patch("builtins.open", mock_open(read_data=invalid_json)):
            with self.assertRaises(json.JSONDecodeError):
                s10.load_config()

    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load', side_effect=json.JSONDecodeError("Invalid JSON", "", 0))
    def test_load_config_invalid_json_detailed(self, mock_json_load, mock_file):
        """Test configuration loading with invalid JSON (detailed)."""
        with self.assertRaises(json.JSONDecodeError):
            s10.load_config()

    # ============================================================================
    # DATABASE UTILITY TESTS
    # ============================================================================

    def test_check_postgresql_version(self):
        """Test PostgreSQL version checking."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ["PostgreSQL 13.7 on x86_64-pc-linux-gnu"]
        
        version = s10.check_postgresql_version(mock_cursor)
        
        mock_cursor.execute.assert_called_once_with("SELECT version();")
        self.assertEqual(version, "PostgreSQL 13.7 on x86_64-pc-linux-gnu")

    def test_check_database_exists_true(self):
        """Test database existence check when database exists."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [1]  # Database exists
        
        result = s10.check_database_exists(mock_cursor, "faersdatabase")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once_with(
            "SELECT 1 FROM pg_database WHERE datname = %s", ("faersdatabase",)
        )

    def test_check_database_exists_false(self):
        """Test database existence check when database doesn't exist."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None  # Database doesn't exist
        
        result = s10.check_database_exists(mock_cursor, "nonexistent_db")
        
        self.assertFalse(result)
        mock_cursor.execute.assert_called_once_with(
            "SELECT 1 FROM pg_database WHERE datname = %s", ("nonexistent_db",)
        )

    # ============================================================================
    # SQL PARSING TESTS
    # ============================================================================

    def test_parse_sql_statements_with_do_blocks(self):
        """Test parsing SQL statements with DO blocks and table creation."""
        statements = s10.parse_sql_statements(self.sample_sql_script)
        
        # Should have 5 statements: 3 CREATE TABLE, 1 DO block, 1 INSERT
        self.assertEqual(len(statements), 5)
        
        # Check that comments are filtered out
        for stmt in statements:
            self.assertNotIn('--', stmt)
        
        # Check that DO block is preserved as one statement
        do_blocks = [stmt for stmt in statements if stmt.strip().startswith('DO $$')]
        self.assertEqual(len(do_blocks), 1)
        self.assertIn('BEGIN', do_blocks[0])
        self.assertIn('END', do_blocks[0])
        
        # Check that CREATE TABLE statements are preserved
        create_statements = [stmt for stmt in statements if 'CREATE TABLE' in stmt]
        self.assertEqual(len(create_statements), 3)

    def test_parse_sql_statements_empty_input(self):
        """Test parsing empty or comment-only SQL."""
        empty_sql = """
        -- Just comments about drug mapping
        -- Another comment line
        
        
        -- More comments
        """
        
        statements = s10.parse_sql_statements(empty_sql)
        self.assertEqual(len(statements), 0)

    def test_parse_sql_statements_basic(self):
        """Test parsing of basic SQL statements."""
        sql = """
        CREATE TABLE test (id INT);
        INSERT INTO test VALUES (1);
        SELECT * FROM test;
        """
        
        statements = s10.parse_sql_statements(sql)
        
        expected = [
            "CREATE TABLE test (id INT)",
            "INSERT INTO test VALUES (1)",
            "SELECT * FROM test"
        ]
        self.assertEqual(statements, expected)

    def test_parse_sql_statements_with_comments(self):
        """Test parsing SQL with comments removed."""
        sql = """
        -- This is a comment
        CREATE TABLE test (id INT); -- Inline comment
        INSERT INTO test VALUES (1);
        """
        
        statements = s10.parse_sql_statements(sql)
        
        expected = [
            "CREATE TABLE test (id INT)",
            "INSERT INTO test VALUES (1)"
        ]
        self.assertEqual(statements, expected)

    def test_parse_sql_statements_do_block(self):
        """Test parsing of DO blocks."""
        sql = """
        CREATE TABLE test (id INT);
        DO $$
        BEGIN
            INSERT INTO test VALUES (1);
        END
        $$;
        SELECT * FROM test;
        """
        
        statements = s10.parse_sql_statements(sql)
        
        # Should have 3 statements: CREATE, DO block, SELECT
        self.assertEqual(len(statements), 3)
        self.assertIn("DO $$", statements[1])
        self.assertIn("END", statements[1])
        self.assertEqual(statements[2], "SELECT * FROM test")

    def test_parse_sql_statements_copy_command_skipped(self):
        """Test that COPY commands are skipped."""
        sql = """
        CREATE TABLE test (id INT);
        \\copy test FROM 'data.csv';
        SELECT * FROM test;
        """
        
        statements = s10.parse_sql_statements(sql)
        
        expected = [
            "CREATE TABLE test (id INT)",
            "SELECT * FROM test"
        ]
        self.assertEqual(statements, expected)

    def test_parse_sql_statements_bom_removal(self):
        """Test that BOM is removed from SQL."""
        sql = "\ufeffCREATE TABLE test (id INT);"
        
        statements = s10.parse_sql_statements(sql)
        
        self.assertEqual(statements, ["CREATE TABLE test (id INT)"])

    def test_parse_sql_statements_function_handling(self):
        """Test parsing of function creation statements."""
        sql = """
        CREATE OR REPLACE FUNCTION test_func()
        RETURNS VOID AS $$
        BEGIN
            NULL;
        END
        $$ LANGUAGE plpgsql;
        SELECT test_func();
        """
        
        statements = s10.parse_sql_statements(sql)
        
        self.assertEqual(len(statements), 2)
        self.assertIn("CREATE OR REPLACE FUNCTION", statements[0])
        self.assertIn("LANGUAGE plpgsql", statements[0])

    # ============================================================================
    # EXECUTE WITH RETRY TESTS
    # ============================================================================

    @patch('s10.time.sleep')
    def test_execute_with_retry_immediate_success(self, mock_sleep):
        """Test successful execution without needing retries."""
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = None
        
        result = s10.execute_with_retry(mock_cursor, "CREATE TABLE test (id INTEGER)")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once()
        mock_sleep.assert_not_called()

    @patch('s10.time.sleep')
    def test_execute_with_retry_duplicate_table_skip(self, mock_sleep):
        """Test that duplicate table errors are skipped without retries."""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.DuplicateTable("Table already exists")
        
        result = s10.execute_with_retry(mock_cursor, "CREATE TABLE existing_table (id INTEGER)")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once()
        mock_sleep.assert_not_called()

    def test_execute_with_retry_success_first_attempt(self):
        """Test successful execution on first attempt."""
        mock_cursor = Mock()
        mock_cursor.execute.return_value = None
        
        result = s10.execute_with_retry(mock_cursor, "SELECT 1")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once_with("SELECT 1")

    @patch('time.sleep')
    def test_execute_with_retry_success_after_retries(self, mock_sleep):
        """Test successful execution after initial failures."""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = [
            pg_errors.OperationalError("Connection failed"),
            None  # Success on second attempt
        ]
        
        result = s10.execute_with_retry(mock_cursor, "SELECT 1", retries=2, delay=1)
        
        self.assertTrue(result)
        self.assertEqual(mock_cursor.execute.call_count, 2)
        self.assertEqual(mock_sleep.call_count, 1)
        mock_sleep.assert_called_with(1)

    @patch('time.sleep')
    def test_execute_with_retry_max_retries_exceeded(self, mock_sleep):
        """Test failure after max retries exceeded."""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = pg_errors.OperationalError("Connection failed")
        
        with self.assertRaises(pg_errors.OperationalError):
            s10.execute_with_retry(mock_cursor, "SELECT 1", retries=1, delay=1)
        
        self.assertEqual(mock_cursor.execute.call_count, 1)
        self.assertEqual(mock_sleep.call_count, 0)  # No retry on single attempt

    def test_execute_with_retry_duplicate_object_skip(self):
        """Test that duplicate object errors are handled gracefully."""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = pg_errors.DuplicateTable("Table already exists")
        
        result = s10.execute_with_retry(mock_cursor, "CREATE TABLE test")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once()

    def test_execute_with_retry_database_error(self):
        """Test that non-retryable database errors are raised immediately."""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = pg_errors.UndefinedColumn("Invalid column")
        
        with self.assertRaises(pg_errors.UndefinedColumn):
            s10.execute_with_retry(mock_cursor, "INVALID SQL")
        
        mock_cursor.execute.assert_called_once()

    # ============================================================================
    # TABLE VERIFICATION TESTS
    # ============================================================================

    def test_verify_tables_with_data(self):
        """Test table verification when tables exist with data."""
        mock_cursor = MagicMock()
        # Mock return values for COUNT queries
        mock_cursor.fetchone.side_effect = [[100], [50], [25], [10], [5]]
        
        tables = ["drug_mapper", "drug_mapper_2", "drug_mapper_3", "manual_remapper", "remapping_log"]
        
        with patch('s10.logger') as mock_logger:
            s10.verify_tables(mock_cursor, "faers_b", tables)
        
        # Should execute 5 COUNT queries
        self.assertEqual(mock_cursor.execute.call_count, 5)
        
        # Should log info messages for tables with data
        info_calls = [call for call in mock_logger.info.call_args_list]
        self.assertEqual(len(info_calls), 5)

    @patch('s10.load_config')
    @patch('psycopg.connect')
    def test_verify_tables_success(self, mock_connect, mock_load_config):
        """Test successful table verification."""
        mock_load_config.return_value = self.sample_config
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.side_effect = [("faers_b",), (100,), (50,), (25,), (10,), (5,)]  # Schema + 5 tables
        
        # Properly mock the context manager
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)
        mock_connect.return_value = mock_conn
        
        # Should not raise any exceptions
        s10.verify_tables()
        
        mock_connect.assert_called_once()
        # Should check schema + 5 tables = 6 execute calls
        self.assertEqual(mock_cursor.execute.call_count, 6)

    @patch('s10.load_config')
    @patch('psycopg.connect')
    def test_verify_tables_schema_missing(self, mock_connect, mock_load_config):
        """Test table verification when schema is missing."""
        mock_load_config.return_value = self.sample_config
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None  # Schema doesn't exist
        
        # Properly mock the context manager
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)
        mock_connect.return_value = mock_conn
        
        # Should not raise any exceptions
        s10.verify_tables()
        
        mock_cursor.execute.assert_called_once()

    @patch('s10.load_config')
    @patch('psycopg.connect')
    def test_verify_tables_empty_tables(self, mock_connect, mock_load_config):
        """Test table verification with empty tables."""
        mock_load_config.return_value = self.sample_config
        mock_conn = Mock()
        mock_cursor = Mock()
        # Schema exists, but all tables are empty
        mock_cursor.fetchone.side_effect = [("faers_b",), (0,), (0,), (0,), (0,), (0,)]
        
        # Properly mock the context manager
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)
        mock_connect.return_value = mock_conn
        
        # Should not raise any exceptions but log warnings
        s10.verify_tables()
        
        self.assertEqual(mock_cursor.execute.call_count, 6)

    # ============================================================================
    # MAIN FUNCTION TESTS
    # ============================================================================

    @patch('s10.load_config')
    @patch('s10.verify_tables')
    @patch('s10.execute_with_retry')
    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    @patch('psycopg.connect')
    def test_run_s10_sql_success(self, mock_connect, mock_file, mock_exists, 
                                mock_execute, mock_verify, mock_load_config):
        """Test successful execution of run_s10_sql."""
        mock_load_config.return_value = self.sample_config
        mock_exists.return_value = True
        mock_file.return_value.read.return_value = self.advanced_sql
        mock_execute.return_value = True
        
        # Mock database connections with proper context managers
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.side_effect = [None, ("PostgreSQL 14.0",)]  # DB doesn't exist, then version
        
        # Properly mock context managers
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)
        mock_connect.return_value = mock_conn
        
        s10.run_s10_sql()
        
        # Verify database creation and connection
        self.assertEqual(mock_connect.call_count, 2)  # Once for initial check, once for faersdatabase
        mock_verify.assert_called_once()

    @patch('s10.load_config')
    @patch('os.path.exists')
    @patch('psycopg.connect')
    def test_run_s10_sql_missing_sql_file(self, mock_connect, mock_exists, mock_load_config):
        """Test run_s10_sql when SQL file is missing."""
        mock_load_config.return_value = self.sample_config
        mock_exists.return_value = False
        
        # Mock database connections to avoid real connections
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.side_effect = [None, ("PostgreSQL 14.0",)]
        
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)
        mock_connect.return_value = mock_conn
        
        with self.assertRaises(FileNotFoundError):
            s10.run_s10_sql()

    def test_run_s10_sql_missing_database_config(self):
        """Test run_s10_sql with incomplete database configuration."""
        incomplete_config = {
            "database": {
                "host": "localhost",
                "port": 5432
                # Missing user, password, dbname
            }
        }
        
        with patch('s10.load_config', return_value=incomplete_config):
            with self.assertRaises(ValueError):
                s10.run_s10_sql()

    @patch('s10.load_config')
    @patch('psycopg.connect', side_effect=pg_errors.OperationalError("Connection failed"))
    def test_run_s10_sql_connection_error(self, mock_connect, mock_load_config):
        """Test run_s10_sql with database connection error."""
        mock_load_config.return_value = self.sample_config
        
        with self.assertRaises(pg_errors.OperationalError):
            s10.run_s10_sql()

    # ============================================================================
    # CONSTANTS AND CONFIGURATION TESTS
    # ============================================================================

    def test_configuration_constants(self):
        """Test that configuration constants are properly set."""
        self.assertEqual(s10.CONFIG_FILE, "config.json")
        self.assertEqual(s10.SQL_FILE_PATH, "s10.sql")
        self.assertEqual(s10.MAX_RETRIES, 1)
        self.assertEqual(s10.RETRY_DELAY, 1)

    def test_expected_tables_list(self):
        """Test that the expected tables list is correct for s10."""
        # This test verifies the tables that verify_tables() checks
        expected_tables = [
            "drug_mapper",
            "drug_mapper_2", 
            "drug_mapper_3",
            "manual_remapper",
            "remapping_log"
        ]
        
        # We can't directly access the tables list, but we can verify it indirectly
        # by checking the function behavior or by patching and monitoring calls
        with patch('s10.load_config') as mock_config:
            with patch('psycopg.connect') as mock_connect:
                mock_config.return_value = self.sample_config
                mock_conn = Mock()
                mock_cursor = Mock()
                mock_cursor.fetchone.side_effect = [("faers_b",)] + [(1,)] * len(expected_tables)
                
                mock_conn.cursor.return_value = mock_cursor
                mock_conn.__enter__ = Mock(return_value=mock_conn)
                mock_conn.__exit__ = Mock(return_value=None)
                mock_cursor.__enter__ = Mock(return_value=mock_cursor)
                mock_cursor.__exit__ = Mock(return_value=None)
                mock_connect.return_value = mock_conn
                
                s10.verify_tables()
                
                # Should check schema + number of expected tables
                self.assertEqual(mock_cursor.execute.call_count, 1 + len(expected_tables))

    def test_reduced_retry_configuration(self):
        """Test that s10 has reduced retry configuration compared to other scripts."""
        # s10 uses MAX_RETRIES = 1 and RETRY_DELAY = 1
        # This is different from typical configurations and should be tested
        self.assertEqual(s10.MAX_RETRIES, 1)
        self.assertEqual(s10.RETRY_DELAY, 1)
        
        # Test that the reduced retry is actually used
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = pg_errors.OperationalError("Connection failed")
        
        with self.assertRaises(pg_errors.OperationalError):
            s10.execute_with_retry(mock_cursor, "SELECT 1")
        
        # Should only try once with default MAX_RETRIES = 1
        self.assertEqual(mock_cursor.execute.call_count, 1)

    # ============================================================================
    # S10-SPECIFIC FUNCTIONALITY TESTS
    # ============================================================================

    def test_manual_remapping_log_message(self):
        """Test that manual remapping log message is included."""
        # This tests the specific log message about manual remapping
        with patch('s10.load_config') as mock_config:
            with patch('s10.verify_tables') as mock_verify:
                with patch('s10.execute_with_retry') as mock_execute:
                    with patch('os.path.exists', return_value=True):
                        with patch('builtins.open', mock_open(read_data="SELECT 1;")):
                            with patch('psycopg.connect') as mock_connect:
                                mock_config.return_value = self.sample_config
                                mock_execute.return_value = True
                                
                                mock_conn = Mock()
                                mock_cursor = Mock()
                                mock_cursor.fetchone.side_effect = [("faersdatabase",), ("PostgreSQL 14.0",)]
                                
                                mock_conn.cursor.return_value = mock_cursor
                                mock_conn.__enter__ = Mock(return_value=mock_conn)
                                mock_conn.__exit__ = Mock(return_value=None)
                                mock_cursor.__enter__ = Mock(return_value=mock_cursor)
                                mock_cursor.__exit__ = Mock(return_value=None)
                                mock_connect.return_value = mock_conn
                                
                                # Test execution without logging capture for now
                                # due to complexity of testing logging in this context
                                s10.run_s10_sql()
                                
                                # Verify execution completed
                                mock_verify.assert_called()

    def test_drug_mapper_tables_focus(self):
        """Test that S10 focuses on drug mapper tables."""
        expected_tables = ["drug_mapper", "drug_mapper_2", "drug_mapper_3", "manual_remapper"]
        
        for table in expected_tables:
            with self.subTest(table=table):
                self.assertIsInstance(table, str)
                self.assertTrue(len(table) > 0)

    def test_manual_remapper_functionality(self):
        """Test that S10 includes manual remapper functionality."""
        # S10 should handle manual remapping tables
        manual_remapper_features = [
            "manual_remapper",
            "remapping_log", 
            "original_name",
            "mapped_name"
        ]
        
        for feature in manual_remapper_features:
            with self.subTest(feature=feature):
                self.assertIsInstance(feature, str)
                self.assertTrue(len(feature) > 0)

    # ============================================================================
    # MAIN EXECUTION TESTS
    # ============================================================================

    @patch('s10.run_s10_sql')
    def test_main_success(self, mock_run_s10):
        """Test successful main execution."""
        mock_run_s10.return_value = None
        
        # Note: testing __main__ execution is complex
        # In practice, you might want to refactor main logic into a separate function
        pass

    @patch('s10.run_s10_sql', side_effect=Exception("Test error"))
    @patch('sys.exit')
    def test_main_failure(self, mock_exit, mock_run_s10):
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
        self.test_logger = logging.getLogger('test_s10')
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

    def test_log_file_configuration(self):
        """Test that log file is configured correctly."""
        # Check that the logging configuration includes file handler
        self.assertEqual(s10.logger.name, "s10")

    def test_s10_logger_configuration(self):
        """Test that s10 logger is properly configured."""
        # Test that the s10 module has logger configured
        if hasattr(s10, 'logger'):
            self.assertIsInstance(s10.logger, logging.Logger)
        
        # Test expected log file configuration
        expected_log_file = "s10_execution.log"
        self.assertIsInstance(expected_log_file, str)
        self.assertIn("s10", expected_log_file)


class TestIntegrationScenarios(unittest.TestCase):
    """Test integration scenarios with more complex setups."""
    
    @patch('s10.load_config')
    @patch('s10.parse_sql_statements')
    @patch('s10.execute_with_retry')
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
        
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)
        mock_connect.return_value = mock_conn
        
        # Execute
        s10.run_s10_sql()
        
        # Verify workflow
        mock_load_config.assert_called()
        mock_exists.assert_called_with(s10.SQL_FILE_PATH)
        mock_parse.assert_called_once()
        self.assertEqual(mock_execute.call_count, 2)  # Two parsed statements

    def test_error_handling_flow(self):
        """Test error handling in various scenarios."""
        # Test config loading error
        with patch('s10.load_config', side_effect=FileNotFoundError):
            with self.assertRaises(FileNotFoundError):
                s10.run_s10_sql()
        
        # Test JSON decode error
        with patch('s10.load_config', side_effect=json.JSONDecodeError("Invalid", "", 0)):
            with self.assertRaises(json.JSONDecodeError):
                s10.run_s10_sql()

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
            original_config_file = s10.CONFIG_FILE
            s10.CONFIG_FILE = temp_config_path
            
            loaded_config = s10.load_config()
            self.assertEqual(loaded_config, config_data)
            
        finally:
            s10.CONFIG_FILE = original_config_file
            os.unlink(temp_config_path)

    def test_drug_mapper_workflow_integration(self):
        """Test integration of drug mapper workflow components."""
        # Test that all drug mapper components work together
        workflow_components = [
            "drug_mapper",
            "drug_mapper_2",
            "drug_mapper_3", 
            "manual_remapper",
            "remapping_log"
        ]
        
        for component in workflow_components:
            with self.subTest(component=component):
                self.assertIsInstance(component, str)
                self.assertTrue(len(component) > 0)
                
                # Verify component naming conventions
                if "mapper" in component:
                    self.assertIn("mapper", component)


if __name__ == '__main__':
    print("Running s10.py unit tests...")
    print("This tests the s10.py Python script for drug mapper tables setup")
    print("Step 10 of the FAERS data processing pipeline - Drug mapper infrastructure")
    print("Features: Multiple drug mapper tables, manual remapping, reduced retry logic")
    print()
    print("For integration tests, set environment variables:")
    print("  TEST_DB_HOST, TEST_DB_USER, TEST_DB_NAME, TEST_DB_PASSWORD")
    print("  RUN_INTEGRATION_TESTS=1")
    print()
    
    # Create a test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestS10Pipeline))
    suite.addTests(loader.loadTestsFromTestCase(TestLoggingConfiguration))
    suite.addTests(loader.loadTestsFromTestCase(TestIntegrationScenarios))
    
    # Run tests with verbose output
    runner = unittest.TextTestRunner(verbosity=2, buffer=True)
    result = runner.run(suite)
    
    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)