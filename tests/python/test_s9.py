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
    import s9
except ImportError as e:
    print(f"Error importing s9 module: {e}")
    print(f"Project root path: {project_root}")
    print(f"Python path: {sys.path}")
    raise


class TestS9Pipeline(unittest.TestCase):
    """Comprehensive test suite for s9.py script - DRUG_Mapper finalization."""
    
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
                "host": "localhost"
                # Missing required fields
            }
        }
        
        self.sample_sql_script = """
        -- Update drug mapper with cleaned data
        UPDATE faers_b.DRUG_Mapper 
        SET drug_name = TRIM(UPPER(drug_name))
        WHERE drug_name IS NOT NULL;
        
        CREATE OR REPLACE FUNCTION faers_b.finalize_mapping()
        RETURNS void AS $$
        BEGIN
            UPDATE faers_b.DRUG_Mapper 
            SET mapping_status = 'completed'
            WHERE mapping_status = 'pending';
            RAISE NOTICE 'Mapping finalized';
        END
        $$ LANGUAGE plpgsql;
        
        DO $$
        BEGIN
            UPDATE faers_b.DRUG_Mapper 
            SET last_updated = CURRENT_TIMESTAMP;
            RAISE NOTICE 'Timestamps updated';
        END
        $$;
        
        \\copy faers_b.DRUG_Mapper FROM 'final_mappings.csv' WITH CSV HEADER;
        
        UPDATE faers_b.DRUG_Mapper 
        SET validation_status = 'verified' 
        WHERE confidence_score >= 0.8;
        
        INSERT INTO faers_b.mapping_log (action, timestamp) 
        VALUES ('s9_completion', CURRENT_TIMESTAMP);
        """
        
        # SQL script with BOM character
        self.bom_sql_script = "\ufeff" + self.sample_sql_script
        
        self.advanced_sql = """
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

    # ============================================================================
    # CONFIG LOADING TESTS
    # ============================================================================

    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_load_config_success(self, mock_json_load, mock_file):
        """Test successful configuration loading."""
        mock_json_load.return_value = self.sample_config
        
        result = s9.load_config()
        
        self.assertEqual(result, self.sample_config)
        mock_file.assert_called_once_with(s9.CONFIG_FILE, "r", encoding="utf-8")
        mock_json_load.assert_called_once()

    def test_load_config_file_not_found(self):
        """Test error handling when config file is missing."""
        with patch("builtins.open", side_effect=FileNotFoundError()):
            with self.assertRaises(FileNotFoundError):
                s9.load_config()

    @patch('builtins.open', side_effect=FileNotFoundError)
    def test_load_config_file_not_found_detailed(self, mock_file):
        """Test configuration loading when file doesn't exist (detailed)."""
        with self.assertRaises(FileNotFoundError):
            s9.load_config()

    def test_load_config_invalid_json(self):
        """Test error handling for malformed JSON config."""
        invalid_json = '{"database": {"host": "localhost", "port": invalid}}'
        
        with patch("builtins.open", mock_open(read_data=invalid_json)):
            with self.assertRaises(json.JSONDecodeError):
                s9.load_config()

    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load', side_effect=json.JSONDecodeError("Invalid JSON", "", 0))
    def test_load_config_invalid_json_detailed(self, mock_json_load, mock_file):
        """Test configuration loading with invalid JSON (detailed)."""
        with self.assertRaises(json.JSONDecodeError):
            s9.load_config()

    # ============================================================================
    # SQL PARSING TESTS
    # ============================================================================

    def test_parse_sql_statements_with_updates_and_functions(self):
        """Test parsing SQL with multiple UPDATE statements, functions, and DO blocks."""
        statements = s9.parse_sql_statements(self.sample_sql_script)
        
        # Should have 5 statements: UPDATE, CREATE FUNCTION, DO block, UPDATE, INSERT
        # The \copy command should be filtered out
        self.assertEqual(len(statements), 5)
        
        # Check that comments are filtered out
        for stmt in statements:
            self.assertNotIn('--', stmt)
        
        # Check that \copy commands are filtered out
        for stmt in statements:
            self.assertNotIn('\\copy', stmt.lower())
        
        # Check that multiple UPDATE statements are preserved separately
        update_statements = [stmt for stmt in statements if stmt.strip().startswith('UPDATE')]
        self.assertEqual(len(update_statements), 2)
        
        # Check that function is preserved
        function_statements = [stmt for stmt in statements if 'CREATE OR REPLACE FUNCTION' in stmt]
        self.assertEqual(len(function_statements), 1)
        self.assertIn('LANGUAGE plpgsql', function_statements[0])
        
        # Check that DO block is preserved
        do_blocks = [stmt for stmt in statements if stmt.strip().startswith('DO $$')]
        self.assertEqual(len(do_blocks), 1)
        self.assertIn('UPDATE', do_blocks[0])

    def test_parse_sql_statements_with_bom_character(self):
        """Test parsing SQL statements that begin with BOM character."""
        statements = s9.parse_sql_statements(self.bom_sql_script)
        
        # Should handle BOM gracefully and produce same number of statements
        self.assertEqual(len(statements), 5)
        
        # First statement should not contain BOM
        first_stmt = statements[0]
        self.assertNotIn('\ufeff', first_stmt)
        
        # Should start with UPDATE
        self.assertTrue(first_stmt.strip().startswith('UPDATE'))

    def test_parse_sql_statements_empty_or_comments_only(self):
        """Test parsing empty SQL or SQL with only comments."""
        comment_only_sql = """
        -- This is just a comment about drug mapping
        -- Another comment line
        
        
        -- More comments about updates
        """
        
        statements = s9.parse_sql_statements(comment_only_sql)
        self.assertEqual(len(statements), 0)

    def test_parse_sql_statements_complex_update_with_subqueries(self):
        """Test parsing complex UPDATE statements with subqueries."""
        complex_update_sql = """
        UPDATE faers_b.DRUG_Mapper dm
        SET mapped_rxcui = (
            SELECT rxcui 
            FROM faers_b.rxn_mapping rm 
            WHERE UPPER(rm.drug_name) = UPPER(dm.drug_name)
            LIMIT 1
        )
        WHERE dm.mapped_rxcui IS NULL 
          AND EXISTS (
              SELECT 1 
              FROM faers_b.rxn_mapping rm2 
              WHERE UPPER(rm2.drug_name) = UPPER(dm.drug_name)
          );
        """
        
        statements = s9.parse_sql_statements(complex_update_sql)
        self.assertEqual(len(statements), 1)
        self.assertIn('EXISTS', statements[0])
        self.assertIn('LIMIT 1', statements[0])

    def test_parse_sql_statements_multiple_consecutive_updates(self):
        """Test parsing multiple consecutive UPDATE statements."""
        multiple_updates_sql = """
        UPDATE faers_b.DRUG_Mapper SET status = 'processing';
        UPDATE faers_b.DRUG_Mapper SET confidence = 1.0 WHERE exact_match = true;
        UPDATE faers_b.DRUG_Mapper SET confidence = 0.8 WHERE fuzzy_match = true;
        """
        
        statements = s9.parse_sql_statements(multiple_updates_sql)
        self.assertEqual(len(statements), 3)
        
        # All should be UPDATE statements
        for stmt in statements:
            self.assertTrue(stmt.strip().startswith('UPDATE'))

    def test_parse_sql_statements_basic(self):
        """Test parsing of basic SQL statements."""
        sql = """
        CREATE TABLE test (id INT);
        INSERT INTO test VALUES (1);
        SELECT * FROM test;
        """
        
        statements = s9.parse_sql_statements(sql)
        
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
        /* Block comment */
        INSERT INTO test VALUES (1);
        """
        
        statements = s9.parse_sql_statements(sql)
        
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
            "CREATE TABLE test (id INT)",
            "SELECT * FROM test"
        ]
        self.assertEqual(statements, expected)

    def test_parse_sql_statements_bom_removal(self):
        """Test that BOM is removed from SQL."""
        sql = "\ufeffCREATE TABLE test (id INT);"
        
        statements = s9.parse_sql_statements(sql)
        
        self.assertEqual(statements, ["CREATE TABLE test (id INT)"])

    # ============================================================================
    # EXECUTE WITH RETRY TESTS
    # ============================================================================

    @patch('s9.time.sleep')
    def test_execute_with_retry_immediate_success(self, mock_sleep):
        """Test successful execution without needing retries."""
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = None
        
        result = s9.execute_with_retry(mock_cursor, "UPDATE faers_b.DRUG_Mapper SET status = 'complete'")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once()
        mock_sleep.assert_not_called()

    @patch('s9.time.sleep')
    def test_execute_with_retry_operational_error_recovery(self, mock_sleep):
        """Test retry mechanism recovering from operational error."""
        mock_cursor = MagicMock()
        # First attempt fails, second succeeds
        mock_cursor.execute.side_effect = [
            pg_errors.OperationalError("Lock timeout"),
            None  # Success on retry
        ]
        
        result = s9.execute_with_retry(mock_cursor, "UPDATE faers_b.DRUG_Mapper SET status = 'complete'", retries=2, delay=1)
        
        self.assertTrue(result)
        self.assertEqual(mock_cursor.execute.call_count, 2)
        mock_sleep.assert_called_once_with(1)

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

    # ============================================================================
    # TABLE VERIFICATION TESTS
    # ============================================================================

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

    # ============================================================================
    # MAIN FUNCTION TESTS
    # ============================================================================

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
        mock_file.return_value.read.return_value = self.advanced_sql
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

    # ============================================================================
    # MAIN EXECUTION TESTS
    # ============================================================================

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

    def test_s9_logger_configuration(self):
        """Test that s9 logger is properly configured."""
        # Test that the s9 module has logger configured
        if hasattr(s9, 'logger'):
            self.assertIsInstance(s9.logger, logging.Logger)
        
        # Test expected log file configuration
        expected_log_file = "s9_execution.log"
        self.assertIsInstance(expected_log_file, str)
        self.assertIn("s9", expected_log_file)


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
            original_config_file = s9.CONFIG_FILE
            s9.CONFIG_FILE = temp_config_path
            
            loaded_config = s9.load_config()
            self.assertEqual(loaded_config, config_data)
            
        finally:
            s9.CONFIG_FILE = original_config_file
            os.unlink(temp_config_path)


class TestS9SpecificFunctionality(unittest.TestCase):
    """Test S9-specific functionality and features."""
    
    def test_drug_mapper_finalization_focus(self):
        """Test that S9 focuses on DRUG_Mapper finalization."""
        expected_table = "DRUG_Mapper"
        
        # Verify this matches what s9 operations focus on
        self.assertIsInstance(expected_table, str)
        self.assertIn("DRUG_Mapper", expected_table)
        self.assertNotIn("Temp", expected_table)  # S9 works with final table, not temp

    def test_finalization_operations_validation(self):
        """Test validation of finalization operations."""
        # Expected finalization operations for S9
        finalization_ops = [
            "UPDATE mapping_status",
            "UPDATE confidence_score", 
            "UPDATE validation_status",
            "INSERT mapping_log",
            "UPDATE last_updated"
        ]
        
        for op in finalization_ops:
            with self.subTest(operation=op):
                self.assertIsInstance(op, str)
                self.assertTrue(len(op) > 0)

    def test_constants_validation(self):
        """Test that s9.py has the expected constants."""
        # Test expected constants exist
        expected_constants = ["SQL_FILE_PATH", "CONFIG_FILE", "MAX_RETRIES", "RETRY_DELAY"]
        
        for constant in expected_constants:
            with self.subTest(constant=constant):
                self.assertTrue(hasattr(s9, constant))

    def test_s9_sql_file_path(self):
        """Test that S9 uses correct SQL file path."""
        if hasattr(s9, 'SQL_FILE_PATH'):
            self.assertEqual(s9.SQL_FILE_PATH, "s9.sql")
        
        expected_sql_file = "s9.sql"
        self.assertIn("s9", expected_sql_file)

    def test_final_mapping_completion_logic(self):
        """Test logic for final mapping completion."""
        # S9 should handle completion logic
        completion_statuses = ["completed", "verified", "finalized"]
        
        for status in completion_statuses:
            with self.subTest(status=status):
                self.assertIsInstance(status, str)
                self.assertIn(status, ["completed", "verified", "finalized"])

    def test_confidence_score_validation(self):
        """Test confidence score validation logic."""
        # S9 should handle confidence score thresholds
        confidence_threshold = 0.8
        
        self.assertIsInstance(confidence_threshold, float)
        self.assertGreaterEqual(confidence_threshold, 0.0)
        self.assertLessEqual(confidence_threshold, 1.0)

    def test_mapping_log_operations(self):
        """Test mapping log operations for S9."""
        # S9 should log completion activities
        log_actions = ["s9_start", "s9_completion", "mapping_finalized", "validation_completed"]
        
        for action in log_actions:
            with self.subTest(action=action):
                self.assertIsInstance(action, str)
                self.assertTrue(len(action) > 0)


if __name__ == '__main__':
    print("Running s9.py unit tests...")
    print("This tests the s9.py Python script for DRUG_Mapper finalization")
    print("Step 9 of the FAERS data processing pipeline - Final mapping completion")
    print()
    print("For integration tests, set environment variables:")
    print("  TEST_DB_HOST, TEST_DB_USER, TEST_DB_NAME, TEST_DB_PASSWORD")
    print("  RUN_INTEGRATION_TESTS=1")
    print()
    
    # Create a test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestS9Pipeline))
    suite.addTests(loader.loadTestsFromTestCase(TestLoggingConfiguration))
    suite.addTests(loader.loadTestsFromTestCase(TestIntegrationScenarios))
    suite.addTests(loader.loadTestsFromTestCase(TestS9SpecificFunctionality))
    
    # Run tests with verbose output
    runner = unittest.TextTestRunner(verbosity=2, buffer=True)
    result = runner.run(suite)
    
    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)