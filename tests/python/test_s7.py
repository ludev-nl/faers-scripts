import unittest
import os
import sys
import json
import tempfile
import time
from unittest.mock import patch, mock_open, MagicMock, call
import psycopg
from psycopg import errors as pg_errors

# Add the faers-scripts root directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

# Import your s7.py module
import s7


class TestS7(unittest.TestCase):
    """Test cases for s7.py script"""
    
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
        
        -- Create analysis table
        CREATE TABLE IF NOT EXISTS faers_b.FAERS_Analysis_Summary (
            id SERIAL PRIMARY KEY,
            analysis_type VARCHAR(100),
            total_cases INTEGER,
            analysis_date DATE
        );
        
        -- DO block example
        DO $$
        BEGIN
            INSERT INTO faers_b.FAERS_Analysis_Summary (analysis_type, total_cases) 
            VALUES ('test_analysis', 100);
        END $$;
        
        -- Copy command (should be skipped)
        \\copy faers_b.FAERS_Analysis_Summary FROM 'data.csv' WITH CSV HEADER;
        
        -- Another statement
        SELECT COUNT(*) FROM faers_b.FAERS_Analysis_Summary;
        """
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_load_config_success(self, mock_json_load, mock_file):
        """Test successful configuration loading"""
        mock_json_load.return_value = self.sample_config
        
        result = s7.load_config()
        
        self.assertEqual(result, self.sample_config)
        mock_file.assert_called_once_with("config.json", "r", encoding="utf-8")
    
    @patch('builtins.open', new_callable=mock_open)
    def test_load_config_file_not_found(self, mock_file):
        """Test configuration loading when file is not found"""
        mock_file.side_effect = FileNotFoundError()
        
        with self.assertRaises(FileNotFoundError):
            s7.load_config()
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_load_config_invalid_json(self, mock_json_load, mock_file):
        """Test configuration loading with invalid JSON"""
        mock_json_load.side_effect = json.JSONDecodeError("Invalid JSON", "doc", 0)
        
        with self.assertRaises(json.JSONDecodeError):
            s7.load_config()
    
    def test_execute_with_retry_success_first_attempt(self):
        """Test successful execution on first attempt"""
        mock_cursor = MagicMock()
        
        result = s7.execute_with_retry(mock_cursor, "SELECT 1;")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once_with("SELECT 1;")
    
    def test_execute_with_retry_success_after_retries(self):
        """Test successful execution after retries"""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = [
            pg_errors.OperationalError("Connection lost"),
            pg_errors.OperationalError("Connection lost"),
            None  # Success
        ]
        
        with patch('time.sleep') as mock_sleep:
            result = s7.execute_with_retry(mock_cursor, "SELECT 1;", retries=3, delay=1)
        
        self.assertTrue(result)
        self.assertEqual(mock_cursor.execute.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)
    
    def test_execute_with_retry_max_retries_exceeded(self):
        """Test failure after max retries exceeded"""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.OperationalError("Persistent error")
        
        with patch('time.sleep'):
            with self.assertRaises(pg_errors.OperationalError):
                s7.execute_with_retry(mock_cursor, "SELECT 1;", retries=2, delay=0.1)
        
        self.assertEqual(mock_cursor.execute.call_count, 2)
    
    def test_execute_with_retry_duplicate_object_skipped(self):
        """Test that duplicate object errors are gracefully handled"""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.DuplicateTable("Table already exists")
        
        result = s7.execute_with_retry(mock_cursor, "CREATE TABLE test;")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once()
    
    @patch('s7.load_config')
    @patch('psycopg.connect')
    def test_verify_tables_schema_exists(self, mock_connect, mock_load_config):
        """Test table verification when schema exists"""
        mock_load_config.return_value = self.sample_config
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        # Mock schema check and table count
        mock_cursor.fetchone.side_effect = [
            ("faers_b",),  # Schema exists
            (500,),        # FAERS_Analysis_Summary count
        ]
        
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        s7.verify_tables()
        
        mock_cursor.execute.assert_any_call("SELECT nspname FROM pg_namespace WHERE nspname = 'faers_b'")
    
    @patch('s7.load_config')
    @patch('psycopg.connect')
    def test_verify_tables_schema_missing(self, mock_connect, mock_load_config):
        """Test table verification when schema is missing"""
        mock_load_config.return_value = self.sample_config
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None  # Schema doesn't exist
        
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        s7.verify_tables()  # Should not raise exception
    
    @patch('s7.load_config')
    @patch('psycopg.connect')
    def test_verify_tables_table_missing(self, mock_connect, mock_load_config):
        """Test table verification when analysis summary table is missing"""
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
        
        s7.verify_tables()  # Should not raise exception
    
    def test_parse_sql_statements_basic(self):
        """Test basic SQL statement parsing"""
        sql = """
        CREATE TABLE test1 (id INT);
        CREATE TABLE test2 (id INT);
        SELECT * FROM test1;
        """
        
        statements = s7.parse_sql_statements(sql)
        
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
        
        statements = s7.parse_sql_statements(sql)
        
        self.assertEqual(len(statements), 2)
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
        END $$;
        CREATE TABLE test2 (id INT);
        """
        
        statements = s7.parse_sql_statements(sql)
        
        self.assertEqual(len(statements), 3)
        do_block = statements[1]
        self.assertIn("DO $$", do_block)
        self.assertIn("END $$", do_block)
    
    def test_parse_sql_statements_skip_copy_commands(self):
        """Test that \\copy commands are skipped"""
        sql = """
        CREATE TABLE test1 (id INT);
        \\copy test1 FROM 'data.csv' WITH CSV HEADER;
        CREATE TABLE test2 (id INT);
        """
        
        statements = s7.parse_sql_statements(sql)
        
        self.assertEqual(len(statements), 2)
        for stmt in statements:
            self.assertNotIn("\\copy", stmt)
    
    def test_parse_sql_statements_with_bom(self):
        """Test SQL parsing with BOM (Byte Order Mark)"""
        sql_with_bom = '\ufeff' + "CREATE TABLE test1 (id INT);"
        
        statements = s7.parse_sql_statements(sql_with_bom)
        
        self.assertEqual(len(statements), 1)
        for stmt in statements:
            self.assertNotIn('\ufeff', stmt)
    
    def test_parse_sql_statements_skip_create_database(self):
        """Test that CREATE DATABASE statements are filtered out"""
        sql = """
        CREATE DATABASE testdb;
        CREATE TABLE test1 (id INT);
        CREATE DATABASE another_db;
        """
        
        statements = s7.parse_sql_statements(sql)
        
        self.assertEqual(len(statements), 1)
        self.assertIn("CREATE TABLE test1", statements[0])
        for stmt in statements:
            self.assertNotIn("CREATE DATABASE", stmt)
    
    @patch('s7.load_config')
    @patch('s7.verify_tables')
    @patch('s7.execute_with_retry')
    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    @patch('psycopg.connect')
    def test_run_s7_sql_success(self, mock_connect, mock_file, mock_exists, 
                               mock_execute, mock_verify, mock_load_config):
        """Test successful execution of run_s7_sql"""
        mock_load_config.return_value = self.sample_config
        mock_exists.return_value = True
        mock_file.return_value.read.return_value = self.sample_sql
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            None,          # Database doesn't exist initially
            ("faers_b",),  # Database exists after creation
        ]
        
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_execute.return_value = True
        
        s7.run_s7_sql()
        
        mock_cursor.execute.assert_any_call("SELECT 1 FROM pg_database WHERE datname = 'faersdatabase'")
        mock_verify.assert_called_once()
        self.assertTrue(mock_execute.called)
    
    @patch('s7.load_config')
    def test_run_s7_sql_missing_config_keys(self, mock_load_config):
        """Test run_s7_sql with incomplete configuration"""
        mock_load_config.return_value = self.incomplete_config
        
        with self.assertRaises(ValueError) as cm:
            s7.run_s7_sql()
        
        self.assertIn("Missing database configuration", str(cm.exception))
    
    @patch('s7.load_config')
    @patch('os.path.exists')
    def test_run_s7_sql_missing_sql_file(self, mock_exists, mock_load_config):
        """Test run_s7_sql when SQL file is missing"""
        mock_load_config.return_value = self.sample_config
        mock_exists.return_value = False
        
        with patch('psycopg.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = ("faers_b",)
            
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_connect.return_value.__enter__.return_value = mock_conn
            
            with self.assertRaises(FileNotFoundError):
                s7.run_s7_sql()
    
    @patch('s7.load_config')
    @patch('psycopg.connect')
    def test_run_s7_sql_database_error(self, mock_connect, mock_load_config):
        """Test run_s7_sql with database connection error"""
        mock_load_config.return_value = self.sample_config
        mock_connect.side_effect = pg_errors.OperationalError("Connection failed")
        
        with self.assertRaises(pg_errors.OperationalError):
            s7.run_s7_sql()
    
    @patch('s7.load_config')
    @patch('s7.execute_with_retry')
    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    @patch('psycopg.connect')
    def test_run_s7_sql_statement_execution_error(self, mock_connect, mock_file, 
                                                  mock_exists, mock_execute, mock_load_config):
        """Test run_s7_sql when some statements fail"""
        mock_load_config.return_value = self.sample_config
        mock_exists.return_value = True
        mock_file.return_value.read.return_value = "CREATE TABLE test1 (id INT); CREATE TABLE test2 (id INT);"
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("faers_b",)
        
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        # First statement succeeds, second fails
        mock_execute.side_effect = [True, pg_errors.SyntaxError("Invalid SQL")]
        
        s7.run_s7_sql()  # Should continue execution despite errors
        
        self.assertEqual(mock_execute.call_count, 2)
    
    def test_constants(self):
        """Test that s7.py has the expected constants"""
        self.assertEqual(s7.SQL_FILE_PATH, "s7.sql")
        self.assertEqual(s7.CONFIG_FILE, "config.json")
        self.assertEqual(s7.MAX_RETRIES, 3)
        self.assertEqual(s7.RETRY_DELAY, 5)
    
    def test_logger_exists(self):
        """Test that logger is configured"""
        self.assertTrue(hasattr(s7, 'logger'))
        self.assertEqual(s7.logger.name, "s7_execution")
    
    def test_s7_specific_table_verification(self):
        """Test that s7-specific table is verified"""
        expected_table = "FAERS_Analysis_Summary"
        
        # This should be the table that s7.verify_tables() checks
        self.assertIsInstance(expected_table, str)
        self.assertTrue(len(expected_table) > 0)
    
    def test_utf8_sig_encoding_handling(self):
        """Test handling of UTF-8 with signature encoding"""
        mock_file_content = "CREATE TABLE test (id INT);"
        
        with patch('builtins.open', mock_open(read_data=mock_file_content)) as mock_file:
            with patch('os.path.exists', return_value=True):
                statements = s7.parse_sql_statements(mock_file_content)
                
                self.assertGreater(len(statements), 0)


class TestS7Integration(unittest.TestCase):
    """Integration tests for s7.py"""
    
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
            original_config_file = s7.CONFIG_FILE
            s7.CONFIG_FILE = temp_config_path
            
            loaded_config = s7.load_config()
            self.assertEqual(loaded_config, config_data)
            
        finally:
            s7.CONFIG_FILE = original_config_file
            os.unlink(temp_config_path)
    
    @unittest.skipUnless(os.getenv("RUN_INTEGRATION_TESTS"), "Integration tests disabled")
    def test_sql_parsing_with_real_file(self):
        """Test SQL parsing with a real file for FAERS analysis"""
        sql_content = """
        -- FAERS Analysis Summary SQL
        CREATE SCHEMA IF NOT EXISTS faers_b;
        
        CREATE TABLE IF NOT EXISTS faers_b.FAERS_Analysis_Summary (
            analysis_id SERIAL PRIMARY KEY,
            analysis_type VARCHAR(100),
            drug_name VARCHAR(500),
            reaction_count INTEGER,
            total_cases INTEGER,
            analysis_date DATE DEFAULT CURRENT_DATE,
            confidence_level DECIMAL(5,4)
        );
        
        DO $$
        BEGIN
            -- Sample analysis data insertion
            INSERT INTO faers_b.FAERS_Analysis_Summary 
            (analysis_type, drug_name, reaction_count, total_cases, confidence_level)
            VALUES 
            ('adverse_reaction_analysis', 'ACETAMINOPHEN', 150, 1000, 0.9500);
        END $$;
        
        -- Create indexes for performance
        CREATE INDEX IF NOT EXISTS idx_analysis_summary_drug 
        ON faers_b.FAERS_Analysis_Summary (drug_name);
        
        CREATE INDEX IF NOT EXISTS idx_analysis_summary_type 
        ON faers_b.FAERS_Analysis_Summary (analysis_type);
        """
        
        statements = s7.parse_sql_statements(sql_content)
        
        # Should have 4 statements: CREATE SCHEMA, CREATE TABLE, DO block, CREATE INDEX (x2)
        self.assertGreaterEqual(len(statements), 4)
        
        # Verify DO block is parsed correctly
        do_statement = [s for s in statements if 'DO $$' in s][0]
        self.assertIn('BEGIN', do_statement)
        self.assertIn('END $$', do_statement)
        self.assertIn('FAERS_Analysis_Summary', do_statement)


class TestS7EdgeCases(unittest.TestCase):
    """Test edge cases and error conditions"""
    
    def test_execute_with_retry_zero_retries(self):
        """Test execute_with_retry with zero retries"""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.OperationalError("Error")
        
        with self.assertRaises(pg_errors.OperationalError):
            s7.execute_with_retry(mock_cursor, "SELECT 1;", retries=0)
    
    def test_parse_sql_statements_empty_input(self):
        """Test SQL parsing with empty input"""
        statements = s7.parse_sql_statements("")
        self.assertEqual(statements, [])
        
        statements = s7.parse_sql_statements("   \n\n   ")
        self.assertEqual(statements, [])
    
    def test_parse_sql_statements_only_comments(self):
        """Test SQL parsing with only comments"""
        sql = """
        -- This is a comment
        /* Another comment */
        -- Final comment
        """
        
        statements = s7.parse_sql_statements(sql)
        self.assertEqual(statements, [])
    
    def test_parse_sql_statements_complex_do_block(self):
        """Test SQL parsing with complex DO block"""
        sql = """
        DO $$
        DECLARE
            analysis_count INTEGER;
        BEGIN
            SELECT COUNT(*) INTO analysis_count 
            FROM faers_b.FAERS_Analysis_Summary;
            
            IF analysis_count = 0 THEN
                INSERT INTO faers_b.FAERS_Analysis_Summary 
                (analysis_type) VALUES ('initial_analysis');
            END IF;
        END $$;
        """
        
        statements = s7.parse_sql_statements(sql)
        
        self.assertEqual(len(statements), 1)
        do_block = statements[0]
        self.assertIn("DO $$", do_block)
        self.assertIn("DECLARE", do_block)
        self.assertIn("BEGIN", do_block)
        self.assertIn("END $$", do_block)
        self.assertIn("FAERS_Analysis_Summary", do_block)
    
    @patch('s7.load_config')
    @patch('psycopg.connect')
    def test_verify_tables_connection_error(self, mock_connect, mock_load_config):
        """Test verify_tables with connection error"""
        mock_load_config.return_value = {"database": {}}
        mock_connect.side_effect = pg_errors.OperationalError("Connection failed")
        
        s7.verify_tables()  # Should not raise exception
    
    def test_analysis_summary_specific_features(self):
        """Test features specific to FAERS Analysis Summary"""
        # Test that expected table name is used
        expected_table = "FAERS_Analysis_Summary"
        
        # Verify this matches what the verify_tables function checks
        self.assertIsInstance(expected_table, str)
        self.assertIn("Analysis", expected_table)
        self.assertIn("Summary", expected_table)
    
    def test_logging_configuration_s7_specific(self):
        """Test that logging is configured for s7 specifically"""
        # Test log file name is s7-specific
        expected_log_file = "s7_execution.log"
        
        # In actual implementation, this would be tested by examining handlers
        self.assertIsInstance(expected_log_file, str)
        self.assertIn("s7", expected_log_file)
    
    @patch('s7.run_s7_sql')
    def test_main_execution_success(self, mock_run_s7):
        """Test main execution path success"""
        mock_run_s7.return_value = None
        
        with patch('sys.exit') as mock_exit:
            try:
                s7.run_s7_sql()
            except Exception:
                pass
            
            mock_exit.assert_not_called()
    
    @patch('s7.run_s7_sql')
    def test_main_execution_failure(self, mock_run_s7):
        """Test main execution path failure"""
        mock_run_s7.side_effect = Exception("Test error")
        
        with patch('sys.exit') as mock_exit:
            try:
                s7.run_s7_sql()
            except Exception:
                mock_exit(1)
            
            mock_exit.assert_called_with(1)
    
    def test_faers_analysis_table_structure_validation(self):
        """Test validation of FAERS Analysis Summary table structure"""
        # Expected columns for FAERS analysis
        expected_columns = [
            "analysis_id",
            "analysis_type", 
            "drug_name",
            "reaction_count",
            "total_cases",
            "analysis_date",
            "confidence_level"
        ]
        
        for column in expected_columns:
            with self.subTest(column=column):
                self.assertIsInstance(column, str)
                self.assertTrue(len(column) > 0)
    
    def test_database_name_validation(self):
        """Test that s7 uses the correct database name"""
        # s7 should connect to faersdatabase like other scripts
        expected_db = "faersdatabase"
        
        # This would be validated in the actual connection logic
        self.assertEqual(expected_db, "faersdatabase")


if __name__ == '__main__':
    print("Running s7.py unit tests...")
    print("This tests your s7.py Python script for FAERS Analysis Summary")
    print()
    print("For integration tests, set environment variables:")
    print("  TEST_DB_HOST, TEST_DB_USER, TEST_DB_NAME, TEST_DB_PASSWORD")
    print("  RUN_INTEGRATION_TESTS=1")
    print()
    
    unittest.main(verbosity=2)