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

# Import your s6.py module
import s6


class TestS6(unittest.TestCase):
    """Test cases for s6.py script"""
    
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
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_load_config_success(self, mock_json_load, mock_file):
        """Test successful configuration loading"""
        mock_json_load.return_value = self.sample_config
        
        result = s6.load_config()
        
        self.assertEqual(result, self.sample_config)
        mock_file.assert_called_once_with("config.json", "r", encoding="utf-8")
    
    @patch('builtins.open', new_callable=mock_open)
    def test_load_config_file_not_found(self, mock_file):
        """Test configuration loading when file is not found"""
        mock_file.side_effect = FileNotFoundError()
        
        with self.assertRaises(FileNotFoundError):
            s6.load_config()
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_load_config_invalid_json(self, mock_json_load, mock_file):
        """Test configuration loading with invalid JSON"""
        mock_json_load.side_effect = json.JSONDecodeError("Invalid JSON", "doc", 0)
        
        with self.assertRaises(json.JSONDecodeError):
            s6.load_config()
    
    def test_execute_with_retry_success_first_attempt(self):
        """Test successful execution on first attempt"""
        mock_cursor = MagicMock()
        
        result = s6.execute_with_retry(mock_cursor, "SELECT 1;")
        
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
            result = s6.execute_with_retry(mock_cursor, "SELECT 1;", retries=3, delay=1)
        
        self.assertTrue(result)
        self.assertEqual(mock_cursor.execute.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)
    
    def test_execute_with_retry_max_retries_exceeded(self):
        """Test failure after max retries exceeded"""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.OperationalError("Persistent error")
        
        with patch('time.sleep'):
            with self.assertRaises(pg_errors.OperationalError):
                s6.execute_with_retry(mock_cursor, "SELECT 1;", retries=2, delay=0.1)
        
        self.assertEqual(mock_cursor.execute.call_count, 2)
    
    def test_execute_with_retry_duplicate_object_skipped(self):
        """Test that duplicate object errors are gracefully handled"""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.DuplicateTable("Table already exists")
        
        result = s6.execute_with_retry(mock_cursor, "CREATE TABLE test;")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once()
    
    @patch('s6.load_config')
    @patch('psycopg.connect')
    def test_verify_tables_schema_exists(self, mock_connect, mock_load_config):
        """Test table verification when schema exists"""
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
        """Test table verification when schema is missing"""
        mock_load_config.return_value = self.sample_config
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None  # Schema doesn't exist
        
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        s6.verify_tables()  # Should not raise exception
    
    def test_parse_sql_statements_basic(self):
        """Test basic SQL statement parsing"""
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
        """Test SQL parsing with DO blocks"""
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
        """Test SQL parsing with function definitions"""
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
        """Test that \\copy commands are skipped"""
        sql = """
        CREATE TABLE test1 (id INT);
        \\copy test1 FROM 'data.csv' WITH CSV HEADER;
        CREATE TABLE test2 (id INT);
        """
        
        statements = s6.parse_sql_statements(sql)
        
        self.assertEqual(len(statements), 2)
        for stmt in statements:
            self.assertNotIn("\\copy", stmt)
    
    def test_parse_sql_statements_with_bom(self):
        """Test SQL parsing with BOM (Byte Order Mark)"""
        sql_with_bom = '\ufeff' + "CREATE TABLE test1 (id INT);"
        
        statements = s6.parse_sql_statements(sql_with_bom)
        
        self.assertEqual(len(statements), 1)
        for stmt in statements:
            self.assertNotIn('\ufeff', stmt)
    
    @patch('s6.load_config')
    @patch('s6.verify_tables')
    @patch('s6.execute_with_retry')
    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    @patch('psycopg.connect')
    def test_run_s6_sql_success(self, mock_connect, mock_file, mock_exists, 
                               mock_execute, mock_verify, mock_load_config):
        """Test successful execution of run_s6_sql"""
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
        """Test run_s6_sql with incomplete configuration"""
        mock_load_config.return_value = self.incomplete_config
        
        with self.assertRaises(ValueError) as cm:
            s6.run_s6_sql()
        
        self.assertIn("Missing database configuration", str(cm.exception))
    
    @patch('s6.load_config')
    @patch('os.path.exists')
    def test_run_s6_sql_missing_sql_file(self, mock_exists, mock_load_config):
        """Test run_s6_sql when SQL file is missing"""
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
        """Test run_s6_sql with database connection error"""
        mock_load_config.return_value = self.sample_config
        mock_connect.side_effect = pg_errors.OperationalError("Connection failed")
        
        with self.assertRaises(pg_errors.OperationalError):
            s6.run_s6_sql()
    
    def test_constants(self):
        """Test that s6.py has the expected constants"""
        self.assertEqual(s6.SQL_FILE_PATH, "s6.sql")
        self.assertEqual(s6.CONFIG_FILE, "config.json")
        self.assertEqual(s6.MAX_RETRIES, 3)
        self.assertEqual(s6.RETRY_DELAY, 5)
    
    def test_logger_exists(self):
        """Test that logger is configured"""
        self.assertTrue(hasattr(s6, 'logger'))
        self.assertEqual(s6.logger.name, "s6_execution")


class TestS6Integration(unittest.TestCase):
    """Integration tests for s6.py"""
    
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
            original_config_file = s6.CONFIG_FILE
            s6.CONFIG_FILE = temp_config_path
            
            loaded_config = s6.load_config()
            self.assertEqual(loaded_config, config_data)
            
        finally:
            s6.CONFIG_FILE = original_config_file
            os.unlink(temp_config_path)
    
    @unittest.skipUnless(os.getenv("RUN_INTEGRATION_TESTS"), "Integration tests disabled")
    def test_sql_parsing_with_real_file(self):
        """Test SQL parsing with a real file"""
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


class TestS6EdgeCases(unittest.TestCase):
    """Test edge cases and error conditions"""
    
    def test_execute_with_retry_zero_retries(self):
        """Test execute_with_retry with zero retries"""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.OperationalError("Error")
        
        with self.assertRaises(pg_errors.OperationalError):
            s6.execute_with_retry(mock_cursor, "SELECT 1;", retries=0)
    
    def test_parse_sql_statements_empty_input(self):
        """Test SQL parsing with empty input"""
        statements = s6.parse_sql_statements("")
        self.assertEqual(statements, [])
        
        statements = s6.parse_sql_statements("   \n\n   ")
        self.assertEqual(statements, [])
    
    def test_parse_sql_statements_only_comments(self):
        """Test SQL parsing with only comments"""
        sql = """
        -- This is a comment
        /* Another comment */
        -- Final comment
        """
        
        statements = s6.parse_sql_statements(sql)
        self.assertEqual(statements, [])
    
    def test_parse_sql_statements_complex_function(self):
        """Test SQL parsing with complex function"""
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
    
    @patch('s6.load_config')
    @patch('psycopg.connect')
    def test_verify_tables_connection_error(self, mock_connect, mock_load_config):
        """Test verify_tables with connection error"""
        mock_load_config.return_value = {"database": {}}
        mock_connect.side_effect = pg_errors.OperationalError("Connection failed")
        
        # Should not raise exception, just log error
        s6.verify_tables()


if __name__ == '__main__':
    print("Running s6.py unit tests...")
    print("This tests your s6.py Python script")
    print()
    print("For integration tests, set environment variables:")
    print("  TEST_DB_HOST, TEST_DB_USER, TEST_DB_NAME, TEST_DB_PASSWORD")
    print("  RUN_INTEGRATION_TESTS=1")
    print()
    
    unittest.main(verbosity=2)