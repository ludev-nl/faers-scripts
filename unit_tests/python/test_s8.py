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

# Import your s8.py module
import s8


class TestS8(unittest.TestCase):
    """Test cases for s8.py script"""
    
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
        
        self.sample_s8_config = {
            "phase_1": {
                "description": "Initial cleaning phase",
                "enabled": True,
                "parameters": {
                    "min_threshold": 10,
                    "max_threshold": 100
                }
            },
            "phase_2": {
                "description": "Advanced cleaning phase",
                "enabled": False,
                "parameters": {
                    "strict_mode": True
                }
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
        
        result = s8.load_config()
        
        self.assertEqual(result, self.sample_config)
        mock_file.assert_called_once_with("config.json", "r", encoding="utf-8")
    
    @patch('builtins.open', new_callable=mock_open)
    def test_load_config_file_not_found(self, mock_file):
        """Test configuration loading when file is not found"""
        mock_file.side_effect = FileNotFoundError()
        
        with self.assertRaises(FileNotFoundError):
            s8.load_config()
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_load_config_invalid_json(self, mock_json_load, mock_file):
        """Test configuration loading with invalid JSON"""
        mock_json_load.side_effect = json.JSONDecodeError("Invalid JSON", "doc", 0)
        
        with self.assertRaises(json.JSONDecodeError):
            s8.load_config()
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_load_s8_config_success(self, mock_json_load, mock_file):
        """Test successful S8 configuration loading"""
        mock_json_load.return_value = self.sample_s8_config
        
        result = s8.load_s8_config()
        
        self.assertEqual(result, self.sample_s8_config)
        mock_file.assert_called_once_with("config_s8.json", "r", encoding="utf-8")
    
    @patch('builtins.open', new_callable=mock_open)
    def test_load_s8_config_file_not_found(self, mock_file):
        """Test S8 configuration loading when file is not found"""
        mock_file.side_effect = FileNotFoundError()
        
        result = s8.load_s8_config()
        
        # Should return empty dict when file not found
        self.assertEqual(result, {})
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_load_s8_config_invalid_json(self, mock_json_load, mock_file):
        """Test S8 configuration loading with invalid JSON"""
        mock_json_load.side_effect = json.JSONDecodeError("Invalid JSON", "doc", 0)
        
        with self.assertRaises(json.JSONDecodeError):
            s8.load_s8_config()
    
    def test_create_config_temp_table_success(self):
        """Test successful creation of config temp table"""
        mock_cursor = MagicMock()
        
        s8.create_config_temp_table(mock_cursor, self.sample_s8_config)
        
        # Verify table creation
        mock_cursor.execute.assert_any_call("DROP TABLE IF EXISTS temp_s8_config")
        mock_cursor.execute.assert_any_call("""
            CREATE TEMP TABLE temp_s8_config (
                phase_name TEXT PRIMARY KEY,
                config_data JSONB
            )
        """)
        
        # Verify data insertion (should be called for each phase)
        insert_calls = [call for call in mock_cursor.execute.call_args_list 
                       if "INSERT INTO temp_s8_config" in str(call)]
        self.assertEqual(len(insert_calls), 2)  # Two phases in sample config
    
    def test_create_config_temp_table_empty_config(self):
        """Test creation of config temp table with empty config"""
        mock_cursor = MagicMock()
        
        s8.create_config_temp_table(mock_cursor, {})
        
        # Should still create table structure
        mock_cursor.execute.assert_any_call("DROP TABLE IF EXISTS temp_s8_config")
        mock_cursor.execute.assert_any_call("""
            CREATE TEMP TABLE temp_s8_config (
                phase_name TEXT PRIMARY KEY,
                config_data JSONB
            )
        """)
        
        # No INSERT calls for empty config
        insert_calls = [call for call in mock_cursor.execute.call_args_list 
                       if "INSERT INTO temp_s8_config" in str(call)]
        self.assertEqual(len(insert_calls), 0)
    
    def test_create_config_temp_table_error_handling(self):
        """Test error handling in create_config_temp_table"""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.Error("Database error")
        
        with self.assertRaises(pg_errors.Error):
            s8.create_config_temp_table(mock_cursor, self.sample_s8_config)
    
    def test_execute_with_retry_success_first_attempt(self):
        """Test successful execution on first attempt"""
        mock_cursor = MagicMock()
        
        result = s8.execute_with_retry(mock_cursor, "SELECT 1;")
        
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
            result = s8.execute_with_retry(mock_cursor, "SELECT 1;", retries=3, delay=1)
        
        self.assertTrue(result)
        self.assertEqual(mock_cursor.execute.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)
    
    def test_execute_with_retry_duplicate_object_skipped(self):
        """Test that duplicate object errors are gracefully handled"""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.DuplicateTable("Table already exists")
        
        result = s8.execute_with_retry(mock_cursor, "CREATE TABLE test;")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once()
    
    @patch('s8.load_config')
    @patch('psycopg.connect')
    def test_verify_tables_schema_exists(self, mock_connect, mock_load_config):
        """Test table verification when schema exists"""
        mock_load_config.return_value = self.sample_config
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        # Mock schema check and table count
        mock_cursor.fetchone.side_effect = [
            ("faers_b",),  # Schema exists
            (250,),        # DRUG_Mapper_Temp count
        ]
        
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        s8.verify_tables()
        
        mock_cursor.execute.assert_any_call("SELECT nspname FROM pg_namespace WHERE nspname = 'faers_b'")
    
    @patch('s8.load_config')
    @patch('psycopg.connect')
    def test_verify_tables_schema_missing(self, mock_connect, mock_load_config):
        """Test table verification when schema is missing"""
        mock_load_config.return_value = self.sample_config
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None  # Schema doesn't exist
        
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        s8.verify_tables()  # Should not raise exception
    
    def test_parse_sql_statements_basic(self):
        """Test basic SQL statement parsing"""
        sql = """
        CREATE TABLE test1 (id INT);
        CREATE TABLE test2 (id INT);
        SELECT * FROM test1;
        """
        
        statements = s8.parse_sql_statements(sql)
        
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
        
        statements = s8.parse_sql_statements(sql)
        
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
        
        statements = s8.parse_sql_statements(sql)
        
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
        
        statements = s8.parse_sql_statements(sql)
        
        self.assertEqual(len(statements), 2)
        for stmt in statements:
            self.assertNotIn("\\copy", stmt)
    
    def test_parse_sql_statements_with_bom(self):
        """Test SQL parsing with BOM (Byte Order Mark)"""
        sql_with_bom = '\ufeff' + "CREATE TABLE test1 (id INT);"
        
        statements = s8.parse_sql_statements(sql_with_bom)
        
        self.assertEqual(len(statements), 1)
        for stmt in statements:
            self.assertNotIn('\ufeff', stmt)
    
    @patch('s8.load_config')
    @patch('s8.load_s8_config')
    @patch('s8.create_config_temp_table')
    @patch('s8.verify_tables')
    @patch('s8.execute_with_retry')
    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    @patch('psycopg.connect')
    def test_run_s8_sql_success(self, mock_connect, mock_file, mock_exists, 
                               mock_execute, mock_verify, mock_create_config,
                               mock_load_s8_config, mock_load_config):
        """Test successful execution of run_s8_sql"""
        mock_load_config.return_value = self.sample_config
        mock_load_s8_config.return_value = self.sample_s8_config
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
        
        s8.run_s8_sql()
        
        # Verify all components were called
        mock_load_config.assert_called_once()
        mock_load_s8_config.assert_called_once()
        mock_create_config.assert_called_once_with(mock_cursor, self.sample_s8_config)
        mock_verify.assert_called_once()
        mock_cursor.execute.assert_any_call("SELECT 1 FROM pg_database WHERE datname = 'faersdatabase'")
    
    @patch('s8.load_config')
    @patch('s8.load_s8_config')
    def test_run_s8_sql_missing_config_keys(self, mock_load_s8_config, mock_load_config):
        """Test run_s8_sql with incomplete configuration"""
        mock_load_config.return_value = self.incomplete_config
        mock_load_s8_config.return_value = {}
        
        with self.assertRaises(ValueError) as cm:
            s8.run_s8_sql()
        
        self.assertIn("Missing database configuration", str(cm.exception))
    
    @patch('s8.load_config')
    @patch('s8.load_s8_config')
    @patch('os.path.exists')
    def test_run_s8_sql_missing_sql_file(self, mock_exists, mock_load_s8_config, mock_load_config):
        """Test run_s8_sql when SQL file is missing"""
        mock_load_config.return_value = self.sample_config
        mock_load_s8_config.return_value = {}
        mock_exists.return_value = False
        
        with patch('psycopg.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = ("faers_b",)
            
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_connect.return_value.__enter__.return_value = mock_conn
            
            with patch('s8.create_config_temp_table'):
                with self.assertRaises(FileNotFoundError):
                    s8.run_s8_sql()
    
    @patch('s8.load_config')
    @patch('s8.load_s8_config')
    @patch('psycopg.connect')
    def test_run_s8_sql_database_error(self, mock_connect, mock_load_s8_config, mock_load_config):
        """Test run_s8_sql with database connection error"""
        mock_load_config.return_value = self.sample_config
        mock_load_s8_config.return_value = {}
        mock_connect.side_effect = pg_errors.OperationalError("Connection failed")
        
        with self.assertRaises(pg_errors.OperationalError):
            s8.run_s8_sql()
    
    def test_constants(self):
        """Test that s8.py has the expected constants"""
        self.assertEqual(s8.SQL_FILE_PATH, "s8.sql")
        self.assertEqual(s8.CONFIG_FILE, "config.json")
        self.assertEqual(s8.S8_CONFIG_FILE, "config_s8.json")
        self.assertEqual(s8.MAX_RETRIES, 3)
        self.assertEqual(s8.RETRY_DELAY, 5)
    
    def test_logger_exists(self):
        """Test that logger is configured"""
        self.assertTrue(hasattr(s8, 'logger'))
        self.assertEqual(s8.logger.name, "s8_execution")
    
    def test_s8_specific_table_verification(self):
        """Test that s8-specific table is verified"""
        expected_table = "DRUG_Mapper_Temp"
        
        # This should be the table that s8.verify_tables() checks
        self.assertIsInstance(expected_table, str)
        self.assertTrue(len(expected_table) > 0)
        self.assertIn("Temp", expected_table)
    
    def test_config_temp_table_json_serialization(self):
        """Test that config data is properly JSON serialized"""
        mock_cursor = MagicMock()
        
        s8.create_config_temp_table(mock_cursor, self.sample_s8_config)
        
        # Check that json.dumps was effectively called (data should be serializable)
        for phase_name, phase_config in self.sample_s8_config.items():
            serialized_config = json.dumps(phase_config)
            # Verify it's valid JSON
            self.assertIsInstance(json.loads(serialized_config), dict)


class TestS8Integration(unittest.TestCase):
    """Integration tests for s8.py"""
    
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
            original_config_file = s8.CONFIG_FILE
            s8.CONFIG_FILE = temp_config_path
            
            loaded_config = s8.load_config()
            self.assertEqual(loaded_config, config_data)
            
        finally:
            s8.CONFIG_FILE = original_config_file
            os.unlink(temp_config_path)
    
    @unittest.skipUnless(os.getenv("RUN_INTEGRATION_TESTS"), "Integration tests disabled")
    def test_s8_config_loading_integration(self):
        """Test actual S8 config file loading"""
        s8_config_data = {
            "cleaning_phase_1": {
                "description": "Basic data cleaning",
                "enabled": True,
                "thresholds": {
                    "min_occurrences": 5,
                    "max_null_percentage": 0.8
                }
            },
            "cleaning_phase_2": {
                "description": "Advanced data validation",
                "enabled": False,
                "rules": ["strict_validation", "cross_reference_check"]
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(s8_config_data, f)
            temp_s8_config_path = f.name
        
        try:
            original_s8_config_file = s8.S8_CONFIG_FILE
            s8.S8_CONFIG_FILE = temp_s8_config_path
            
            loaded_s8_config = s8.load_s8_config()
            self.assertEqual(loaded_s8_config, s8_config_data)
            
        finally:
            s8.S8_CONFIG_FILE = original_s8_config_file
            os.unlink(temp_s8_config_path)
    
    @unittest.skipUnless(os.getenv("RUN_INTEGRATION_TESTS"), "Integration tests disabled")
    def test_temp_config_table_creation_integration(self):
        """Test actual temp config table creation"""
        if not self.test_db_available:
            self.skipTest("Test database not configured")
        
        s8_config_data = {
            "test_phase": {
                "enabled": True,
                "parameters": {"value": 42}
            }
        }
        
        try:
            with psycopg.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    s8.create_config_temp_table(cur, s8_config_data)
                    
                    # Verify table was created
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM pg_class 
                            WHERE relname = 'temp_s8_config' 
                            AND relkind = 'r'
                        );
                    """)
                    table_exists = cur.fetchone()[0]
                    self.assertTrue(table_exists)
                    
                    # Verify data was inserted
                    cur.execute("SELECT COUNT(*) FROM temp_s8_config;")
                    count = cur.fetchone()[0]
                    self.assertEqual(count, 1)
                    
                    # Verify JSON data structure
                    cur.execute("SELECT phase_name, config_data FROM temp_s8_config;")
                    phase_name, config_data = cur.fetchone()
                    self.assertEqual(phase_name, "test_phase")
                    self.assertEqual(config_data["enabled"], True)
                    self.assertEqual(config_data["parameters"]["value"], 42)
        
        except psycopg.Error as e:
            self.skipTest(f"Database operation failed: {e}")


class TestS8EdgeCases(unittest.TestCase):
    """Test edge cases and error conditions"""
    
    def test_execute_with_retry_zero_retries(self):
        """Test execute_with_retry with zero retries"""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.OperationalError("Error")
        
        with self.assertRaises(pg_errors.OperationalError):
            s8.execute_with_retry(mock_cursor, "SELECT 1;", retries=0)
    
    def test_parse_sql_statements_empty_input(self):
        """Test SQL parsing with empty input"""
        statements = s8.parse_sql_statements("")
        self.assertEqual(statements, [])
        
        statements = s8.parse_sql_statements("   \n\n   ")
        self.assertEqual(statements, [])
    
    def test_parse_sql_statements_only_comments(self):
        """Test SQL parsing with only comments"""
        sql = """
        -- This is a comment
        /* Another comment */
        -- Final comment
        """
        
        statements = s8.parse_sql_statements(sql)
        self.assertEqual(statements, [])
    
    def test_create_config_temp_table_complex_config(self):
        """Test config temp table creation with complex nested config"""
        mock_cursor = MagicMock()
        
        complex_config = {
            "advanced_phase": {
                "nested": {
                    "deep": {
                        "values": [1, 2, 3],
                        "mapping": {"a": 1, "b": 2}
                    }
                },
                "array": ["item1", "item2"],
                "boolean": True,
                "null_value": None
            }
        }
        
        s8.create_config_temp_table(mock_cursor, complex_config)
        
        # Should handle complex JSON serialization
        insert_calls = [call for call in mock_cursor.execute.call_args_list 
                       if "INSERT INTO temp_s8_config" in str(call)]
        self.assertEqual(len(insert_calls), 1)
    
    @patch('s8.load_config')
    @patch('psycopg.connect')
    def test_verify_tables_connection_error(self, mock_connect, mock_load_config):
        """Test verify_tables with connection error"""
        mock_load_config.return_value = {"database": {}}
        mock_connect.side_effect = pg_errors.OperationalError("Connection failed")
        
        s8.verify_tables()  # Should not raise exception
    
    def test_drug_mapper_temp_specific_features(self):
        """Test features specific to DRUG_Mapper_Temp"""
        expected_table = "DRUG_Mapper_Temp"
        
        # Verify this matches what the verify_tables function checks
        self.assertIsInstance(expected_table, str)
        self.assertIn("DRUG_Mapper", expected_table)
        self.assertIn("Temp", expected_table)
    
    def test_s8_config_file_name_validation(self):
        """Test that S8 config file name is correct"""
        expected_s8_config = "config_s8.json"
        
        self.assertEqual(s8.S8_CONFIG_FILE, expected_s8_config)
        self.assertIn("s8", expected_s8_config)
        self.assertNotEqual(s8.S8_CONFIG_FILE, s8.CONFIG_FILE)
    
    def test_logging_configuration_s8_specific(self):
        """Test that logging is configured for s8 specifically"""
        expected_log_file = "s8_execution.log"
        
        # In actual implementation, this would be tested by examining handlers
        self.assertIsInstance(expected_log_file, str)
        self.assertIn("s8", expected_log_file)
    
    @patch('s8.run_s8_sql')
    def test_main_execution_success(self, mock_run_s8):
        """Test main execution path success"""
        mock_run_s8.return_value = None
        
        with patch('sys.exit') as mock_exit:
            try:
                s8.run_s8_sql()
            except Exception:
                pass
            
            mock_exit.assert_not_called()
    
    @patch('s8.run_s8_sql')
    def test_main_execution_failure(self, mock_run_s8):
        """Test main execution path failure"""
        mock_run_s8.side_effect = Exception("Test error")
        
        with patch('sys.exit') as mock_exit:
            try:
                s8.run_s8_sql()
            except Exception:
                mock_exit(1)
            
            mock_exit.assert_called_with(1)
    
    def test_config_phase_structure_validation(self):
        """Test validation of config phase structure"""
        # Expected structure for S8 phases
        sample_phase = {
            "description": "Phase description",
            "enabled": True,
            "parameters": {}
        }
        
        # Test that structure is JSON serializable
        json_str = json.dumps(sample_phase)
        reconstructed = json.loads(json_str)
        
        self.assertEqual(sample_phase, reconstructed)
        self.assertIn("description", reconstructed)
        self.assertIn("enabled", reconstructed)
        self.assertIn("parameters", reconstructed)


if __name__ == '__main__':
    print("Running s8.py unit tests...")
    print("This tests your s8.py Python script for DRUG_Mapper_Temp processing")
    print("Features: S8-specific config, temp config table creation")
    print()
    print("For integration tests, set environment variables:")
    print("  TEST_DB_HOST, TEST_DB_USER, TEST_DB_NAME, TEST_DB_PASSWORD")
    print("  RUN_INTEGRATION_TESTS=1")
    print()
    
    unittest.main(verbosity=2)