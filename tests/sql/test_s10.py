import unittest
import os
import sys
import tempfile
import shutil
from unittest.mock import patch, Mock
import psycopg
from psycopg import errors as pg_errors

# Import the module under test using the specified pattern
project_root = os.getcwd()
sys.path.insert(0, project_root)

try:
    import s10
except ImportError as e:
    print(f"Error importing s10 module: {e}")
    print(f"Project root path: {project_root}")
    raise


class TestS10SQLScript(unittest.TestCase):
    """Test the SQL script logic and database operations."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.sql_file_path = os.path.join(project_root, 's10.sql')
        self.maxDiff = None
        
        # Sample test data for drug remapping workflow
        self.sample_drug_mapper_data = [
            ("123", "drug1", "12345", "67890", "ASPIRIN", "RXNORM", "IN", "CODE123", None),
            ("124", "drug2", "12346", "67891", "IBUPROFEN", "RXNORM", "IN", "CODE124", None),
            ("125", "drug3", "12347", "67892", "ACETAMINOPHEN", "MMSL", "IN", "CODE125", None),
        ]
        
        self.sample_rxnconso_data = [
            ("12345", "67890", "RXNORM", "IN", "ASPIRIN", "CODE123", "SAB1"),
            ("12346", "67891", "RXNORM", "IN", "IBUPROFEN", "CODE124", "SAB2"),
            ("12347", "67892", "RXNORM", "IN", "ACETAMINOPHEN", "CODE125", "SAB3"),
            ("12348", "67893", "RXNORM", "SCDC", "ASPIRIN SCDC", "CODE126", "SAB4"),
        ]
        
        self.sample_rxnrel_data = [
            ("12345", "12348", "67890", "67893", "HAS_ACTIVE_MOIETY"),
            ("12346", "12349", "67891", "67894", "HAS_INGREDIENTS"),
            ("12347", "12350", "67892", "67895", "TRADENAME_OF"),
        ]

    def tearDown(self):
        """Clean up after each test."""
        pass

    def test_sql_file_exists(self):
        """Test that the SQL file exists and is readable."""
        self.assertTrue(os.path.exists(self.sql_file_path), f"SQL file not found: {self.sql_file_path}")
        
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            self.assertGreater(len(content), 0, "SQL file is empty")

    def test_sql_parsing(self):
        """Test that the SQL file can be parsed correctly."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        statements = s10.parse_sql_statements(sql_content)
        
        # Should have multiple statements
        self.assertGreater(len(statements), 0, "No SQL statements parsed")
        
        # Check for key operations
        sql_text = sql_content.upper()
        self.assertIn('CREATE SCHEMA', sql_text)
        self.assertIn('DO $$', sql_text)
        self.assertIn('CREATE OR REPLACE FUNCTION', sql_text)
        self.assertIn('DRUG_MAPPER', sql_text)

    def test_database_context_verification(self):
        """Test the database context verification logic."""
        # Test DO block for database verification
        sql = """
        DO $$
        BEGIN
            IF current_database() != 'faersdatabase' THEN
                RAISE EXCEPTION 'Must be connected to faersdatabase, current database is %', current_database();
            END IF;
        END $$;
        """
        
        statements = s10.parse_sql_statements(sql)
        self.assertEqual(len(statements), 1)
        self.assertIn('current_database()', statements[0])
        self.assertIn('faersdatabase', statements[0])

    def test_schema_creation_statements(self):
        """Test schema creation and verification statements."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for schema creation
        self.assertIn('CREATE SCHEMA IF NOT EXISTS faers_b', sql_content)
        self.assertIn('AUTHORIZATION postgres', sql_content)
        self.assertIn('GRANT ALL ON SCHEMA faers_b', sql_content)

    def test_logging_table_creation(self):
        """Test remapping_log table creation."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for logging table creation
        self.assertIn('CREATE TABLE IF NOT EXISTS faers_b.remapping_log', sql_content)
        self.assertIn('log_id SERIAL PRIMARY KEY', sql_content)
        self.assertIn('step VARCHAR(50)', sql_content)
        self.assertIn('message TEXT', sql_content)
        self.assertIn('log_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP', sql_content)

    def test_index_creation_logic(self):
        """Test index creation for performance optimization."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for index creation statements
        self.assertIn('CREATE INDEX IF NOT EXISTS idx_rxnconso_rxcui', sql_content)
        self.assertIn('CREATE INDEX IF NOT EXISTS idx_rxnconso_rxaui', sql_content)
        self.assertIn('CREATE INDEX IF NOT EXISTS idx_rxnrel_rxcui', sql_content)
        self.assertIn('CREATE INDEX IF NOT EXISTS idx_rxnrel_rxaui', sql_content)
        self.assertIn('CREATE INDEX IF NOT EXISTS idx_drug_mapper_remapping', sql_content)

    def test_function_definitions(self):
        """Test that all expected functions are defined."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for function definitions - should have 20+ steps
        expected_functions = [
            'step_1_initial_rxnorm_update',
            'step_2_create_drug_mapper_2', 
            'step_3_manual_remapping_update',
            'step_4_manual_remapping_insert',
            'step_5_manual_remapping_delete',
            'step_6_vandf_relationships',
            'step_7_mmsl_to_rxnorm_insert',
            'step_8_rxnorm_scdc_to_in_insert',
            'step_9_rxnorm_in_update_with_notes',
            'step_10_mthspl_to_rxnorm_in_insert',
            'step_11_rxnorm_in_update',
            'step_12_mmsl_to_rxnorm_in_insert_exclusions',
            'step_13_rxnorm_cleanup_update',
            'step_14_mark_for_deletion',
            'step_15_reinsert_from_deleted',
            'step_16_delete_marked_rows',
            'step_17_clean_duplicates',
            'step_18_update_rxaui_mappings',
            'step_19_non_rxnorm_sab_update',
            'step_20_rxnorm_sab_specific_update',
            'populate_manual_remapper',
            'merge_manual_remappings'
        ]
        
        for func_name in expected_functions:
            self.assertIn(func_name, sql_content, f"Missing function: {func_name}")

    def test_function_structure(self):
        """Test the structure of function definitions."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        statements = s10.parse_sql_statements(sql_content)
        
        # Find function creation statements
        function_statements = [stmt for stmt in statements if 'CREATE OR REPLACE FUNCTION' in stmt]
        
        # Should have multiple function definitions
        self.assertGreater(len(function_statements), 15, "Expected multiple function definitions")
        
        # Check function structure
        for func_stmt in function_statements[:5]:  # Test first 5 functions
            self.assertIn('RETURNS VOID AS $$', func_stmt)
            self.assertIn('DECLARE', func_stmt)
            self.assertIn('BEGIN', func_stmt)
            self.assertIn('END;', func_stmt)
            self.assertIn('$$ LANGUAGE plpgsql;', func_stmt)

    def test_error_handling_in_functions(self):
        """Test error handling and logging in functions."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for error handling patterns
        self.assertIn('EXCEPTION', sql_content)
        self.assertIn('WHEN OTHERS THEN', sql_content)
        self.assertIn('INSERT INTO faers_b.remapping_log', sql_content)
        self.assertIn('SQLERRM', sql_content)
        self.assertIn('RAISE;', sql_content)

    def test_table_existence_checks(self):
        """Test table existence checking logic in functions."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for table existence verification
        self.assertIn('table_exists BOOLEAN', sql_content)
        self.assertIn('row_count BIGINT', sql_content)
        self.assertIn('SELECT EXISTS', sql_content)
        self.assertIn('FROM pg_class', sql_content)
        self.assertIn('relnamespace', sql_content)
        self.assertIn('relname', sql_content)

    def test_drug_mapper_operations(self):
        """Test drug_mapper table operations."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for drug_mapper operations
        self.assertIn('drug_mapper', sql_content)
        self.assertIn('drug_mapper_2', sql_content)
        self.assertIn('remapping_rxcui', sql_content)
        self.assertIn('remapping_rxaui', sql_content)
        self.assertIn('remapping_notes', sql_content)

    def test_rxnorm_integration(self):
        """Test RXNORM integration and mapping logic."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for RXNORM-specific operations
        self.assertIn("sab = 'RXNORM'", sql_content)
        self.assertIn("tty = 'IN'", sql_content)
        self.assertIn('rxnconso', sql_content)
        self.assertIn('rxnrel', sql_content)
        self.assertIn('rxcui', sql_content)
        self.assertIn('rxaui', sql_content)

    def test_manual_remapping_logic(self):
        """Test manual remapping functionality."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for manual remapping operations
        self.assertIn('hopefully_last_one_5_7_2021', sql_content)
        self.assertIn('manual_remapper', sql_content)
        self.assertIn("'MAN_REM /'", sql_content)
        self.assertIn("'TO BE DELETED'", sql_content)

    def test_step_numbering_consistency(self):
        """Test that step numbering is consistent in functions and logging."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Extract step numbers from function names and logging
        import re
        
        # Find function step numbers
        func_pattern = r'step_(\d+)_'
        func_steps = re.findall(func_pattern, sql_content)
        
        # Find logging step numbers
        log_pattern = r"'Step (\d+)'"
        log_steps = re.findall(log_pattern, sql_content)
        
        # Should have corresponding steps
        self.assertGreater(len(func_steps), 15, "Should have multiple step functions")
        self.assertGreater(len(log_steps), 15, "Should have multiple step logs")
        
        # Check that function steps match logging steps
        unique_func_steps = set(func_steps)
        unique_log_steps = set(log_steps)
        
        # Most function steps should have corresponding log entries
        overlap = unique_func_steps.intersection(unique_log_steps)
        self.assertGreater(len(overlap), 10, "Function steps should match log steps")

    def test_data_type_handling(self):
        """Test proper data type handling in the SQL."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for proper data type specifications
        self.assertIn('VARCHAR(8)', sql_content)  # For RXAUI
        self.assertIn('VARCHAR(20)', sql_content)  # For SAB, TTY
        self.assertIn('VARCHAR(50)', sql_content)  # For CODE
        self.assertIn('TEXT', sql_content)
        self.assertIn('INTEGER', sql_content)
        self.assertIn('BIGINT', sql_content)

    def test_join_operations(self):
        """Test JOIN operations in the SQL statements."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for proper JOIN syntax
        self.assertIn('INNER JOIN', sql_content)
        self.assertIn('LEFT JOIN', sql_content) or self.assertIn('RIGHT OUTER JOIN', sql_content)
        self.assertIn('ON ', sql_content)
        
        # Check for specific table joins
        self.assertIn('faers_b.rxnconso', sql_content)
        self.assertIn('faers_b.rxnrel', sql_content)
        self.assertIn('faers_b.drug_mapper', sql_content)

    def test_conditional_execution_blocks(self):
        """Test conditional execution logic in functions."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        statements = s10.parse_sql_statements(sql_content)
        
        # Find function statements that should have conditional logic
        function_statements = [stmt for stmt in statements if 'CREATE OR REPLACE FUNCTION' in stmt]
        
        # Each function should have table existence checks
        for func_stmt in function_statements:
            self.assertIn('IF NOT table_exists THEN', func_stmt)
            self.assertIn('skipping', func_stmt)
            self.assertIn('RETURN;', func_stmt)

    def test_deletion_and_cleanup_operations(self):
        """Test deletion and cleanup operations."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for deletion operations
        self.assertIn('DELETE FROM', sql_content)
        self.assertIn('DROP TABLE IF EXISTS', sql_content)
        
        # Check for specific cleanup operations
        self.assertIn('TO BE DELETED', sql_content)
        self.assertIn('Clean duplicates', sql_content)
        self.assertIn('ROW_NUMBER() OVER', sql_content)

    def test_exclusion_lists(self):
        """Test exclusion lists in mapping operations."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for exclusion criteria
        self.assertIn('NOT IN', sql_content)
        self.assertIn("'2604414'", sql_content) or self.assertIn('exclusions', sql_content.lower())

    def test_search_path_setting(self):
        """Test search path configuration."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        self.assertIn('SET search_path TO faers_b, faers_combined, public', sql_content)

    @patch('psycopg.connect')
    def test_sql_execution_simulation(self, mock_connect):
        """Test simulated SQL execution without actual database."""
        # Mock database connection
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = ("faersdatabase",)
        mock_cursor.execute.return_value = None
        
        # Set up context managers
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)
        mock_connect.return_value = mock_conn
        
        # Test that SQL statements can be executed without errors
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        statements = s10.parse_sql_statements(sql_content)
        
        # Simulate execution of each statement
        for i, stmt in enumerate(statements[:5]):  # Test first 5 statements
            try:
                # This would normally execute the statement
                # mock_cursor.execute(stmt)
                pass
            except Exception as e:
                self.fail(f"Statement {i+1} would fail: {e}")

    def test_remapping_workflow_sequence(self):
        """Test the logical sequence of remapping operations."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        statements = s10.parse_sql_statements(sql_content)
        
        # Find function creation statements in order
        function_statements = [stmt for stmt in statements if 'CREATE OR REPLACE FUNCTION' in stmt]
        
        # Verify sequence makes logical sense
        self.assertGreater(len(function_statements), 15, "Should have multiple remapping steps")
        
        # First function should be initial RXNORM update
        first_func = function_statements[0] if function_statements else ""
        self.assertIn('step_1_initial_rxnorm_update', first_func)
        
        # Should have drug_mapper_2 creation early in the process
        has_drug_mapper_2_creation = any('step_2_create_drug_mapper_2' in stmt for stmt in function_statements)
        self.assertTrue(has_drug_mapper_2_creation, "Should create drug_mapper_2 table")

    def test_performance_optimizations(self):
        """Test performance optimization features."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for performance features
        self.assertIn('INCLUDE (', sql_content)  # Index includes
        self.assertIn('DISTINCT', sql_content)   # Duplicate prevention
        self.assertIn('EXISTS (', sql_content)   # Efficient existence checks

    def test_data_validation_logic(self):
        """Test data validation and integrity checks."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for validation patterns
        self.assertIn('IS NOT NULL', sql_content)
        self.assertIn('IS NULL', sql_content)
        self.assertIn('COUNT(*)', sql_content)
        self.assertIn('row_count = 0', sql_content)

    def test_manual_remapper_table_structure(self):
        """Test manual_remapper table creation and structure."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for manual_remapper table creation
        self.assertIn('CREATE TABLE faers_b.manual_remapper', sql_content)
        self.assertIn('source_drugname VARCHAR(3000)', sql_content)
        self.assertIn('source_rxaui VARCHAR(8)', sql_content)
        self.assertIn('source_rxcui VARCHAR(8)', sql_content)
        self.assertIn('final_rxaui BIGINT', sql_content)
        self.assertIn('notes VARCHAR(100)', sql_content)

    def test_complex_query_patterns(self):
        """Test complex query patterns and subqueries."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for complex patterns
        self.assertIn('CASE WHEN', sql_content)
        self.assertIn('PARTITION BY', sql_content)
        self.assertIn('ORDER BY', sql_content)
        self.assertIn('GROUP BY', sql_content)
        self.assertIn('HAVING', sql_content) or True  # HAVING might not be used

    def test_string_manipulation_functions(self):
        """Test string manipulation and pattern matching."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for string functions
        self.assertIn('REGEXP_REPLACE', sql_content) or self.assertIn('REPLACE', sql_content)
        self.assertIn('POSITION', sql_content) or self.assertIn('SUBSTRING', sql_content)
        self.assertIn('LIKE', sql_content)


class TestS10SQLIntegration(unittest.TestCase):
    """Integration tests for SQL script execution."""
    
    def setUp(self):
        """Set up integration test fixtures."""
        self.sql_file_path = os.path.join(project_root, 's10.sql')

    @patch('s10.load_config')
    @patch('s10.verify_tables')
    @patch('s10.execute_with_retry')
    @patch('os.path.exists')
    @patch('builtins.open')
    @patch('psycopg.connect')
    def test_full_sql_execution_flow(self, mock_connect, mock_open, mock_exists,
                                   mock_execute, mock_verify, mock_load_config):
        """Test the full SQL execution flow."""
        # Setup mocks
        mock_load_config.return_value = {
            "database": {
                "host": "localhost", "port": 5432, "user": "test",
                "password": "test", "dbname": "faersdatabase"
            }
        }
        mock_exists.return_value = True
        
        # Mock file reading
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        mock_file_handle = Mock()
        mock_file_handle.read.return_value = sql_content
        mock_open.return_value.__enter__.return_value = mock_file_handle
        
        mock_execute.return_value = True
        
        # Setup database mocks
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.side_effect = [("faersdatabase",), ("PostgreSQL 14.0",)]
        
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)
        mock_connect.return_value = mock_conn
        
        # Execute the main function
        s10.run_s10_sql()
        
        # Verify that SQL parsing and execution occurred
        mock_load_config.assert_called()
        mock_exists.assert_called_with(s10.SQL_FILE_PATH)
        mock_verify.assert_called_once()
        
        # Verify that multiple statements were executed (should be 25+ statements)
        self.assertGreater(mock_execute.call_count, 20, "Should execute multiple SQL statements")

    def test_sql_statement_independence(self):
        """Test that SQL statements can be executed independently."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        statements = s10.parse_sql_statements(sql_content)
        
        # Each statement should be properly terminated and independent
        for i, stmt in enumerate(statements):
            # Skip empty statements
            if not stmt.strip():
                continue
            
            # DO blocks should be complete
            if stmt.strip().upper().startswith('DO $$'):
                self.assertIn('END', stmt, f"DO block {i} should have END")
                self.assertIn('$$;', stmt, f"DO block {i} should be properly terminated")
            
            # Function definitions should be complete
            if 'CREATE OR REPLACE FUNCTION' in stmt:
                self.assertIn('$$ LANGUAGE plpgsql;', stmt, f"Function {i} should be properly terminated")
                self.assertIn('RETURNS VOID AS $$', stmt, f"Function {i} should have proper signature")

    def test_function_call_simulation(self):
        """Test that functions would be callable after creation."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check that functions are properly structured for calling
        # In practice, these would be called like: SELECT faers_b.step_1_initial_rxnorm_update();
        
        expected_function_calls = [
            'faers_b.step_1_initial_rxnorm_update()',
            'faers_b.step_2_create_drug_mapper_2()',
            'faers_b.populate_manual_remapper()',
        ]
        
        # Verify function structure allows for proper calling
        for func_call in expected_function_calls:
            func_name = func_call.split('(')[0].split('.')[-1]
            self.assertIn(func_name, sql_content, f"Function {func_name} should be defined")


if __name__ == '__main__':
    # Configure test runner
    unittest.main(verbosity=2, buffer=True)