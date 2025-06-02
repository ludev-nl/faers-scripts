import unittest
from unittest.mock import Mock, patch, mock_open, MagicMock, call
import json
import tempfile
import os
import re
import psycopg
from psycopg import errors as pg_errors
import sys

# Import the module under test (assuming it's saved as db_executor.py)
# You'll need to adjust this import based on your actual module name

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from s2_5 import (
    load_config, execute_with_retry, verify_tables, 
    parse_sql_statements, run_s2_5_sql
)


class TestSqlScriptParsing(unittest.TestCase):
    """Test cases specific to the s2-5.sql script parsing."""
    
    def setUp(self):
        # Sample content from the actual s2-5.sql file
        self.sql_content = """-- s2-5.sql: Create and populate combined tables in faers_combined schema

-- Set session parameters
SET search_path TO faers_combined, faers_a, public;
SET work_mem = '256MB';
SET statement_timeout = '600s';
SET client_min_messages TO NOTICE;

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS faers_combined;

-- Create tables
CREATE TABLE IF NOT EXISTS faers_combined."DEMO_Combined" (
    "DEMO_ID" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    primaryid BIGINT,
    caseid BIGINT
);

-- Populate combined tables using get_completed_year_quarters
DO $$
DECLARE
    rec RECORD;
    table_prefixes TEXT[] := ARRAY['demo', 'drug', 'indi'];
BEGIN
    FOR rec IN SELECT year, quarter FROM faers_a.get_completed_year_quarters(4)
    LOOP
        RAISE NOTICE 'Processing year %, quarter %', rec.year, rec.quarter;
    END LOOP;
END $$;

-- Create indexes
DO $$  
BEGIN
    CREATE INDEX IF NOT EXISTS idx_demo_combined ON faers_combined."DEMO_Combined" (primaryid);
    RAISE NOTICE 'Indexes created successfully';
END $$;"""
    
    def test_parse_set_statements(self):
        """Test parsing of SET statements."""
        statements = parse_sql_statements(self.sql_content)
        
        set_statements = [stmt for stmt in statements if stmt.strip().startswith('SET')]
        self.assertEqual(len(set_statements), 4)
        
        # Check specific SET statements
        self.assertTrue(any('search_path' in stmt for stmt in set_statements))
        self.assertTrue(any('work_mem' in stmt for stmt in set_statements))
        self.assertTrue(any('statement_timeout' in stmt for stmt in set_statements))
        self.assertTrue(any('client_min_messages' in stmt for stmt in set_statements))
    
    def test_parse_schema_creation(self):
        """Test parsing of schema creation statement."""
        statements = parse_sql_statements(self.sql_content)
        
        schema_statements = [stmt for stmt in statements if 'CREATE SCHEMA' in stmt]
        self.assertEqual(len(schema_statements), 1)
        self.assertIn('faers_combined', schema_statements[0])
    
    def test_parse_table_creation(self):
        """Test parsing of table creation statements."""
        statements = parse_sql_statements(self.sql_content)
        
        table_statements = [stmt for stmt in statements if 'CREATE TABLE' in stmt]
        self.assertGreaterEqual(len(table_statements), 1)
        
        # Check that DEMO_Combined table is parsed
        demo_table = next((stmt for stmt in table_statements if 'DEMO_Combined' in stmt), None)
        self.assertIsNotNone(demo_table)
        self.assertIn('BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY', demo_table)
    
    def test_parse_do_blocks(self):
        """Test parsing of DO blocks (PL/pgSQL blocks)."""
        statements = parse_sql_statements(self.sql_content)
        
        do_blocks = [stmt for stmt in statements if stmt.strip().startswith('DO $$')]
        self.assertEqual(len(do_blocks), 2)
        
        # First DO block should contain the main data population logic
        main_do_block = do_blocks[0]
        self.assertIn('DECLARE', main_do_block)
        self.assertIn('get_completed_year_quarters', main_do_block)
        self.assertIn('FOR rec IN', main_do_block)
        
        # Second DO block should contain index creation
        index_do_block = do_blocks[1]
        self.assertIn('CREATE INDEX', index_do_block)
        self.assertIn('idx_demo_combined', index_do_block)
    
    def test_parse_complex_do_block_structure(self):
        """Test parsing of complex DO block with nested structures."""
        complex_do_block = """
        DO $$
        DECLARE
            rec RECORD;
            table_prefixes TEXT[] := ARRAY['demo', 'drug'];
            sql_text TEXT;
        BEGIN
            FOR rec IN SELECT year, quarter FROM faers_a.get_completed_year_quarters(4)
            LOOP
                IF rec.year > 2020 THEN
                    RAISE NOTICE 'Processing recent year %', rec.year;
                END IF;
            END LOOP;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Error occurred: %', SQLERRM;
        END $$;
        """
        
        statements = parse_sql_statements(complex_do_block)
        self.assertEqual(len(statements), 1)
        
        do_block = statements[0]
        self.assertIn('DECLARE', do_block)
        self.assertIn('BEGIN', do_block)
        self.assertIn('EXCEPTION', do_block)
        self.assertIn('END $$', do_block)


class TestSqlExecutionFlow(unittest.TestCase):
    """Test the execution flow of the s2-5.sql script."""
    
    def setUp(self):
        self.mock_cursor = Mock()
        self.mock_conn = Mock()
        self.mock_cursor.fetchone.return_value = (100,)  # Default row count
        
        # Full SQL content for testing
        with open('s2_5.sql', 'r') as f:
            self.full_sql_content = f.read()
    
    def test_session_parameters_execution(self):
        """Test that session parameters are set correctly."""
        statements = parse_sql_statements(self.full_sql_content)
        
        # Find SET statements
        set_statements = [stmt for stmt in statements if stmt.strip().startswith('SET')]
        
        # Execute each SET statement
        for stmt in set_statements:
            result = execute_with_retry(self.mock_cursor, stmt)
            self.assertTrue(result)
        
        # Verify that SET statements were executed
        expected_set_calls = len(set_statements)
        self.assertEqual(self.mock_cursor.execute.call_count, expected_set_calls)
    
    def test_schema_and_table_creation_order(self):
        """Test that schema is created before tables."""
        statements = parse_sql_statements(self.full_sql_content)
        
        schema_index = None
        first_table_index = None
        
        for i, stmt in enumerate(statements):
            if 'CREATE SCHEMA' in stmt and schema_index is None:
                schema_index = i
            elif 'CREATE TABLE' in stmt and first_table_index is None:
                first_table_index = i
        
        self.assertIsNotNone(schema_index)
        self.assertIsNotNone(first_table_index)
        self.assertLess(schema_index, first_table_index, 
                       "Schema should be created before tables")
    
    def test_all_required_tables_created(self):
        """Test that all required combined tables are created."""
        statements = parse_sql_statements(self.full_sql_content)
        
        expected_tables = [
            'DEMO_Combined', 'DRUG_Combined', 'INDI_Combined', 
            'THER_Combined', 'REAC_Combined', 'RPSR_Combined', 
            'OUTC_Combined', 'COMBINED_DELETED_CASES'
        ]
        
        table_statements = [stmt for stmt in statements if 'CREATE TABLE' in stmt]
        
        for table in expected_tables:
            table_found = any(table in stmt for stmt in table_statements)
            self.assertTrue(table_found, f"Table {table} not found in CREATE statements")
    
    def test_do_block_execution_with_errors(self):
        """Test DO block execution with simulated errors."""
        # Simulate the main DO block
        do_block = """
        DO $$
        DECLARE
            rec RECORD;
        BEGIN
            FOR rec IN SELECT 2023 as year, 1 as quarter
            LOOP
                RAISE NOTICE 'Processing year %, quarter %', rec.year, rec.quarter;
            END LOOP;
        END $$
        """
        
        # Test successful execution
        result = execute_with_retry(self.mock_cursor, do_block)
        self.assertTrue(result)
        
        # Test with operational error (should retry)
        self.mock_cursor.reset_mock()
        self.mock_cursor.execute.side_effect = [
            pg_errors.OperationalError("Temporary connection issue"),
            None  # Success on retry
        ]
        
        with patch('time.sleep'):
            result = execute_with_retry(self.mock_cursor, do_block, retries=2)
        
        self.assertTrue(result)
        self.assertEqual(self.mock_cursor.execute.call_count, 2)


class TestDatabaseTableVerification(unittest.TestCase):
    """Test table verification for the combined tables."""
    
    def setUp(self):
        self.mock_cursor = Mock()
        self.expected_tables = [
            "DEMO_Combined", "DRUG_Combined", "INDI_Combined", "THER_Combined",
            "REAC_Combined", "RPSR_Combined", "OUTC_Combined", "COMBINED_DELETED_CASES"
        ]
    
    def test_verify_all_combined_tables_exist(self):
        """Test verification of all combined tables."""
        # Mock row counts for each table
        row_counts = [1000, 5000, 2000, 1500, 3000, 500, 800, 100]
        self.mock_cursor.fetchone.side_effect = [(count,) for count in row_counts]
        
        verify_tables(self.mock_cursor, self.expected_tables)
        
        # Verify that COUNT queries were executed for each table
        self.assertEqual(self.mock_cursor.execute.call_count, len(self.expected_tables))
        
        # Check that proper table names were queried
        for i, table in enumerate(self.expected_tables):
            expected_query = f'SELECT COUNT(*) FROM faers_combined."{table}"'
            actual_call = self.mock_cursor.execute.call_args_list[i]
            self.assertEqual(actual_call[0][0], expected_query)
    
    def test_verify_tables_with_empty_tables(self):
        """Test verification when some tables are empty."""
        # Mix of populated and empty tables
        row_counts = [1000, 0, 2000, 0, 3000, 500, 0, 100]
        self.mock_cursor.fetchone.side_effect = [(count,) for count in row_counts]
        
        with patch('paste.logger') as mock_logger:
            verify_tables(self.mock_cursor, self.expected_tables)
            
            # Should log warnings for empty tables
            warning_calls = [call for call in mock_logger.warning.call_args_list 
                           if 'is empty' in str(call)]
            self.assertEqual(len(warning_calls), 3)  # Three empty tables
    
    def test_verify_tables_with_missing_table(self):
        """Test verification when a table doesn't exist."""
        # First table exists, second doesn't
        self.mock_cursor.fetchone.side_effect = [(1000,)]
        self.mock_cursor.execute.side_effect = [
            None,  # First table query succeeds
            pg_errors.UndefinedTable("Table does not exist")  # Second fails
        ]
        
        with self.assertRaises(pg_errors.UndefinedTable):
            verify_tables(self.mock_cursor, self.expected_tables[:2])


class TestIndexCreation(unittest.TestCase):
    """Test index creation from the SQL script."""
    
    def setUp(self):
        self.mock_cursor = Mock()
        
        # Index creation DO block
        self.index_do_block = """
        DO $$  
        BEGIN
            CREATE INDEX IF NOT EXISTS idx_demo_combined ON faers_combined."DEMO_Combined" (primaryid);
            CREATE INDEX IF NOT EXISTS idx_drug_combined ON faers_combined."DRUG_Combined" (primaryid);
            CREATE INDEX IF NOT EXISTS idx_indi_combined ON faers_combined."INDI_Combined" (primaryid);
            RAISE NOTICE 'Indexes created successfully';
        END $$
        """
    
    def test_index_creation_execution(self):
        """Test that index creation executes successfully."""
        result = execute_with_retry(self.mock_cursor, self.index_do_block)
        self.assertTrue(result)
        self.mock_cursor.execute.assert_called_once_with(self.index_do_block)
    
    def test_index_creation_with_existing_indexes(self):
        """Test index creation when indexes already exist."""
        # Simulate index already exists scenario
        self.mock_cursor.execute.side_effect = pg_errors.DuplicateObject("Index already exists")
        
        result = execute_with_retry(self.mock_cursor, self.index_do_block)
        self.assertTrue(result)  # Should handle duplicate gracefully


class TestErrorHandling(unittest.TestCase):
    """Test error handling scenarios specific to the SQL script."""
    
    def setUp(self):
        self.mock_cursor = Mock()
    
    def test_missing_function_error(self):
        """Test handling when get_completed_year_quarters function doesn't exist."""
        do_block_with_function = """
        DO $$
        DECLARE
            rec RECORD;
        BEGIN
            FOR rec IN SELECT year, quarter FROM faers_a.get_completed_year_quarters(4)
            LOOP
                RAISE NOTICE 'Processing %', rec.year;
            END LOOP;
        END $$
        """
        
        self.mock_cursor.execute.side_effect = pg_errors.UndefinedFunction(
            "Function get_completed_year_quarters does not exist"
        )
        
        with self.assertRaises(pg_errors.UndefinedFunction):
            execute_with_retry(self.mock_cursor, do_block_with_function)
    
    def test_missing_source_table_error(self):
        """Test handling when source tables don't exist."""
        insert_statement = """
        INSERT INTO faers_combined."DEMO_Combined" (primaryid, caseid)
        SELECT primaryid, caseid FROM faers_a.demo23q1
        """
        
        self.mock_cursor.execute.side_effect = pg_errors.UndefinedTable(
            "Table faers_a.demo23q1 does not exist"
        )
        
        with self.assertRaises(pg_errors.UndefinedTable):
            execute_with_retry(self.mock_cursor, insert_statement)
    
    def test_permission_error(self):
        """Test handling of permission errors."""
        create_schema = "CREATE SCHEMA IF NOT EXISTS faers_combined"
        
        self.mock_cursor.execute.side_effect = pg_errors.InsufficientPrivilege(
            "Permission denied to create schema"
        )
        
        with self.assertRaises(pg_errors.InsufficientPrivilege):
            execute_with_retry(self.mock_cursor, create_schema)


class TestS25SqlIntegration(unittest.TestCase):
    """Integration tests for the complete s2-5.sql execution."""
    
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
    
    @patch('paste.load_config')
    @patch('paste.os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    @patch('paste.psycopg.connect')
    def test_complete_s2_5_execution_success(self, mock_connect, mock_file, mock_exists, mock_load_config):
        """Test complete successful execution of s2-5.sql."""
        # Setup mocks
        mock_load_config.return_value = self.config_data
        mock_exists.return_value = True
        
        # Read actual SQL file content
        with open('s2_5.sql', 'r') as f:
            sql_content = f.read()
        mock_file.return_value.read.return_value = sql_content
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Mock successful table verification
        mock_cursor.fetchone.return_value = (1000,)  # Tables have data
        
        # Execute the function
        run_s2_5_sql()
        
        # Verify database connection was established
        mock_connect.assert_called_once_with(**self.config_data["database"])
        
        # Verify SQL execution occurred
        self.assertGreater(mock_cursor.execute.call_count, 0)
        
        # Verify transaction was committed
        mock_conn.commit.assert_called_once()
    
    @patch('paste.load_config')
    @patch('paste.os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    @patch('paste.psycopg.connect')
    def test_execution_with_statement_failures(self, mock_connect, mock_file, mock_exists, mock_load_config):
        """Test execution when some statements fail but execution continues."""
        # Setup mocks
        mock_load_config.return_value = self.config_data
        mock_exists.return_value = True
        
        # Simplified SQL content for testing
        sql_content = """
        CREATE SCHEMA IF NOT EXISTS faers_combined;
        CREATE TABLE faers_combined.test_table (id INT);
        INSERT INTO faers_combined.test_table VALUES (1);
        """
        mock_file.return_value.read.return_value = sql_content
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Simulate second statement failing
        mock_cursor.execute.side_effect = [
            None,  # First statement succeeds
            pg_errors.SyntaxError("Table creation failed"),  # Second fails
            None   # Third succeeds
        ]
        
        mock_cursor.fetchone.return_value = (0,)  # Empty table
        
        # Should not raise exception but continue execution
        run_s2_5_sql()
        
        # Verify rollback was called for failed statement
        self.assertTrue(mock_conn.rollback.called)


class TestSqlStatementValidation(unittest.TestCase):
    """Test validation of specific SQL statements from the script."""
    
    def test_table_creation_statements_syntax(self):
        """Test that table creation statements have valid syntax structure."""
        with open('s2_5.sql', 'r') as f:
            sql_content = f.read()
        
        statements = parse_sql_statements(sql_content)
        table_statements = [stmt for stmt in statements if 'CREATE TABLE' in stmt]
        
        for stmt in table_statements:
            # Check basic CREATE TABLE syntax
            self.assertRegex(stmt, r'CREATE TABLE.*\(.*\)', 
                           "CREATE TABLE statement should have column definitions")
            
            # Check for proper closing
            self.assertTrue(stmt.count('(') == stmt.count(')'), 
                          "Parentheses should be balanced in CREATE TABLE")
    
    def test_insert_statements_structure(self):
        """Test INSERT statements within DO blocks have proper structure."""
        with open('s2_5.sql', 'r') as f:
            sql_content = f.read()
        
        # Find DO blocks containing INSERT statements
        statements = parse_sql_statements(sql_content)
        do_blocks = [stmt for stmt in statements if stmt.strip().startswith('DO $$')]
        
        for do_block in do_blocks:
            if 'INSERT INTO' in do_block:
                # Should have format() function calls for dynamic SQL
                self.assertIn('format(', do_block, 
                            "Dynamic INSERT should use format() function")
                
                # Should have proper exception handling
                self.assertIn('EXCEPTION WHEN OTHERS', do_block,
                            "INSERT operations should have exception handling")


if __name__ == '__main__':
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    test_classes = [
        TestSqlScriptParsing,
        TestSqlExecutionFlow,
        TestDatabaseTableVerification,
        TestIndexCreation,
        TestErrorHandling,
        TestS25SqlIntegration,
        TestSqlStatementValidation
    ]
    
    for test_class in test_classes:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    
    # Run tests with verbose output
    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(suite)
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    
    # Exit with error code if tests failed
    sys.exit(0 if result.wasSuccessful() else 1)