import unittest
import json
import sys
import os
from unittest.mock import patch, mock_open, MagicMock
from psycopg import errors as pg_errors

# Add the project root to the path to import s6
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
import s6

class TestS6Pipeline(unittest.TestCase):
    
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
        
        self.bom_sql_script = "\ufeff" + self.sample_sql_script

    def test_load_config_success(self):
        """Test successful config loading."""
        mock_config = json.dumps(self.sample_config)
        
        with patch("builtins.open", mock_open(read_data=mock_config)):
            result = s6.load_config()
            
        self.assertEqual(result, self.sample_config)

    def test_load_config_file_not_found(self):
        """Test config loading when file doesn't exist."""
        with patch("builtins.open", side_effect=FileNotFoundError()):
            with self.assertRaises(FileNotFoundError):
                s6.load_config()

    def test_load_config_invalid_json(self):
        """Test config loading with malformed JSON."""
        invalid_json = '{"database": invalid json}'
        
        with patch("builtins.open", mock_open(read_data=invalid_json)):
            with self.assertRaises(json.JSONDecodeError):
                s6.load_config()

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

if __name__ == "__main__":
    # Run the tests with verbose output
    unittest.main(verbosity=2)