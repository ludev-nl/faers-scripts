import unittest
import json
import sys
import os
from unittest.mock import patch, mock_open, MagicMock
from psycopg import errors as pg_errors

# Add the project root to the path to import s2_5
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
import s2_5

class TestS25Pipeline(unittest.TestCase):
    
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

    def test_load_config_success(self):
        """Test successful config loading."""
        mock_config = json.dumps(self.sample_config)
        
        with patch("builtins.open", mock_open(read_data=mock_config)):
            result = s2_5.load_config()
            
        self.assertEqual(result, self.sample_config)

    def test_load_config_invalid_json(self):
        """Test config loading with invalid JSON."""
        invalid_json = '{"database": invalid json}'
        
        with patch("builtins.open", mock_open(read_data=invalid_json)):
            with self.assertRaises(json.JSONDecodeError):
                s2_5.load_config()

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
    def test_execute_with_retry_duplicate_table_handling(self, mock_sleep):
        """Test handling of duplicate table errors (should not retry)."""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.DuplicateTable("Table already exists")
        
        result = s2_5.execute_with_retry(mock_cursor, "CREATE TABLE test")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once()
        mock_sleep.assert_not_called()

if __name__ == "__main__":
    # Run the tests with verbose output
    unittest.main(verbosity=2)