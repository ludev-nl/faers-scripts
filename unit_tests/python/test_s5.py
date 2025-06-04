import unittest
import json
import sys
import os
from unittest.mock import patch, mock_open, MagicMock
from psycopg import errors as pg_errors

# Add the project root to the path to import s5
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
import s5

class TestS5Pipeline(unittest.TestCase):
    
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
        
        DO $$
        BEGIN
            INSERT INTO faers_b.test_table VALUES (1, 'test');
            RAISE NOTICE 'Data inserted successfully';
        END
        $$;
        
        \\copy faers_b.test_table FROM 'data.csv' WITH CSV HEADER;
        
        INSERT INTO faers_b.test_table VALUES (2, 'another test');
        """

    def test_load_config_success(self):
        """Test successful config loading."""
        mock_config = json.dumps(self.sample_config)
        
        with patch("builtins.open", mock_open(read_data=mock_config)):
            result = s5.load_config()
            
        self.assertEqual(result, self.sample_config)

    def test_load_config_missing_file(self):
        """Test config loading when file is missing."""
        with patch("builtins.open", side_effect=FileNotFoundError()):
            with self.assertRaises(FileNotFoundError):
                s5.load_config()

    def test_parse_sql_statements_with_copy_commands(self):
        """Test parsing SQL statements while filtering out COPY commands."""
        statements = s5.parse_sql_statements(self.sample_sql_script)
        
        # Should have 4 statements: CREATE SCHEMA, CREATE TABLE, DO block, INSERT
        # The \copy command should be filtered out
        self.assertEqual(len(statements), 4)
        
        # Check that comments are filtered out
        for stmt in statements:
            self.assertNotIn('--', stmt)
        
        # Check that \copy commands are filtered out
        for stmt in statements:
            self.assertNotIn('\\copy', stmt.lower())
        
        # Check that DO block is preserved as one statement
        do_block = [stmt for stmt in statements if stmt.strip().startswith('DO $$')]
        self.assertEqual(len(do_block), 1)
        self.assertIn('BEGIN', do_block[0])
        self.assertIn('END', do_block[0])

    def test_parse_sql_statements_empty_and_comments_only(self):
        """Test parsing SQL with only comments and empty lines."""
        comment_only_sql = """
        -- Just comments
        -- Another comment
        
        
        -- More comments
        """
        
        statements = s5.parse_sql_statements(comment_only_sql)
        self.assertEqual(len(statements), 0)

    @patch('s5.time.sleep')
    def test_execute_with_retry_operational_error_then_success(self, mock_sleep):
        """Test retry logic with operational error followed by success."""
        mock_cursor = MagicMock()
        # First call fails with OperationalError, second succeeds
        mock_cursor.execute.side_effect = [
            pg_errors.OperationalError("Connection lost"),
            None  # Success on second attempt
        ]
        
        result = s5.execute_with_retry(mock_cursor, "SELECT 1", retries=2, delay=1)
        
        self.assertTrue(result)
        self.assertEqual(mock_cursor.execute.call_count, 2)
        mock_sleep.assert_called_once_with(1)

    @patch('s5.time.sleep')
    def test_execute_with_retry_duplicate_table_no_retry(self, mock_sleep):
        """Test that duplicate table errors don't trigger retries."""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.DuplicateTable("Table already exists")
        
        result = s5.execute_with_retry(mock_cursor, "CREATE TABLE test", retries=3)
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once()
        mock_sleep.assert_not_called()

if __name__ == "__main__":
    # Run the tests with verbose output
    unittest.main(verbosity=2)