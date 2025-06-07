import unittest
import json
import sys
import os
from unittest.mock import patch, mock_open, MagicMock
from psycopg import errors as pg_errors

# Add the project root to the path to import s7
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
import s7

class TestS7Pipeline(unittest.TestCase):
    
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
        -- Create FAERS Analysis Summary table
        CREATE TABLE IF NOT EXISTS faers_b.FAERS_Analysis_Summary (
            analysis_id SERIAL PRIMARY KEY,
            drug_name VARCHAR(255),
            reaction_count INTEGER
        );
        
        DO $$
        BEGIN
            INSERT INTO faers_b.FAERS_Analysis_Summary 
            (drug_name, reaction_count) 
            VALUES ('Aspirin', 150);
            RAISE NOTICE 'Analysis data inserted';
        END
        $$;
        
        \\copy faers_b.FAERS_Analysis_Summary FROM 'analysis_data.csv' WITH CSV HEADER;
        
        INSERT INTO faers_b.FAERS_Analysis_Summary 
        (drug_name, reaction_count) 
        VALUES ('Ibuprofen', 75);
        """
        
        self.bom_sql_script = "\ufeff" + self.sample_sql_script
        
        self.incomplete_config = {
            "database": {
                "host": "localhost",
                "port": 5432
                # Missing required fields
            }
        }

    def test_load_config_success(self):
        """Test successful configuration loading."""
        mock_config = json.dumps(self.sample_config)
        
        with patch("builtins.open", mock_open(read_data=mock_config)):
            result = s7.load_config()
            
        self.assertEqual(result, self.sample_config)

    def test_load_config_file_not_found(self):
        """Test error handling when config file is missing."""
        with patch("builtins.open", side_effect=FileNotFoundError()):
            with self.assertRaises(FileNotFoundError):
                s7.load_config()

    def test_load_config_invalid_json(self):
        """Test error handling for malformed JSON config."""
        invalid_json = '{"database": {"host": "localhost", "port": invalid}}'
        
        with patch("builtins.open", mock_open(read_data=invalid_json)):
            with self.assertRaises(json.JSONDecodeError):
                s7.load_config()

    def test_parse_sql_statements_basic_functionality(self):
        """Test basic SQL statement parsing with DO blocks and copy commands."""
        statements = s7.parse_sql_statements(self.sample_sql_script)
        
        # Should have 3 statements: CREATE TABLE, DO block, INSERT
        # The \copy command should be filtered out
        self.assertEqual(len(statements), 3)
        
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

    def test_parse_sql_statements_with_bom_character(self):
        """Test parsing SQL statements that begin with BOM character."""
        statements = s7.parse_sql_statements(self.bom_sql_script)
        
        # Should handle BOM gracefully and produce same number of statements
        self.assertEqual(len(statements), 3)
        
        # First statement should not contain BOM
        first_stmt = statements[0]
        self.assertNotIn('\ufeff', first_stmt)
        
        # Should start with CREATE TABLE
        self.assertTrue(first_stmt.strip().startswith('CREATE TABLE'))

    def test_parse_sql_statements_empty_or_comments_only(self):
        """Test parsing empty SQL or SQL with only comments."""
        comment_only_sql = """
        -- This is just a comment
        -- Another comment line
        
        
        -- More comments
        """
        
        statements = s7.parse_sql_statements(comment_only_sql)
        self.assertEqual(len(statements), 0)

    def test_parse_sql_statements_complex_do_block(self):
        """Test parsing complex DO blocks with nested dollar quotes."""
        complex_do_sql = """
        DO $$
        DECLARE
            sql_text TEXT;
        BEGIN
            sql_text := 'CREATE TABLE test AS SELECT $tag$hello$tag$ as greeting';
            EXECUTE sql_text;
            RAISE NOTICE 'Executed: %', sql_text;
        END
        $$;
        """
        
        statements = s7.parse_sql_statements(complex_do_sql)
        self.assertEqual(len(statements), 1)
        self.assertIn('$tag$hello$tag$', statements[0])

    @patch('s7.time.sleep')
    def test_execute_with_retry_immediate_success(self, mock_sleep):
        """Test successful execution on first attempt without retries."""
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = None
        
        result = s7.execute_with_retry(mock_cursor, "SELECT 1")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once_with("SELECT 1")
        mock_sleep.assert_not_called()

    @patch('s7.time.sleep')
    def test_execute_with_retry_operational_error_recovery(self, mock_sleep):
        """Test retry mechanism with operational error followed by success."""
        mock_cursor = MagicMock()
        # First attempt fails, second succeeds
        mock_cursor.execute.side_effect = [
            pg_errors.OperationalError("Connection timeout"),
            None  # Success on retry
        ]
        
        result = s7.execute_with_retry(mock_cursor, "SELECT 1", retries=2, delay=1)
        
        self.assertTrue(result)
        self.assertEqual(mock_cursor.execute.call_count, 2)
        mock_sleep.assert_called_once_with(1)

    @patch('s7.time.sleep')
    def test_execute_with_retry_duplicate_object_skip(self, mock_sleep):
        """Test that duplicate object errors are skipped without retries."""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.DuplicateObject("Object already exists")
        
        result = s7.execute_with_retry(mock_cursor, "CREATE SCHEMA test", retries=3)
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once()
        mock_sleep.assert_not_called()

if __name__ == "__main__":
    # Run the tests with verbose output
    unittest.main(verbosity=2)