import unittest
import json
import sys
import os
from unittest.mock import patch, mock_open, MagicMock
from psycopg import errors as pg_errors

# Add the project root to the path to import s10
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
import s10

class TestS10Pipeline(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.sample_config = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "user": "testuser",
                "dbname": "faersdatabase",
                "password": "testpass"
            }
        }
        
        self.sample_sql_script = """
        -- Create drug mapper tables
        CREATE TABLE IF NOT EXISTS faers_b.drug_mapper (
            id SERIAL PRIMARY KEY,
            drug_name VARCHAR(255),
            mapping_status VARCHAR(50)
        );
        
        CREATE TABLE IF NOT EXISTS faers_b.drug_mapper_2 (
            id SERIAL PRIMARY KEY,
            drug_name VARCHAR(255),
            rxcui INTEGER
        );
        
        DO $$
        BEGIN
            INSERT INTO faers_b.drug_mapper (drug_name, mapping_status) 
            VALUES ('Aspirin', 'mapped');
            RAISE NOTICE 'Initial data loaded';
        END
        $$;
        
        CREATE TABLE IF NOT EXISTS faers_b.manual_remapper (
            id SERIAL PRIMARY KEY,
            original_name VARCHAR(255),
            mapped_name VARCHAR(255)
        );
        
        INSERT INTO faers_b.remapping_log (action, timestamp) 
        VALUES ('s10_initialization', CURRENT_TIMESTAMP);
        """
        
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
            result = s10.load_config()
            
        self.assertEqual(result, self.sample_config)

    def test_load_config_missing_file(self):
        """Test error handling when config file is missing."""
        with patch("builtins.open", side_effect=FileNotFoundError()):
            with self.assertRaises(FileNotFoundError):
                s10.load_config()

    def test_load_config_invalid_json(self):
        """Test error handling for malformed JSON config."""
        invalid_json = '{"database": {"host": localhost, "port": 5432}}'  # Missing quotes
        
        with patch("builtins.open", mock_open(read_data=invalid_json)):
            with self.assertRaises(json.JSONDecodeError):
                s10.load_config()

    def test_check_postgresql_version(self):
        """Test PostgreSQL version checking."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ["PostgreSQL 13.7 on x86_64-pc-linux-gnu"]
        
        version = s10.check_postgresql_version(mock_cursor)
        
        mock_cursor.execute.assert_called_once_with("SELECT version();")
        self.assertEqual(version, "PostgreSQL 13.7 on x86_64-pc-linux-gnu")

    def test_check_database_exists_true(self):
        """Test database existence check when database exists."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [1]  # Database exists
        
        result = s10.check_database_exists(mock_cursor, "faersdatabase")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once_with(
            "SELECT 1 FROM pg_database WHERE datname = %s", ("faersdatabase",)
        )

    def test_check_database_exists_false(self):
        """Test database existence check when database doesn't exist."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None  # Database doesn't exist
        
        result = s10.check_database_exists(mock_cursor, "nonexistent_db")
        
        self.assertFalse(result)
        mock_cursor.execute.assert_called_once_with(
            "SELECT 1 FROM pg_database WHERE datname = %s", ("nonexistent_db",)
        )

    def test_parse_sql_statements_with_do_blocks(self):
        """Test parsing SQL statements with DO blocks and table creation."""
        statements = s10.parse_sql_statements(self.sample_sql_script)
        
        # Should have 5 statements: 3 CREATE TABLE, 1 DO block, 1 INSERT
        self.assertEqual(len(statements), 5)
        
        # Check that comments are filtered out
        for stmt in statements:
            self.assertNotIn('--', stmt)
        
        # Check that DO block is preserved as one statement
        do_blocks = [stmt for stmt in statements if stmt.strip().startswith('DO $$')]
        self.assertEqual(len(do_blocks), 1)
        self.assertIn('BEGIN', do_blocks[0])
        self.assertIn('END', do_blocks[0])
        
        # Check that CREATE TABLE statements are preserved
        create_statements = [stmt for stmt in statements if 'CREATE TABLE' in stmt]
        self.assertEqual(len(create_statements), 3)

    def test_parse_sql_statements_empty_input(self):
        """Test parsing empty or comment-only SQL."""
        empty_sql = """
        -- Just comments about drug mapping
        -- Another comment line
        
        
        -- More comments
        """
        
        statements = s10.parse_sql_statements(empty_sql)
        self.assertEqual(len(statements), 0)

    def test_verify_tables_with_data(self):
        """Test table verification when tables exist with data."""
        mock_cursor = MagicMock()
        # Mock return values for COUNT queries
        mock_cursor.fetchone.side_effect = [[100], [50], [25], [10], [5]]
        
        tables = ["drug_mapper", "drug_mapper_2", "drug_mapper_3", "manual_remapper", "remapping_log"]
        
        with patch('s10.logger') as mock_logger:
            s10.verify_tables(mock_cursor, "faers_b", tables)
        
        # Should execute 5 COUNT queries
        self.assertEqual(mock_cursor.execute.call_count, 5)
        
        # Should log info messages for tables with data
        info_calls = [call for call in mock_logger.info.call_args_list]
        self.assertEqual(len(info_calls), 5)

    @patch('s10.time.sleep')
    def test_execute_with_retry_immediate_success(self, mock_sleep):
        """Test successful execution without needing retries."""
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = None
        
        result = s10.execute_with_retry(mock_cursor, "CREATE TABLE test (id INTEGER)")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once()
        mock_sleep.assert_not_called()

    @patch('s10.time.sleep')
    def test_execute_with_retry_duplicate_table_skip(self, mock_sleep):
        """Test that duplicate table errors are skipped without retries."""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.DuplicateTable("Table already exists")
        
        result = s10.execute_with_retry(mock_cursor, "CREATE TABLE existing_table (id INTEGER)")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once()
        mock_sleep.assert_not_called()

if __name__ == "__main__":
    # Run the tests with verbose output
    unittest.main(verbosity=2)