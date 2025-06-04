import unittest
import json
import sys
import os
from unittest.mock import patch, mock_open, MagicMock
from psycopg import errors as pg_errors

# Add the project root to the path to import s8
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
import s8

class TestS8Pipeline(unittest.TestCase):
    
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
        
        self.sample_s8_config = {
            "phase1": {
                "cleaning_rules": ["remove_duplicates", "standardize_names"],
                "thresholds": {"confidence": 0.8}
            },
            "phase2": {
                "cleaning_rules": ["validate_dates"],
                "thresholds": {"confidence": 0.9}
            }
        }
        
        self.sample_sql_script = """
        -- Create temp table for drug mapping
        CREATE TEMP TABLE drug_mapper_temp AS 
        SELECT * FROM faers_b.DRUG_Mapper WHERE 1=0;
        
        CREATE OR REPLACE FUNCTION faers_b.clean_drug_names()
        RETURNS void AS $$
        BEGIN
            UPDATE faers_b.DRUG_Mapper_Temp 
            SET drug_name = TRIM(UPPER(drug_name));
            RAISE NOTICE 'Drug names cleaned';
        END
        $$ LANGUAGE plpgsql;
        
        DO $$
        DECLARE
            config_data JSONB;
        BEGIN
            SELECT config_data INTO config_data 
            FROM temp_s8_config WHERE phase_name = 'phase1';
            RAISE NOTICE 'Using config: %', config_data;
        END
        $$;
        
        \\copy faers_b.DRUG_Mapper_Temp FROM 'cleaned_data.csv' WITH CSV HEADER;
        
        INSERT INTO faers_b.DRUG_Mapper_Temp (drug_name) VALUES ('Aspirin');
        """

    def test_load_config_success(self):
        """Test successful loading of main configuration."""
        mock_config = json.dumps(self.sample_config)
        
        with patch("builtins.open", mock_open(read_data=mock_config)):
            result = s8.load_config()
            
        self.assertEqual(result, self.sample_config)

    def test_load_s8_config_success(self):
        """Test successful loading of S8-specific configuration."""
        mock_s8_config = json.dumps(self.sample_s8_config)
        
        with patch("builtins.open", mock_open(read_data=mock_s8_config)):
            result = s8.load_s8_config()
            
        self.assertEqual(result, self.sample_s8_config)

    def test_load_s8_config_file_not_found(self):
        """Test S8 config loading when file doesn't exist (should return empty dict)."""
        with patch("builtins.open", side_effect=FileNotFoundError()):
            result = s8.load_s8_config()
            
        self.assertEqual(result, {})

    def test_load_s8_config_invalid_json(self):
        """Test S8 config loading with malformed JSON."""
        invalid_json = '{"phase1": {"rules": invalid}}'
        
        with patch("builtins.open", mock_open(read_data=invalid_json)):
            with self.assertRaises(json.JSONDecodeError):
                s8.load_s8_config()

    def test_create_config_temp_table_with_phases(self):
        """Test creation of temporary config table with phase data."""
        mock_cursor = MagicMock()
        
        s8.create_config_temp_table(mock_cursor, self.sample_s8_config)
        
        # Should execute DROP, CREATE, and 2 INSERT statements, plus SELECT for logging
        self.assertGreaterEqual(mock_cursor.execute.call_count, 4)
        
        # Check that temp table was created
        create_calls = [call for call in mock_cursor.execute.call_args_list 
                       if 'CREATE TEMP TABLE' in str(call)]
        self.assertEqual(len(create_calls), 1)
        
        # Check that inserts were made for each phase
        insert_calls = [call for call in mock_cursor.execute.call_args_list 
                       if 'INSERT INTO temp_s8_config' in str(call)]
        self.assertEqual(len(insert_calls), 2)

    def test_create_config_temp_table_empty_config(self):
        """Test creation of temp table with empty S8 config."""
        mock_cursor = MagicMock()
        
        s8.create_config_temp_table(mock_cursor, {})
        
        # Should still create table structure, but no inserts
        create_calls = [call for call in mock_cursor.execute.call_args_list 
                       if 'CREATE TEMP TABLE' in str(call)]
        self.assertEqual(len(create_calls), 1)
        
        insert_calls = [call for call in mock_cursor.execute.call_args_list 
                       if 'INSERT INTO temp_s8_config' in str(call)]
        self.assertEqual(len(insert_calls), 0)

    def test_parse_sql_statements_with_functions_and_config_blocks(self):
        """Test parsing SQL with functions, DO blocks, and config references."""
        statements = s8.parse_sql_statements(self.sample_sql_script)
        
        # Should have 5 statements: CREATE TEMP TABLE, CREATE FUNCTION, DO block, INSERT
        # The \copy command should be filtered out
        self.assertEqual(len(statements), 4)
        
        # Check that \copy commands are filtered out
        for stmt in statements:
            self.assertNotIn('\\copy', stmt.lower())
        
        # Check that function is preserved
        function_statements = [stmt for stmt in statements if 'CREATE OR REPLACE FUNCTION' in stmt]
        self.assertEqual(len(function_statements), 1)
        self.assertIn('LANGUAGE plpgsql', function_statements[0])
        
        # Check that DO block with config reference is preserved
        do_blocks = [stmt for stmt in statements if stmt.strip().startswith('DO $$')]
        self.assertEqual(len(do_blocks), 1)
        self.assertIn('temp_s8_config', do_blocks[0])

    def test_parse_sql_statements_complex_nested_blocks(self):
        """Test parsing complex nested blocks with multiple dollar quotes."""
        complex_sql = """
        CREATE OR REPLACE FUNCTION faers_b.process_with_config()
        RETURNS void AS $$
        DECLARE
            phase_config JSONB;
            dynamic_sql TEXT;
        BEGIN
            SELECT config_data INTO phase_config 
            FROM temp_s8_config WHERE phase_name = 'cleanup';
            
            dynamic_sql := 'UPDATE table SET field = $tag$value$tag$';
            EXECUTE dynamic_sql;
        END
        $$ LANGUAGE plpgsql;
        """
        
        statements = s8.parse_sql_statements(complex_sql)
        self.assertEqual(len(statements), 1)
        self.assertIn('$tag$value$tag$', statements[0])
        self.assertIn('temp_s8_config', statements[0])

    @patch('s8.time.sleep')
    def test_execute_with_retry_success_immediately(self, mock_sleep):
        """Test successful execution without needing retries."""
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = None
        
        result = s8.execute_with_retry(mock_cursor, "SELECT 1")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once_with("SELECT 1")
        mock_sleep.assert_not_called()

    @patch('s8.time.sleep')
    def test_execute_with_retry_database_error_then_success(self, mock_sleep):
        """Test retry mechanism recovering from database error."""
        mock_cursor = MagicMock()
        # First attempt fails, second succeeds
        mock_cursor.execute.side_effect = [
            pg_errors.DatabaseError("Temporary connection issue"),
            None  # Success on retry
        ]
        
        result = s8.execute_with_retry(mock_cursor, "SELECT 1", retries=2, delay=1)
        
        self.assertTrue(result)
        self.assertEqual(mock_cursor.execute.call_count, 2)
        mock_sleep.assert_called_once_with(1)

if __name__ == "__main__":
    # Run the tests with verbose output
    unittest.main(verbosity=2)