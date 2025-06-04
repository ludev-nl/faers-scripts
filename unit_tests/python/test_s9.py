import unittest
import json
import sys
import os
from unittest.mock import patch, mock_open, MagicMock
from psycopg import errors as pg_errors

# Add the project root to the path to import s9
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
import s9

class TestS9Pipeline(unittest.TestCase):
    
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
        -- Update drug mapper with cleaned data
        UPDATE faers_b.DRUG_Mapper 
        SET drug_name = TRIM(UPPER(drug_name))
        WHERE drug_name IS NOT NULL;
        
        CREATE OR REPLACE FUNCTION faers_b.finalize_mapping()
        RETURNS void AS $$
        BEGIN
            UPDATE faers_b.DRUG_Mapper 
            SET mapping_status = 'completed'
            WHERE mapping_status = 'pending';
            RAISE NOTICE 'Mapping finalized';
        END
        $$ LANGUAGE plpgsql;
        
        DO $$
        BEGIN
            UPDATE faers_b.DRUG_Mapper 
            SET last_updated = CURRENT_TIMESTAMP;
            RAISE NOTICE 'Timestamps updated';
        END
        $$;
        
        \\copy faers_b.DRUG_Mapper FROM 'final_mappings.csv' WITH CSV HEADER;
        
        UPDATE faers_b.DRUG_Mapper 
        SET validation_status = 'verified' 
        WHERE confidence_score >= 0.8;
        
        INSERT INTO faers_b.mapping_log (action, timestamp) 
        VALUES ('s9_completion', CURRENT_TIMESTAMP);
        """
        
        self.bom_sql_script = "\ufeff" + self.sample_sql_script
        
        self.incomplete_config = {
            "database": {
                "host": "localhost"
                # Missing required fields
            }
        }

    def test_load_config_success(self):
        """Test successful configuration loading."""
        mock_config = json.dumps(self.sample_config)
        
        with patch("builtins.open", mock_open(read_data=mock_config)):
            result = s9.load_config()
            
        self.assertEqual(result, self.sample_config)

    def test_load_config_file_not_found(self):
        """Test error handling when config file is missing."""
        with patch("builtins.open", side_effect=FileNotFoundError()):
            with self.assertRaises(FileNotFoundError):
                s9.load_config()

    def test_load_config_invalid_json(self):
        """Test error handling for malformed JSON config."""
        invalid_json = '{"database": {"host": "localhost", "port": invalid}}'
        
        with patch("builtins.open", mock_open(read_data=invalid_json)):
            with self.assertRaises(json.JSONDecodeError):
                s9.load_config()

    def test_parse_sql_statements_with_updates_and_functions(self):
        """Test parsing SQL with multiple UPDATE statements, functions, and DO blocks."""
        statements = s9.parse_sql_statements(self.sample_sql_script)
        
        # Should have 5 statements: UPDATE, CREATE FUNCTION, DO block, UPDATE, INSERT
        # The \copy command should be filtered out
        self.assertEqual(len(statements), 5)
        
        # Check that comments are filtered out
        for stmt in statements:
            self.assertNotIn('--', stmt)
        
        # Check that \copy commands are filtered out
        for stmt in statements:
            self.assertNotIn('\\copy', stmt.lower())
        
        # Check that multiple UPDATE statements are preserved separately
        update_statements = [stmt for stmt in statements if stmt.strip().startswith('UPDATE')]
        self.assertEqual(len(update_statements), 2)
        
        # Check that function is preserved
        function_statements = [stmt for stmt in statements if 'CREATE OR REPLACE FUNCTION' in stmt]
        self.assertEqual(len(function_statements), 1)
        self.assertIn('LANGUAGE plpgsql', function_statements[0])
        
        # Check that DO block is preserved
        do_blocks = [stmt for stmt in statements if stmt.strip().startswith('DO $$')]
        self.assertEqual(len(do_blocks), 1)
        self.assertIn('UPDATE', do_blocks[0])

    def test_parse_sql_statements_with_bom_character(self):
        """Test parsing SQL statements that begin with BOM character."""
        statements = s9.parse_sql_statements(self.bom_sql_script)
        
        # Should handle BOM gracefully and produce same number of statements
        self.assertEqual(len(statements), 5)
        
        # First statement should not contain BOM
        first_stmt = statements[0]
        self.assertNotIn('\ufeff', first_stmt)
        
        # Should start with UPDATE
        self.assertTrue(first_stmt.strip().startswith('UPDATE'))

    def test_parse_sql_statements_empty_or_comments_only(self):
        """Test parsing empty SQL or SQL with only comments."""
        comment_only_sql = """
        -- This is just a comment about drug mapping
        -- Another comment line
        
        
        -- More comments about updates
        """
        
        statements = s9.parse_sql_statements(comment_only_sql)
        self.assertEqual(len(statements), 0)

    def test_parse_sql_statements_complex_update_with_subqueries(self):
        """Test parsing complex UPDATE statements with subqueries."""
        complex_update_sql = """
        UPDATE faers_b.DRUG_Mapper dm
        SET mapped_rxcui = (
            SELECT rxcui 
            FROM faers_b.rxn_mapping rm 
            WHERE UPPER(rm.drug_name) = UPPER(dm.drug_name)
            LIMIT 1
        )
        WHERE dm.mapped_rxcui IS NULL 
          AND EXISTS (
              SELECT 1 
              FROM faers_b.rxn_mapping rm2 
              WHERE UPPER(rm2.drug_name) = UPPER(dm.drug_name)
          );
        """
        
        statements = s9.parse_sql_statements(complex_update_sql)
        self.assertEqual(len(statements), 1)
        self.assertIn('EXISTS', statements[0])
        self.assertIn('LIMIT 1', statements[0])

    def test_parse_sql_statements_multiple_consecutive_updates(self):
        """Test parsing multiple consecutive UPDATE statements."""
        multiple_updates_sql = """
        UPDATE faers_b.DRUG_Mapper SET status = 'processing';
        UPDATE faers_b.DRUG_Mapper SET confidence = 1.0 WHERE exact_match = true;
        UPDATE faers_b.DRUG_Mapper SET confidence = 0.8 WHERE fuzzy_match = true;
        """
        
        statements = s9.parse_sql_statements(multiple_updates_sql)
        self.assertEqual(len(statements), 3)
        
        # All should be UPDATE statements
        for stmt in statements:
            self.assertTrue(stmt.strip().startswith('UPDATE'))

    @patch('s9.time.sleep')
    def test_execute_with_retry_immediate_success(self, mock_sleep):
        """Test successful execution without needing retries."""
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = None
        
        result = s9.execute_with_retry(mock_cursor, "UPDATE faers_b.DRUG_Mapper SET status = 'complete'")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once()
        mock_sleep.assert_not_called()

    @patch('s9.time.sleep')
    def test_execute_with_retry_operational_error_recovery(self, mock_sleep):
        """Test retry mechanism recovering from operational error."""
        mock_cursor = MagicMock()
        # First attempt fails, second succeeds
        mock_cursor.execute.side_effect = [
            pg_errors.OperationalError("Lock timeout"),
            None  # Success on retry
        ]
        
        result = s9.execute_with_retry(mock_cursor, "UPDATE faers_b.DRUG_Mapper SET status = 'complete'", retries=2, delay=1)
        
        self.assertTrue(result)
        self.assertEqual(mock_cursor.execute.call_count, 2)
        mock_sleep.assert_called_once_with(1)

if __name__ == "__main__":
    # Run the tests with verbose output
    unittest.main(verbosity=2)