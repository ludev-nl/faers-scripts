import unittest
import json
import sys
import os
from unittest.mock import patch, mock_open, MagicMock
from psycopg import errors as pg_errors

# Add the project root to the path to import s11
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
import s11

class TestS11Pipeline(unittest.TestCase):
    
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
        -- Create dataset tables for FAERS analysis
        CREATE TABLE IF NOT EXISTS faers_b.drugs_standardized (
            id SERIAL PRIMARY KEY,
            drug_name VARCHAR(255),
            rxcui INTEGER
        );
        
        CREATE TABLE IF NOT EXISTS faers_b.adverse_reactions (
            id SERIAL PRIMARY KEY,
            reaction_name VARCHAR(255),
            meddra_code VARCHAR(50)
        );
        
        $$
        CREATE OR REPLACE FUNCTION faers_b.calculate_proportions()
        RETURNS void AS $func$
        BEGIN
            INSERT INTO faers_b.proportionate_analysis 
            SELECT drug_id, reaction_id, count(*) as frequency
            FROM faers_b.drug_adverse_reactions_pairs
            GROUP BY drug_id, reaction_id;
        END
        $func$ LANGUAGE plpgsql;
        $$
        
        \\copy faers_b.drugs_standardized FROM 'standardized_drugs.csv' WITH CSV HEADER;
        
        INSERT INTO faers_b.contingency_table (drug_id, reaction_id, cell_count)
        SELECT d.id, r.id, COUNT(*)
        FROM faers_b.drugs_standardized d
        CROSS JOIN faers_b.adverse_reactions r
        GROUP BY d.id, r.id;
        
        CREATE INDEX idx_drug_reactions ON faers_b.drug_adverse_reactions_pairs(drug_id, reaction_id);
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
            result = s11.load_config()
            
        self.assertEqual(result, self.sample_config)

    def test_load_config_file_not_found(self):
        """Test error handling when config file is missing."""
        with patch("builtins.open", side_effect=FileNotFoundError()):
            with self.assertRaises(FileNotFoundError):
                s11.load_config()

    def test_load_config_invalid_json(self):
        """Test error handling for malformed JSON config."""
        invalid_json = '{"database": {"host": "localhost", "port": invalid}}'
        
        with patch("builtins.open", mock_open(read_data=invalid_json)):
            with self.assertRaises(json.JSONDecodeError):
                s11.load_config()

    def test_parse_sql_statements_with_dollar_quotes(self):
        """Test parsing SQL statements with dollar-quoted functions and COPY commands."""
        statements = s11.parse_sql_statements(self.sample_sql_script)
        
        # Should have 5 statements: 2 CREATE TABLE, 1 function, 1 INSERT, 1 CREATE INDEX
        # The \copy command should be filtered out
        self.assertEqual(len(statements), 5)
        
        # Check that comments are filtered out
        for stmt in statements:
            self.assertNotIn('--', stmt)
        
        # Check that \copy commands are filtered out
        for stmt in statements:
            self.assertNotIn('\\copy', stmt.lower())
        
        # Check that dollar-quoted function is preserved as one statement
        function_statements = [stmt for stmt in statements if '$func$' in stmt]
        self.assertEqual(len(function_statements), 1)
        self.assertIn('LANGUAGE plpgsql', function_statements[0])
        
        # Check that CREATE INDEX is preserved
        index_statements = [stmt for stmt in statements if 'CREATE INDEX' in stmt]
        self.assertEqual(len(index_statements), 1)

    def test_parse_sql_statements_with_bom_character(self):
        """Test parsing SQL statements that begin with BOM character."""
        statements = s11.parse_sql_statements(self.bom_sql_script)
        
        # Should handle BOM gracefully and produce same number of statements
        self.assertEqual(len(statements), 5)
        
        # First statement should not contain BOM
        first_stmt = statements[0]
        self.assertNotIn('\ufeff', first_stmt)

    def test_parse_sql_statements_empty_or_comments_only(self):
        """Test parsing empty SQL or SQL with only comments."""
        comment_only_sql = """
        -- This is just a comment about dataset creation
        -- Another comment line
        
        
        -- More comments about FAERS analysis
        """
        
        statements = s11.parse_sql_statements(comment_only_sql)
        self.assertEqual(len(statements), 0)

    def test_parse_sql_statements_incomplete_statement_warning(self):
        """Test parsing SQL with incomplete statements (no semicolon)."""
        incomplete_sql = """
        CREATE TABLE faers_b.test_table (
            id INTEGER,
            name VARCHAR(100)
        )
        -- Missing semicolon
        """
        
        with patch('s11.logger') as mock_logger:
            statements = s11.parse_sql_statements(incomplete_sql)
        
        # Should still capture the incomplete statement
        self.assertEqual(len(statements), 1)
        
        # Should log a warning about incomplete statement
        mock_logger.warning.assert_called_once()

    def test_parse_sql_statements_complex_dollar_quotes(self):
        """Test parsing complex dollar-quoted blocks with nested quotes."""
        complex_sql = """
        $$
        CREATE OR REPLACE FUNCTION faers_b.complex_analysis()
        RETURNS TABLE(drug_name TEXT, reaction_count INTEGER) AS $body$
        DECLARE
            sql_query TEXT;
        BEGIN
            sql_query := 'SELECT drug_name, COUNT(*) FROM faers_b.analysis_view';
            RETURN QUERY EXECUTE sql_query;
        END
        $body$ LANGUAGE plpgsql;
        $$
        """
        
        statements = s11.parse_sql_statements(complex_sql)
        self.assertEqual(len(statements), 1)
        self.assertIn('$body$', statements[0])
        self.assertIn('RETURN QUERY EXECUTE', statements[0])

    @patch('s11.time.sleep')
    def test_execute_with_retry_immediate_success(self, mock_sleep):
        """Test successful execution without needing retries."""
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = None
        
        result = s11.execute_with_retry(mock_cursor, "CREATE TABLE test (id INTEGER)")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once()
        mock_sleep.assert_not_called()

    @patch('s11.time.sleep')
    def test_execute_with_retry_duplicate_index_skip(self, mock_sleep):
        """Test that duplicate index errors are skipped without retries."""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pg_errors.DuplicateIndex("Index already exists")
        
        result = s11.execute_with_retry(mock_cursor, "CREATE INDEX test_idx ON test(id)")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once()
        mock_sleep.assert_not_called()

if __name__ == "__main__":
    # Run the tests with verbose output
    unittest.main(verbosity=2)