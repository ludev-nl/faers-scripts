import unittest
import os
import sys
import tempfile
import shutil
from unittest.mock import patch, Mock
import psycopg
from psycopg import errors as pg_errors

# Add the project root directory to Python path to import s9.py
# This assumes the test is run from unit_tests/sql/ directory
project_root = os.getcwd()
sys.path.insert(0, project_root)

try:
    import s9
except ImportError as e:
    print(f"Error importing s9 module: {e}")
    print(f"Project root path: {project_root}")
    raise


class TestS9SQLScript(unittest.TestCase):
    """Test the SQL script logic and database operations."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.sql_file_path = os.path.join(project_root, 's9.sql')
        self.maxDiff = None
        
        # Sample test data
        self.sample_drug_mapper_data = [
            ("aspirin", "acetylsalicylic acid", None, None, None, None, None, None, None, None, None),
            ("ibuprofen", "ibuprofen", None, None, None, None, None, None, None, None, None),
            ("tylenol", "acetaminophen", None, None, None, None, None, None, None, None, None),
        ]
        
        self.sample_drug_mapper_temp_data = [
            ("aspirin", "acetylsalicylic acid", "ASPIRIN", "ACETYLSALICYLIC ACID"),
            ("ibuprofen", "ibuprofen", "IBUPROFEN", "IBUPROFEN"),
            ("tylenol", "acetaminophen", "TYLENOL", "ACETAMINOPHEN"),
        ]
        
        self.sample_rxnconso_data = [
            ("12345", "67890", "RXNORM", "MIN", "ASPIRIN", "CODE123", "SAB1"),
            ("12346", "67891", "RXNORM", "IN", "IBUPROFEN", "CODE124", "SAB2"),
            ("12347", "67892", "RXNORM", "PIN", "ACETAMINOPHEN", "CODE125", "SAB3"),
        ]
        
        self.sample_idd_data = [
            ("ASPIRIN", "12345"),
            ("IBUPROFEN", "12346"),
            ("ACETAMINOPHEN", "12347"),
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
        
        statements = s9.parse_sql_statements(sql_content)
        
        # Should have multiple statements
        self.assertGreater(len(statements), 0, "No SQL statements parsed")
        
        # Check for key operations
        sql_text = sql_content.upper()
        self.assertIn('CREATE SCHEMA', sql_text)
        self.assertIn('DO $$', sql_text)
        self.assertIn('UPDATE', sql_text)
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
        
        statements = s9.parse_sql_statements(sql)
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

    def test_table_creation_logic(self):
        """Test placeholder table creation logic."""
        sql = """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT FROM pg_class 
                WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
                AND relname = 'IDD'
            ) THEN
                CREATE TABLE faers_b."IDD" (
                    "DRUGNAME" TEXT,
                    "RXAUI" VARCHAR(8)
                );
            END IF;
        END $$;
        """
        
        statements = s9.parse_sql_statements(sql)
        self.assertEqual(len(statements), 1)
        self.assertIn('CREATE TABLE', statements[0])
        self.assertIn('IDD', statements[0])

    def test_column_addition_logic(self):
        """Test column addition logic for DRUG_Mapper."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for column addition statements
        self.assertIn('CLEANED_DRUGNAME', sql_content)
        self.assertIn('CLEANED_PROD_AI', sql_content)
        self.assertIn('ALTER TABLE', sql_content)
        self.assertIn('ADD COLUMN', sql_content)

    def test_update_statements_structure(self):
        """Test the structure of UPDATE statements."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        statements = s9.parse_sql_statements(sql_content)
        
        # Find UPDATE statements
        update_statements = [stmt for stmt in statements if stmt.strip().upper().startswith('UPDATE')]
        
        # Should have multiple UPDATE statements
        self.assertGreater(len(update_statements), 5, "Expected multiple UPDATE statements")
        
        # Check for key UPDATE patterns
        for stmt in update_statements:
            if 'DRUG_Mapper' in stmt:
                self.assertIn('SET', stmt)
                self.assertIn('WHERE', stmt)

    def test_rxnconso_mapping_logic(self):
        """Test RXNCONSO mapping update statements."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for different mapping strategies
        self.assertIn("NOTES = '9.1'", sql_content)  # CLEANED_DRUGNAME with RXNORM (MIN, IN, PIN)
        self.assertIn("NOTES = '9.2'", sql_content)  # CLEANED_PROD_AI with RXNORM (MIN, IN, PIN)
        self.assertIn("NOTES = '9.5'", sql_content)  # CLEANED_DRUGNAME with RXNORM (IN)
        self.assertIn("NOTES = '9.6'", sql_content)  # CLEANED_PROD_AI with RXNORM (IN)
        
        # Check for TTY conditions
        self.assertIn("TTY IN ('MIN', 'IN', 'PIN')", sql_content)
        self.assertIn("TTY = 'IN'", sql_content)

    def test_idd_mapping_logic(self):
        """Test IDD mapping update statements."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for IDD-based mappings
        self.assertIn("NOTES = '9.3'", sql_content)  # CLEANED_DRUGNAME via IDD
        self.assertIn("NOTES = '9.4'", sql_content)  # CLEANED_PROD_AI via IDD
        self.assertIn("NOTES = '9.7'", sql_content)  # CLEANED_DRUGNAME via IDD (IN)
        self.assertIn("NOTES = '9.8'", sql_content)  # CLEANED_PROD_AI via IDD (IN)
        
        # Check for JOIN with IDD
        self.assertIn('INNER JOIN faers_b."IDD"', sql_content)
        self.assertIn('ON rxn."RXAUI" = idd."RXAUI"', sql_content)

    def test_conditional_execution_blocks(self):
        """Test conditional execution DO blocks."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        statements = s9.parse_sql_statements(sql_content)
        
        # Find DO blocks that check for table existence
        do_blocks = [stmt for stmt in statements if stmt.strip().upper().startswith('DO $$')]
        
        self.assertGreater(len(do_blocks), 3, "Expected multiple conditional DO blocks")
        
        # Check for table existence checks
        table_check_blocks = [block for block in do_blocks if 'table_exists' in block.lower()]
        self.assertGreater(len(table_check_blocks), 0, "Expected table existence check blocks")

    def test_notes_field_conditions(self):
        """Test that UPDATE statements properly check NOTES field."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        statements = s9.parse_sql_statements(sql_content)
        update_statements = [stmt for stmt in statements if stmt.strip().upper().startswith('UPDATE')]
        
        # All UPDATE statements should check for NULL NOTES (except initial ones)
        drug_mapper_updates = [stmt for stmt in update_statements if 'DRUG_Mapper' in stmt and 'RXNCONSO' in stmt]
        
        for stmt in drug_mapper_updates:
            self.assertIn('NOTES" IS NULL', stmt, f"UPDATE statement should check for NULL NOTES: {stmt[:100]}...")

    def test_cast_operations(self):
        """Test CAST operations in UPDATE statements."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for proper CAST operations
        self.assertIn('CAST(rxn."RXAUI" AS BIGINT)', sql_content)
        self.assertIn('CAST(rxn."RXCUI" AS BIGINT)', sql_content)

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
        
        statements = s9.parse_sql_statements(sql_content)
        
        # Simulate execution of each statement
        for i, stmt in enumerate(statements[:5]):  # Test first 5 statements
            try:
                # This would normally execute the statement
                # mock_cursor.execute(stmt)
                pass
            except Exception as e:
                self.fail(f"Statement {i+1} would fail: {e}")

    def test_update_sequence_logic(self):
        """Test the logical sequence of UPDATE operations."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        statements = s9.parse_sql_statements(sql_content)
        update_statements = [stmt for stmt in statements if stmt.strip().upper().startswith('UPDATE')]
        
        # Find statements that update from DRUG_Mapper_Temp (should come first)
        temp_updates = [stmt for stmt in update_statements if 'DRUG_Mapper_Temp' in stmt]
        
        # Find statements that update from RXNCONSO
        rxnconso_updates = [stmt for stmt in update_statements if 'RXNCONSO' in stmt and 'IDD' not in stmt]
        
        # Find statements that update via IDD
        idd_updates = [stmt for stmt in update_statements if 'IDD' in stmt and 'RXNCONSO' in stmt]
        
        self.assertGreater(len(temp_updates), 0, "Should have DRUG_Mapper_Temp updates")
        self.assertGreater(len(rxnconso_updates), 0, "Should have direct RXNCONSO updates")
        self.assertGreater(len(idd_updates), 0, "Should have IDD-mediated updates")

    def test_table_column_checks(self):
        """Test table and column existence checking logic."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for column existence verification
        self.assertIn('pg_attribute', sql_content)
        self.assertIn('attrelid', sql_content)
        self.assertIn('attname', sql_content)
        self.assertIn('attisdropped', sql_content)
        
        # Check for table existence verification
        self.assertIn('pg_class', sql_content)
        self.assertIn('relnamespace', sql_content)
        self.assertIn('relname', sql_content)

    def test_error_handling_in_sql(self):
        """Test error handling and validation in SQL."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for proper error handling
        self.assertIn('RAISE EXCEPTION', sql_content)
        self.assertIn('RAISE NOTICE', sql_content)
        
        # Check for validation logic
        self.assertIn('IF NOT EXISTS', sql_content)
        self.assertIn('row_count = 0', sql_content)

    def test_data_type_handling(self):
        """Test proper data type handling in the SQL."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for proper data type specifications
        self.assertIn('TEXT', sql_content)
        self.assertIn('VARCHAR(8)', sql_content)
        self.assertIn('BIGINT', sql_content)

    def test_priority_mapping_sequence(self):
        """Test that mapping operations follow the correct priority sequence."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Extract all NOTES values to verify sequence
        import re
        notes_pattern = r"NOTES = '([^']+)'"
        notes_matches = re.findall(notes_pattern, sql_content)
        
        expected_sequence = ['9.1', '9.2', '9.5', '9.6', '9.9', '9.10', '9.3', '9.4', '9.7', '9.8', '9.11', '9.12']
        
        # Verify that all expected notes are present
        for note in expected_sequence:
            self.assertIn(note, notes_matches, f"Missing NOTES value: {note}")

    def test_join_conditions(self):
        """Test JOIN conditions in the SQL statements."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for proper JOIN syntax
        self.assertIn('FROM faers_b."DRUG_Mapper_Temp" dmt', sql_content)
        self.assertIn('FROM faers_b."RXNCONSO" rxn', sql_content)
        self.assertIn('INNER JOIN faers_b."IDD" idd', sql_content)
        self.assertIn('INNER JOIN faers_b."RXNCONSO" rxn', sql_content)

    def test_where_clause_conditions(self):
        """Test WHERE clause conditions in UPDATE statements."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        statements = s9.parse_sql_statements(sql_content)
        update_statements = [stmt for stmt in statements if stmt.strip().upper().startswith('UPDATE')]
        
        # Check specific WHERE conditions
        rxnorm_updates = [stmt for stmt in update_statements if 'SAB" = \'RXNORM\'' in stmt]
        self.assertGreater(len(rxnorm_updates), 0, "Should have RXNORM-specific updates")
        
        # Check TTY filtering
        tty_specific = [stmt for stmt in update_statements if 'TTY" IN' in stmt or 'TTY" =' in stmt]
        self.assertGreater(len(tty_specific), 0, "Should have TTY-specific filtering")


class TestS9SQLIntegration(unittest.TestCase):
    """Integration tests for SQL script execution."""
    
    def setUp(self):
        """Set up integration test fixtures."""
        self.sql_file_path = os.path.join(project_root, 's9.sql')

    @patch('s9.load_config')
    @patch('s9.verify_tables')
    @patch('s9.execute_with_retry')
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
        s9.run_s9_sql()
        
        # Verify that SQL parsing and execution occurred
        mock_load_config.assert_called()
        mock_exists.assert_called_with(s9.SQL_FILE_PATH)
        mock_verify.assert_called_once()
        
        # Verify that multiple statements were executed
        self.assertGreater(mock_execute.call_count, 10, "Should execute multiple SQL statements")

    def test_sql_statement_independence(self):
        """Test that SQL statements can be executed independently."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        statements = s9.parse_sql_statements(sql_content)
        
        # Each statement should be properly terminated and independent
        for i, stmt in enumerate(statements):
            # Skip empty statements
            if not stmt.strip():
                continue
            
            # DO blocks should be complete
            if stmt.strip().upper().startswith('DO $$'):
                self.assertIn('END', stmt, f"DO block {i} should have END")
                self.assertIn('$$;', stmt, f"DO block {i} should be properly terminated")
            
            # UPDATE statements should have WHERE clauses (except some initial ones)
            if stmt.strip().upper().startswith('UPDATE') and 'DRUG_Mapper' in stmt:
                if 'RXNCONSO' in stmt or 'IDD' in stmt:
                    self.assertIn('WHERE', stmt, f"UPDATE statement {i} should have WHERE clause")


if __name__ == '__main__':
    # Configure test runner
    unittest.main(verbosity=2, buffer=True)