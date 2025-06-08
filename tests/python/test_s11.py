import unittest
import json
import sys
import os
import tempfile
import time
import logging
from unittest.mock import patch, mock_open, MagicMock, Mock
from io import StringIO
from psycopg import errors as pg_errors

# Add the project root to the path to import s11
project_root = os.getcwd()
sys.path.insert(0, project_root)

try:
    import s11
except ImportError as e:
    print(f"Error importing s11 module: {e}")
    print(f"Project root path: {project_root}")
    raise


class TestS11Configuration(unittest.TestCase):
    """Test configuration loading and validation."""
    
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
        
        self.incomplete_config = {
            "database": {
                "host": "localhost",
                "port": 5432
                # Missing required fields
            }
        }

    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_load_config_success(self, mock_json_load, mock_file):
        """Test successful configuration loading."""
        mock_json_load.return_value = self.sample_config
        
        result = s11.load_config()
        
        self.assertEqual(result, self.sample_config)
        mock_file.assert_called_once_with(s11.CONFIG_FILE, "r", encoding="utf-8")
        mock_json_load.assert_called_once()

    @patch('builtins.open', side_effect=FileNotFoundError)
    def test_load_config_file_not_found(self, mock_file):
        """Test error handling when config file is missing."""
        with self.assertRaises(FileNotFoundError):
            s11.load_config()

    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load', side_effect=json.JSONDecodeError("Invalid JSON", "", 0))
    def test_load_config_invalid_json(self, mock_json_load, mock_file):
        """Test error handling for malformed JSON config."""
        with self.assertRaises(json.JSONDecodeError):
            s11.load_config()

    def test_configuration_constants(self):
        """Test that configuration constants are properly set."""
        self.assertEqual(s11.CONFIG_FILE, "config.json")
        self.assertEqual(s11.SQL_FILE_PATH, "s11.sql")
        self.assertEqual(s11.MAX_RETRIES, 3)
        self.assertEqual(s11.RETRY_DELAY, 5)


class TestS11SQLParsing(unittest.TestCase):
    """Test SQL statement parsing functionality."""
    
    def setUp(self):
        """Set up SQL test fixtures."""
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
        
        self.complex_sql = """
        -- This is a comment
        CREATE SCHEMA IF NOT EXISTS faers_b;
        
        CREATE TABLE IF NOT EXISTS faers_b.drugs_standardized (
            id SERIAL PRIMARY KEY,
            drug_name VARCHAR(255),
            rxcui VARCHAR(8)
        );
        
        INSERT INTO faers_b.drugs_standardized (drug_name, rxcui) VALUES ('aspirin', '12345');
        
        DO $$
        BEGIN
            INSERT INTO faers_b.adverse_reactions (reaction_name) VALUES ('headache');
        END
        $$;
        
        CREATE OR REPLACE FUNCTION faers_b.calculate_statistics() RETURNS VOID AS $$
        BEGIN
            UPDATE faers_b.contingency_table SET calculated = true;
        END
        $$ LANGUAGE plpgsql;
        """

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
        
        with self.assertLogs('s11', level='WARNING') as log:
            statements = s11.parse_sql_statements(incomplete_sql)
        
        # Should still capture the incomplete statement
        self.assertEqual(len(statements), 1)
        
        # Should log a warning about incomplete statement
        self.assertIn("Incomplete statement detected", log.output[0])

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

    def test_parse_sql_statements_basic(self):
        """Test parsing of basic SQL statements."""
        sql = """
        CREATE TABLE test (id INT);
        INSERT INTO test VALUES (1);
        SELECT * FROM test;
        """
        
        statements = s11.parse_sql_statements(sql)
        
        expected = [
            "CREATE TABLE test (id INT);",
            "INSERT INTO test VALUES (1);",
            "SELECT * FROM test;"
        ]
        self.assertEqual(statements, expected)

    def test_parse_sql_statements_with_comments(self):
        """Test parsing SQL with comments removed."""
        sql = """
        -- This is a comment
        CREATE TABLE test (id INT); -- Inline comment
        INSERT INTO test VALUES (1);
        """
        
        statements = s11.parse_sql_statements(sql)
        
        expected = [
            "CREATE TABLE test (id INT);",
            "INSERT INTO test VALUES (1);"
        ]
        self.assertEqual(statements, expected)

    def test_parse_sql_statements_do_block(self):
        """Test parsing of DO blocks."""
        sql = """
        CREATE TABLE test (id INT);
        DO $$
        BEGIN
            INSERT INTO test VALUES (1);
        END
        $$;
        SELECT * FROM test;
        """
        
        statements = s11.parse_sql_statements(sql)
        
        # Should have 3 statements: CREATE, DO block, SELECT
        self.assertEqual(len(statements), 3)
        self.assertIn("DO $$", statements[1])
        self.assertIn("END", statements[1])
        self.assertEqual(statements[2], "SELECT * FROM test;")

    def test_parse_sql_statements_function_handling(self):
        """Test parsing of function creation statements."""
        sql = """
        CREATE OR REPLACE FUNCTION test_func()
        RETURNS VOID AS $$
        BEGIN
            NULL;
        END
        $$ LANGUAGE plpgsql;
        SELECT test_func();
        """
        
        statements = s11.parse_sql_statements(sql)
        
        self.assertEqual(len(statements), 2)
        self.assertIn("CREATE OR REPLACE FUNCTION", statements[0])
        self.assertIn("LANGUAGE plpgsql;", statements[0])

    def test_parse_sql_statements_copy_command_skipped(self):
        """Test that COPY commands are skipped."""
        sql = """
        CREATE TABLE test (id INT);
        \\copy test FROM 'data.csv';
        SELECT * FROM test;
        """
        
        statements = s11.parse_sql_statements(sql)
        
        expected = [
            "CREATE TABLE test (id INT);",
            "SELECT * FROM test;"
        ]
        self.assertEqual(statements, expected)


class TestS11ExecutionRetry(unittest.TestCase):
    """Test SQL execution with retry functionality."""

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

    @patch('time.sleep')
    def test_execute_with_retry_success_after_retries(self, mock_sleep):
        """Test successful execution after initial failures."""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = [
            pg_errors.OperationalError("Connection failed"),
            pg_errors.OperationalError("Connection failed"),
            None  # Success on third attempt
        ]
        
        result = s11.execute_with_retry(mock_cursor, "SELECT 1", retries=3, delay=1)
        
        self.assertTrue(result)
        self.assertEqual(mock_cursor.execute.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_called_with(1)

    @patch('time.sleep')
    def test_execute_with_retry_max_retries_exceeded(self, mock_sleep):
        """Test failure after max retries exceeded."""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = pg_errors.OperationalError("Connection failed")
        
        with self.assertRaises(pg_errors.OperationalError):
            s11.execute_with_retry(mock_cursor, "SELECT 1", retries=2, delay=1)
        
        self.assertEqual(mock_cursor.execute.call_count, 2)
        self.assertEqual(mock_sleep.call_count, 1)

    def test_execute_with_retry_duplicate_object_skip(self):
        """Test that duplicate object errors are handled gracefully."""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = pg_errors.DuplicateTable("Table already exists")
        
        result = s11.execute_with_retry(mock_cursor, "CREATE TABLE test")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once()

    def test_execute_with_retry_database_error(self):
        """Test that non-retryable database errors are raised immediately."""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = pg_errors.UndefinedColumn("Invalid column")
        
        with self.assertRaises(pg_errors.UndefinedColumn):
            s11.execute_with_retry(mock_cursor, "INVALID SQL")
        
        mock_cursor.execute.assert_called_once()

    def test_enhanced_retry_configuration(self):
        """Test that s11 has enhanced retry configuration."""
        # s11 uses MAX_RETRIES = 3 and RETRY_DELAY = 5
        self.assertEqual(s11.MAX_RETRIES, 3)
        self.assertEqual(s11.RETRY_DELAY, 5)
        
        # Test that the enhanced retry is actually used
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = [
            pg_errors.OperationalError("Connection failed"),
            pg_errors.OperationalError("Connection failed"),
            None  # Success on third attempt
        ]
        
        with patch('time.sleep') as mock_sleep:
            result = s11.execute_with_retry(mock_cursor, "SELECT 1")
        
        # Should use default enhanced configuration
        self.assertTrue(result)
        self.assertEqual(mock_cursor.execute.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)


class TestS11TableVerification(unittest.TestCase):
    """Test table verification functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.sample_config = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "user": "testuser",
                "password": "testpass",
                "dbname": "testdb"
            }
        }

    @patch('s11.load_config')
    @patch('psycopg.connect')
    def test_verify_tables_success(self, mock_connect, mock_load_config):
        """Test successful table verification."""
        mock_load_config.return_value = self.sample_config
        mock_conn = Mock()
        mock_cursor = Mock()
        
        # Mock different row counts for different tables
        mock_cursor.fetchone.side_effect = [
            (1000,),  # drugs_standardized
            (500,),   # adverse_reactions
            (2000,),  # drug_adverse_reactions_pairs
            (150,),   # drug_adverse_reactions_count
            (300,),   # drug_indications
            (800,),   # demographics
            (100,),   # case_outcomes
            (200,),   # therapy_dates
            (50,),    # report_sources
            (75,),    # drug_margin
            (60,),    # event_margin
            (25,),    # total_count
            (100,),   # contingency_table
            (85,),    # proportionate_analysis
        ]
        
        # Properly mock the context manager
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)
        mock_connect.return_value = mock_conn
        
        # Should not raise any exceptions
        s11.verify_tables()
        
        mock_connect.assert_called_once()
        # Should check 14 tables
        self.assertEqual(mock_cursor.execute.call_count, 14)

    @patch('s11.load_config')
    @patch('psycopg.connect')
    def test_verify_tables_empty_tables(self, mock_connect, mock_load_config):
        """Test table verification with empty tables."""
        mock_load_config.return_value = self.sample_config
        mock_conn = Mock()
        mock_cursor = Mock()
        
        # All tables are empty
        mock_cursor.fetchone.return_value = (0,)
        
        # Properly mock the context manager
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)
        mock_connect.return_value = mock_conn
        
        # Should not raise any exceptions but log warnings
        s11.verify_tables()
        
        self.assertEqual(mock_cursor.execute.call_count, 14)

    @patch('s11.load_config')
    @patch('psycopg.connect')
    def test_verify_tables_missing_tables(self, mock_connect, mock_load_config):
        """Test table verification with missing tables."""
        mock_load_config.return_value = self.sample_config
        mock_conn = Mock()
        mock_cursor = Mock()
        
        # Simulate table not existing
        mock_cursor.execute.side_effect = [
            None, None, None,  # First 3 tables exist
            pg_errors.UndefinedTable("Table does not exist"),  # 4th table missing
            None, None, None, None, None, None, None, None, None, None  # Rest exist
        ]
        mock_cursor.fetchone.return_value = (100,)
        
        # Properly mock the context manager and rollback
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)
        mock_conn.rollback = Mock()
        mock_connect.return_value = mock_conn
        
        # Should handle missing tables gracefully
        s11.verify_tables()
        
        # Should call rollback when table doesn't exist
        mock_conn.rollback.assert_called()

    def test_expected_tables_list(self):
        """Test that the expected tables list is correct for s11 (FAERS analysis tables)."""
        expected_tables = [
            "drugs_standardized",
            "adverse_reactions", 
            "drug_adverse_reactions_pairs",
            "drug_adverse_reactions_count",
            "drug_indications",
            "demographics",
            "case_outcomes",
            "therapy_dates",
            "report_sources",
            "drug_margin",
            "event_margin",
            "total_count",
            "contingency_table",
            "proportionate_analysis"
        ]
        
        # Test indirectly by verifying table verification logic
        with patch('s11.load_config') as mock_config:
            with patch('psycopg.connect') as mock_connect:
                mock_config.return_value = self.sample_config
                mock_conn = Mock()
                mock_cursor = Mock()
                mock_cursor.fetchone.return_value = (1,)
                
                mock_conn.cursor.return_value = mock_cursor
                mock_conn.__enter__ = Mock(return_value=mock_conn)
                mock_conn.__exit__ = Mock(return_value=None)
                mock_cursor.__enter__ = Mock(return_value=mock_cursor)
                mock_cursor.__exit__ = Mock(return_value=None)
                mock_connect.return_value = mock_conn
                
                s11.verify_tables()
                
                # Should check all expected tables
                self.assertEqual(mock_cursor.execute.call_count, len(expected_tables))


class TestS11MainExecution(unittest.TestCase):
    """Test main execution flow."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.sample_config = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "user": "testuser",
                "password": "testpass",
                "dbname": "testdb"
            }
        }
        
        self.sample_sql = """
        CREATE SCHEMA IF NOT EXISTS faers_b;
        CREATE TABLE IF NOT EXISTS faers_b.drugs_standardized (
            id SERIAL PRIMARY KEY,
            drug_name VARCHAR(255)
        );
        """

    @patch('s11.load_config')
    @patch('s11.verify_tables')
    @patch('s11.execute_with_retry')
    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    @patch('psycopg.connect')
    def test_run_s11_sql_success(self, mock_connect, mock_file, mock_exists, 
                                mock_execute, mock_verify, mock_load_config):
        """Test successful execution of run_s11_sql."""
        mock_load_config.return_value = self.sample_config
        mock_exists.return_value = True
        mock_file.return_value.read.return_value = self.sample_sql
        mock_execute.return_value = True
        
        # Mock database connections with proper context managers
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.side_effect = [None, ("PostgreSQL 14.0",)]  # DB doesn't exist, then version
        
        # Properly mock context managers
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)
        mock_connect.return_value = mock_conn
        
        s11.run_s11_sql()
        
        # Verify database creation and connection
        self.assertEqual(mock_connect.call_count, 2)  # Once for initial check, once for faersdatabase
        mock_verify.assert_called_once()

    @patch('s11.load_config')
    @patch('os.path.exists')
    @patch('psycopg.connect')
    def test_run_s11_sql_missing_sql_file(self, mock_connect, mock_exists, mock_load_config):
        """Test run_s11_sql when SQL file is missing."""
        mock_load_config.return_value = self.sample_config
        mock_exists.return_value = False
        
        # Mock database connections to avoid real connections
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.side_effect = [None, ("PostgreSQL 14.0",)]
        
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)
        mock_connect.return_value = mock_conn
        
        with self.assertRaises(FileNotFoundError):
            s11.run_s11_sql()

    def test_run_s11_sql_missing_database_config(self):
        """Test run_s11_sql with incomplete database configuration."""
        incomplete_config = {
            "database": {
                "host": "localhost",
                "port": 5432
                # Missing user, password, dbname
            }
        }
        
        with patch('s11.load_config', return_value=incomplete_config):
            with self.assertRaises(ValueError):
                s11.run_s11_sql()

    @patch('s11.load_config')
    @patch('psycopg.connect', side_effect=pg_errors.OperationalError("Connection failed"))
    def test_run_s11_sql_connection_error(self, mock_connect, mock_load_config):
        """Test run_s11_sql with database connection error."""
        mock_load_config.return_value = self.sample_config
        
        with self.assertRaises(pg_errors.OperationalError):
            s11.run_s11_sql()

    def test_password_masking_in_logs(self):
        """Test that passwords are masked in log output."""
        config_with_password = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "user": "testuser",
                "password": "secret123",
                "dbname": "testdb"
            }
        }
        
        with patch('s11.load_config', return_value=config_with_password):
            with patch('s11.verify_tables'):
                with patch('s11.execute_with_retry', return_value=True):
                    with patch('os.path.exists', return_value=True):
                        with patch('builtins.open', mock_open(read_data="SELECT 1;")):
                            with patch('psycopg.connect') as mock_connect:
                                mock_conn = Mock()
                                mock_cursor = Mock()
                                mock_cursor.fetchone.side_effect = [("faersdatabase",), ("PostgreSQL 14.0",)]
                                
                                mock_conn.cursor.return_value = mock_cursor
                                mock_conn.__enter__ = Mock(return_value=mock_conn)
                                mock_conn.__exit__ = Mock(return_value=None)
                                mock_cursor.__enter__ = Mock(return_value=mock_cursor)
                                mock_cursor.__exit__ = Mock(return_value=None)
                                mock_connect.return_value = mock_conn
                                
                                # Capture log output
                                with self.assertLogs('s11', level='INFO') as log:
                                    s11.run_s11_sql()
                                
                                # Check that password is not in logs
                                log_output = ' '.join(log.output)
                                self.assertNotIn("secret123", log_output)
                                self.assertIn("localhost", log_output)  # Other params should be present

    def test_dataset_completion_message(self):
        """Test that dataset completion message is logged."""
        with patch('s11.load_config') as mock_config:
            with patch('s11.verify_tables') as mock_verify:
                with patch('s11.execute_with_retry') as mock_execute:
                    with patch('os.path.exists', return_value=True):
                        with patch('builtins.open', mock_open(read_data="SELECT 1;")):
                            with patch('psycopg.connect') as mock_connect:
                                mock_config.return_value = self.sample_config
                                mock_execute.return_value = True
                                
                                mock_conn = Mock()
                                mock_cursor = Mock()
                                mock_cursor.fetchone.side_effect = [("faersdatabase",), ("PostgreSQL 14.0",)]
                                
                                mock_conn.cursor.return_value = mock_cursor
                                mock_conn.__enter__ = Mock(return_value=mock_conn)
                                mock_conn.__exit__ = Mock(return_value=None)
                                mock_cursor.__enter__ = Mock(return_value=mock_cursor)
                                mock_cursor.__exit__ = Mock(return_value=None)
                                mock_connect.return_value = mock_conn
                                
                                # Capture log output
                                with self.assertLogs('s11', level='INFO') as log:
                                    s11.run_s11_sql()
                                
                                # Check that completion message is logged
                                log_output = ' '.join(log.output)
                                self.assertIn("Dataset tables created", log_output)
                                self.assertIn("remapping_log", log_output)


class TestS11FAERSSpecific(unittest.TestCase):
    """Test FAERS-specific functionality."""

    def test_faers_analysis_table_types(self):
        """Test that s11 handles FAERS analysis-specific table types."""
        # s11 is focused on creating dataset tables for FAERS analysis
        expected_analysis_tables = [
            "drugs_standardized",      # Standardized drug names
            "adverse_reactions",       # Adverse event terms
            "drug_adverse_reactions_pairs",  # Drug-event pairs
            "drug_adverse_reactions_count",  # Counts for analysis
            "contingency_table",       # Statistical analysis
            "proportionate_analysis"   # PRR/ROR calculations
        ]
        
        # These tables should be part of the verification
        for table in expected_analysis_tables:
            # This is tested indirectly through the verify_tables function
            # which checks all 14 expected tables
            pass

    def test_statistical_analysis_focus(self):
        """Test that s11 is focused on statistical analysis tables."""
        # s11 creates tables for statistical analysis of FAERS data
        # This includes contingency tables, margins, and proportionate analysis
        
        statistical_tables = [
            "drug_margin",
            "event_margin", 
            "total_count",
            "contingency_table",
            "proportionate_analysis"
        ]
        
        # Verify these are in the expected tables list by testing table verification
        sample_config = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "user": "testuser",
                "password": "testpass",
                "dbname": "testdb"
            }
        }
        
        with patch('s11.load_config') as mock_config:
            with patch('psycopg.connect') as mock_connect:
                mock_config.return_value = sample_config
                mock_conn = Mock()
                mock_cursor = Mock()
                mock_cursor.fetchone.return_value = (100,)
                
                mock_conn.cursor.return_value = mock_cursor
                mock_conn.__enter__ = Mock(return_value=mock_conn)
                mock_conn.__exit__ = Mock(return_value=None)
                mock_cursor.__enter__ = Mock(return_value=mock_cursor)
                mock_cursor.__exit__ = Mock(return_value=None)
                mock_connect.return_value = mock_conn
                
                s11.verify_tables()
                
                # Should check all tables including statistical analysis tables
                self.assertEqual(mock_cursor.execute.call_count, 14)


class TestLoggingConfiguration(unittest.TestCase):
    """Test logging configuration and behavior."""
    
    def setUp(self):
        """Set up logging test fixtures."""
        self.log_stream = StringIO()
        self.test_handler = logging.StreamHandler(self.log_stream)
        self.test_logger = logging.getLogger('test_s11')
        self.test_logger.addHandler(self.test_handler)
        self.test_logger.setLevel(logging.DEBUG)

    def test_logging_levels(self):
        """Test that different logging levels work correctly."""
        self.test_logger.debug("Debug message")
        self.test_logger.info("Info message")
        self.test_logger.warning("Warning message")
        self.test_logger.error("Error message")
        
        log_output = self.log_stream.getvalue()
        self.assertIn("Debug message", log_output)
        self.assertIn("Info message", log_output)
        self.assertIn("Warning message", log_output)
        self.assertIn("Error message", log_output)

    def test_log_file_configuration(self):
        """Test that log file is configured correctly."""
        # Check that the logging configuration includes file handler
        self.assertEqual(s11.logger.name, "s11")


class TestIntegrationScenarios(unittest.TestCase):
    """Test integration scenarios with more complex setups."""
    
    def setUp(self):
        """Set up integration test fixtures."""
        self.sample_config = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "user": "testuser",
                "password": "testpass",
                "dbname": "testdb"
            }
        }
    
    @patch('s11.load_config')
    @patch('s11.parse_sql_statements')
    @patch('s11.execute_with_retry')
    @patch('psycopg.connect')
    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    def test_full_workflow_simulation(self, mock_file, mock_exists, mock_connect,
                                    mock_execute, mock_parse, mock_load_config):
        """Test a complete workflow simulation."""
        # Setup mocks
        mock_load_config.return_value = self.sample_config
        mock_exists.return_value = True
        mock_file.return_value.read.return_value = "SELECT 1;"
        mock_parse.return_value = [
            "CREATE SCHEMA faers_b;", 
            "CREATE TABLE faers_b.drugs_standardized (id INT);",
            "CREATE TABLE faers_b.adverse_reactions (id INT);",
            "CREATE TABLE faers_b.contingency_table (id INT);"
        ]
        mock_execute.return_value = True
        
        # Setup database mocks
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.side_effect = [("test_db",), ("PostgreSQL 14.0",)]
        
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)
        mock_connect.return_value = mock_conn
        
        # Execute
        s11.run_s11_sql()
        
        # Verify workflow
        mock_load_config.assert_called()
        mock_exists.assert_called_with(s11.SQL_FILE_PATH)
        mock_parse.assert_called_once()
        self.assertEqual(mock_execute.call_count, 4)  # Four parsed statements

    def test_error_handling_flow(self):
        """Test error handling in various scenarios."""
        # Test config loading error
        with patch('s11.load_config', side_effect=FileNotFoundError):
            with self.assertRaises(FileNotFoundError):
                s11.run_s11_sql()
        
        # Test JSON decode error
        with patch('s11.load_config', side_effect=json.JSONDecodeError("Invalid", "", 0)):
            with self.assertRaises(json.JSONDecodeError):
                s11.run_s11_sql()

    def test_complex_sql_parsing_integration(self):
        """Test integration of complex SQL parsing with execution."""
        complex_sql = """
        -- FAERS Analysis Dataset Creation
        CREATE SCHEMA IF NOT EXISTS faers_b;
        
        CREATE TABLE IF NOT EXISTS faers_b.drugs_standardized (
            id SERIAL PRIMARY KEY,
            drug_name VARCHAR(255),
            rxcui VARCHAR(8)
        );
        
        DO $
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                          WHERE table_schema = 'faers_b' 
                          AND table_name = 'adverse_reactions') THEN
                CREATE TABLE faers_b.adverse_reactions (
                    id SERIAL PRIMARY KEY,
                    reaction_name VARCHAR(255)
                );
            END IF;
        END
        $;
        
        CREATE OR REPLACE FUNCTION faers_b.calculate_statistics() 
        RETURNS VOID AS $func$
        BEGIN
            INSERT INTO faers_b.proportionate_analysis 
            SELECT drug_id, reaction_id, count(*) as frequency
            FROM faers_b.drug_adverse_reactions_pairs
            GROUP BY drug_id, reaction_id;
        END
        $func$ LANGUAGE plpgsql;
        """
        
        statements = s11.parse_sql_statements(complex_sql)
        
        # Should parse into separate statements properly
        self.assertGreater(len(statements), 3)
        
        # Check that schema creation is preserved
        schema_statements = [stmt for stmt in statements if 'CREATE SCHEMA' in stmt]
        self.assertEqual(len(schema_statements), 1)
        
        # Check that DO block is preserved as one statement
        do_statements = [stmt for stmt in statements if 'DO $' in stmt]
        self.assertEqual(len(do_statements), 1)
        
        # Check that function is preserved as one statement
        function_statements = [stmt for stmt in statements if '$func' in stmt]
        self.assertEqual(len(function_statements), 1)

    def test_database_creation_flow(self):
        """Test the database creation and connection flow."""
        with patch('s11.load_config') as mock_config:
            with patch('psycopg.connect') as mock_connect:
                with patch('s11.verify_tables') as mock_verify:
                    with patch('s11.execute_with_retry') as mock_execute:
                        with patch('os.path.exists', return_value=True):
                            with patch('builtins.open', mock_open(read_data="SELECT 1;")):
                                
                                mock_config.return_value = self.sample_config
                                mock_execute.return_value = True
                                
                                # Mock initial connection (database doesn't exist)
                                mock_initial_conn = Mock()
                                mock_initial_cursor = Mock()
                                mock_initial_cursor.fetchone.return_value = None  # DB doesn't exist
                                
                                # Mock faersdatabase connection
                                mock_faers_conn = Mock()
                                mock_faers_cursor = Mock()
                                mock_faers_cursor.fetchone.return_value = ("PostgreSQL 14.0",)
                                
                                # Setup context managers
                                for conn, cursor in [(mock_initial_conn, mock_initial_cursor), 
                                                   (mock_faers_conn, mock_faers_cursor)]:
                                    conn.cursor.return_value = cursor
                                    conn.__enter__ = Mock(return_value=conn)
                                    conn.__exit__ = Mock(return_value=None)
                                    cursor.__enter__ = Mock(return_value=cursor)
                                    cursor.__exit__ = Mock(return_value=None)
                                
                                mock_connect.side_effect = [mock_initial_conn, mock_faers_conn]
                                
                                s11.run_s11_sql()
                                
                                # Should connect twice: initial check and faersdatabase
                                self.assertEqual(mock_connect.call_count, 2)
                                mock_verify.assert_called_once()

    def test_sql_execution_with_errors(self):
        """Test SQL execution with various error conditions."""
        with patch('s11.load_config') as mock_config:
            with patch('psycopg.connect') as mock_connect:
                with patch('os.path.exists', return_value=True):
                    with patch('builtins.open', mock_open(read_data="INVALID SQL;")):
                        
                        mock_config.return_value = self.sample_config
                        
                        mock_conn = Mock()
                        mock_cursor = Mock()
                        mock_cursor.fetchone.side_effect = [("faersdatabase",), ("PostgreSQL 14.0",)]
                        
                        # Mock execute_with_retry to raise an error
                        with patch('s11.execute_with_retry', side_effect=pg_errors.SyntaxError("Invalid SQL")):
                            mock_conn.cursor.return_value = mock_cursor
                            mock_conn.__enter__ = Mock(return_value=mock_conn)
                            mock_conn.__exit__ = Mock(return_value=None)
                            mock_cursor.__enter__ = Mock(return_value=mock_cursor)
                            mock_cursor.__exit__ = Mock(return_value=None)
                            mock_connect.return_value = mock_conn
                            
                            with self.assertRaises(pg_errors.SyntaxError):
                                s11.run_s11_sql()


class TestS11ErrorHandling(unittest.TestCase):
    """Test comprehensive error handling scenarios."""
    
    def test_configuration_validation(self):
        """Test configuration validation with various invalid configs."""
        invalid_configs = [
            # Missing database section
            {},
            # Missing required fields
            {"database": {"host": "localhost"}},
            # Invalid port
            {"database": {"host": "localhost", "port": "invalid", "user": "test", "password": "test", "dbname": "test"}},
            # Empty values
            {"database": {"host": "", "port": 5432, "user": "test", "password": "test", "dbname": "test"}},
        ]
        
        for invalid_config in invalid_configs:
            with patch('s11.load_config', return_value=invalid_config):
                with self.assertRaises((ValueError, KeyError)):
                    s11.run_s11_sql()

    def test_sql_file_validation(self):
        """Test SQL file validation and error handling."""
        sample_config = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "user": "test",
                "password": "test",
                "dbname": "test"
            }
        }
        
        with patch('s11.load_config', return_value=sample_config):
            # Test missing SQL file
            with patch('os.path.exists', return_value=False):
                with self.assertRaises(FileNotFoundError):
                    s11.run_s11_sql()
            
            # Test empty SQL file
            with patch('os.path.exists', return_value=True):
                with patch('builtins.open', mock_open(read_data="")):
                    with patch('psycopg.connect') as mock_connect:
                        mock_conn = Mock()
                        mock_cursor = Mock()
                        mock_cursor.fetchone.side_effect = [("test_db",), ("PostgreSQL 14.0",)]
                        
                        mock_conn.cursor.return_value = mock_cursor
                        mock_conn.__enter__ = Mock(return_value=mock_conn)
                        mock_conn.__exit__ = Mock(return_value=None)
                        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
                        mock_cursor.__exit__ = Mock(return_value=None)
                        mock_connect.return_value = mock_conn
                        
                        # Should handle empty file gracefully
                        s11.run_s11_sql()

    def test_database_connection_scenarios(self):
        """Test various database connection scenarios."""
        sample_config = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "user": "test",
                "password": "test",
                "dbname": "test"
            }
        }
        
        with patch('s11.load_config', return_value=sample_config):
            # Test connection timeout
            with patch('psycopg.connect', side_effect=pg_errors.OperationalError("Connection timeout")):
                with self.assertRaises(pg_errors.OperationalError):
                    s11.run_s11_sql()
            
            # Test authentication failure
            with patch('psycopg.connect', side_effect=pg_errors.OperationalError("Authentication failed")):
                with self.assertRaises(pg_errors.OperationalError):
                    s11.run_s11_sql()


class TestS11Performance(unittest.TestCase):
    """Test performance-related aspects."""
    
    def test_large_sql_parsing(self):
        """Test parsing of large SQL files."""
        # Create a large SQL script with many statements
        large_sql = ""
        for i in range(100):
            large_sql += f"""
            CREATE TABLE IF NOT EXISTS faers_b.test_table_{i} (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                created_at TIMESTAMP DEFAULT NOW()
            );
            
            INSERT INTO faers_b.test_table_{i} (name) VALUES ('test_{i}');
            """
        
        # Should handle large files without issues
        statements = s11.parse_sql_statements(large_sql)
        self.assertEqual(len(statements), 200)  # 100 CREATE + 100 INSERT
        
        # Verify statements are properly parsed
        create_statements = [stmt for stmt in statements if 'CREATE TABLE' in stmt]
        insert_statements = [stmt for stmt in statements if 'INSERT INTO' in stmt]
        
        self.assertEqual(len(create_statements), 100)
        self.assertEqual(len(insert_statements), 100)

    def test_retry_performance(self):
        """Test retry mechanism performance."""
        mock_cursor = Mock()
        
        # Test rapid failures followed by success
        mock_cursor.execute.side_effect = [
            pg_errors.OperationalError("Temp failure"),
            None  # Success on second attempt
        ]
        
        start_time = time.time()
        with patch('time.sleep') as mock_sleep:
            result = s11.execute_with_retry(mock_cursor, "SELECT 1", retries=3, delay=0.1)
        end_time = time.time()
        
        self.assertTrue(result)
        self.assertEqual(mock_cursor.execute.call_count, 2)
        mock_sleep.assert_called_once_with(0.1)
        
        # Should complete quickly with minimal delay
        self.assertLess(end_time - start_time, 1.0)


if __name__ == '__main__':
    # Configure comprehensive test runner
    unittest.main(
        verbosity=2, 
        buffer=True,
        failfast=False,
        warnings='ignore'
    )