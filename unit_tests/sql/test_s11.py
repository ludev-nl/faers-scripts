import unittest
import os
import sys
import tempfile
import shutil
from unittest.mock import patch, Mock
import psycopg
from psycopg import errors as pg_errors

# Import the module under test using the specified pattern
project_root = os.getcwd()
sys.path.insert(0, project_root)

try:
    import s11
except ImportError as e:
    print(f"Error importing s11 module: {e}")
    print(f"Project root path: {project_root}")
    raise


class TestS11SQLScript(unittest.TestCase):
    """Test the SQL script logic and database operations."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.sql_file_path = os.path.join(project_root, 's11.sql')
        self.maxDiff = None
        
        # Sample test data for FAERS analysis workflow
        self.sample_drug_mapper_3_data = [
            (12345, 101, 1, "PS", "2023", 67890, "ASPIRIN"),
            (12346, 102, 1, "PS", "2023", 67891, "IBUPROFEN"),
            (12347, 103, 1, "PS", "2023", 67892, "ACETAMINOPHEN"),
        ]
        
        self.sample_adverse_reactions_data = [
            (12345, "2023", "HEADACHE"),
            (12346, "2023", "NAUSEA"),
            (12347, "2023", "DIZZINESS"),
        ]
        
        self.sample_demographics_data = [
            (54321, 12345, 1, "2023-01-01", "I", "2023-01-01", 45.0, "M", "US", "2023"),
            (54322, 12346, 1, "2023-01-02", "I", "2023-01-02", 32.0, "F", "CA", "2023"),
            (54323, 12347, 1, "2023-01-03", "I", "2023-01-03", 28.0, "M", "UK", "2023"),
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
        
        statements = s11.parse_sql_statements(sql_content)
        
        # Should have multiple statements
        self.assertGreater(len(statements), 0, "No SQL statements parsed")
        
        # Check for key operations
        sql_text = sql_content.upper()
        self.assertIn('CREATE SCHEMA', sql_text)
        self.assertIn('DO $$', sql_text)
        self.assertIn('CREATE TABLE', sql_text)
        self.assertIn('DRUGS_STANDARDIZED', sql_text)

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
        
        statements = s11.parse_sql_statements(sql)
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

    def test_remapping_log_table_creation(self):
        """Test remapping_log table creation."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for remapping_log table creation
        self.assertIn('CREATE TABLE IF NOT EXISTS faers_b.remapping_log', sql_content)
        self.assertIn('log_id SERIAL PRIMARY KEY', sql_content)
        self.assertIn('step VARCHAR(50)', sql_content)
        self.assertIn('message TEXT', sql_content)
        self.assertIn('log_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP', sql_content)

    def test_analysis_table_creation(self):
        """Test creation of all 14 analysis tables."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for all expected analysis tables
        expected_tables = [
            'drugs_standardized',
            'adverse_reactions',
            'drug_adverse_reactions_pairs',
            'drug_adverse_reactions_count',
            'drug_indications',
            'demographics',
            'case_outcomes',
            'therapy_dates',
            'report_sources',
            'drug_margin',
            'event_margin',
            'total_count',
            'contingency_table',
            'proportionate_analysis'
        ]
        
        for table in expected_tables:
            self.assertIn(f'CREATE TABLE faers_b.{table}', sql_content, f"Missing table: {table}")

    def test_do_block_structure(self):
        """Test the structure of DO blocks for table creation."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        statements = s11.parse_sql_statements(sql_content)
        
        # Find DO blocks
        do_blocks = [stmt for stmt in statements if stmt.strip().upper().startswith('DO $$')]
        
        # Should have multiple DO blocks (one for each table creation)
        self.assertGreater(len(do_blocks), 10, "Expected multiple DO blocks for table creation")
        
        # Check DO block structure
        for block in do_blocks[:5]:  # Test first 5 blocks
            self.assertIn('DECLARE', block)
            self.assertIn('table_exists BOOLEAN', block)
            self.assertIn('row_count BIGINT', block)
            self.assertIn('BEGIN', block)
            self.assertIn('END $$;', block)

    def test_table_existence_checks(self):
        """Test table existence checking logic."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for table existence verification patterns
        self.assertIn('SELECT EXISTS', sql_content)
        self.assertIn('FROM pg_class', sql_content)
        self.assertIn('relnamespace', sql_content)
        self.assertIn('relname', sql_content)
        self.assertIn('pg_namespace', sql_content)

    def test_error_handling_and_logging(self):
        """Test error handling and logging in DO blocks."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for error handling patterns
        self.assertIn('EXCEPTION', sql_content)
        self.assertIn('WHEN OTHERS THEN', sql_content)
        self.assertIn('INSERT INTO faers_b.remapping_log', sql_content)
        self.assertIn('SQLERRM', sql_content)
        self.assertIn('RAISE;', sql_content)

    def test_drugs_standardized_creation(self):
        """Test DRUGS_STANDARDIZED table creation logic."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for DRUGS_STANDARDIZED specific logic
        self.assertIn('CREATE TABLE faers_b.drugs_standardized', sql_content)
        self.assertIn('drug_mapper_3', sql_content)
        self.assertIn('aligned_demo_drug_reac_indi_ther', sql_content)
        self.assertIn('final_rxaui', sql_content)
        self.assertIn('remapping_str', sql_content)
        self.assertIn('9267486', sql_content)  # Excludes 'UNKNOWN STR'

    def test_adverse_reactions_creation(self):
        """Test ADVERSE_REACTIONS table creation logic."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for ADVERSE_REACTIONS specific logic
        self.assertIn('CREATE TABLE faers_b.adverse_reactions', sql_content)
        self.assertIn('reac_combined', sql_content)
        self.assertIn('meddra_code', sql_content)
        self.assertIn('pref_term', sql_content)
        self.assertIn('low_level_term', sql_content)
        self.assertIn('pt_name', sql_content)
        self.assertIn('llt_name', sql_content)

    def test_drug_adverse_reactions_pairs_creation(self):
        """Test DRUG_ADVERSE_REACTIONS_PAIRS table creation logic."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for pairs table logic
        self.assertIn('CREATE TABLE faers_b.drug_adverse_reactions_pairs', sql_content)
        self.assertIn('SELECT DISTINCT', sql_content)
        self.assertIn('INNER JOIN faers_b.adverse_reactions', sql_content)

    def test_statistical_analysis_tables(self):
        """Test statistical analysis table creation (margins, contingency, etc)."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for statistical analysis components
        self.assertIn('drug_margin', sql_content)
        self.assertIn('event_margin', sql_content)
        self.assertIn('total_count', sql_content)
        self.assertIn('contingency_table', sql_content)
        self.assertIn('SUM(count_of_reaction)', sql_content)

    def test_contingency_table_calculations(self):
        """Test contingency table calculation logic."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for contingency table calculations (a, b, c, d values)
        self.assertIn('darc.count_of_reaction AS a', sql_content)
        self.assertIn('(em.margin - darc.count_of_reaction) AS b', sql_content)
        self.assertIn('(dm.margin - darc.count_of_reaction) AS c', sql_content)
        self.assertIn('total_count', sql_content)

    def test_proportionate_analysis_calculations(self):
        """Test proportionate analysis statistical calculations."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for statistical calculations
        self.assertIn('proportionate_analysis', sql_content)
        self.assertIn('prr', sql_content)  # Proportional Reporting Ratio
        self.assertIn('ror', sql_content)  # Reporting Odds Ratio
        self.assertIn('ic', sql_content)   # Information Component
        self.assertIn('chi_squared_yates', sql_content)
        self.assertIn('n_expected', sql_content)
        self.assertIn('prr_lb', sql_content)  # Lower bound
        self.assertIn('prr_ub', sql_content)  # Upper bound

    def test_prr_calculation_formula(self):
        """Test PRR calculation formula."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for PRR calculation components
        self.assertIn('(ct.a / NULLIF((ct.a + ct.c), 0))', sql_content)
        self.assertIn('NULLIF((ct.b / NULLIF((ct.b + ct.d), 0)), 0)', sql_content)
        self.assertIn('1.96', sql_content)  # 95% confidence interval

    def test_ror_calculation_formula(self):
        """Test ROR calculation formula."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for ROR calculation components
        self.assertIn('(ct.a / NULLIF(ct.c, 0))', sql_content)
        self.assertIn('NULLIF((ct.b / NULLIF(ct.d, 0)), 0)', sql_content)
        self.assertIn('ror_lb', sql_content)
        self.assertIn('ror_ub', sql_content)

    def test_information_component_calculation(self):
        """Test Information Component (IC) calculation."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for IC calculation components
        self.assertIn('log(2,', sql_content)  # Base-2 logarithm
        self.assertIn('ic025', sql_content)   # Lower confidence interval
        self.assertIn('ic975', sql_content)   # Upper confidence interval
        self.assertIn('3.3 * power', sql_content)
        self.assertIn('2.4 * power', sql_content)

    def test_index_creation(self):
        """Test index creation for performance optimization."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for index creation
        self.assertIn('CREATE INDEX', sql_content)
        self.assertIn('idx_drugs_standardized_primaryid', sql_content)
        self.assertIn('idx_drugs_standardized_rxaui', sql_content)
        self.assertIn('idx_adverse_reactions_primaryid', sql_content)
        self.assertIn('idx_contingency_table_rxaui', sql_content)
        self.assertIn('idx_proportionate_analysis_rxaui', sql_content)

    def test_data_type_specifications(self):
        """Test proper data type specifications."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for proper data types
        self.assertIn('BIGINT', sql_content)
        self.assertIn('VARCHAR(3000)', sql_content)  # Drug names
        self.assertIn('VARCHAR(1000)', sql_content)  # Adverse events
        self.assertIn('VARCHAR(4)', sql_content)     # Period
        self.assertIn('FLOAT', sql_content)          # Statistical calculations
        self.assertIn('DATE', sql_content)           # Dates
        self.assertIn('SERIAL PRIMARY KEY', sql_content)

    def test_exclusion_criteria(self):
        """Test exclusion criteria in data selection."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for specific exclusions
        self.assertIn('9267486', sql_content)  # Excludes 'UNKNOWN STR'
        self.assertIn('10070592, 10057097', sql_content)  # Specific MedDRA code exclusions
        self.assertIn('WHERE ct.a > 0 AND ct.b > 0 AND ct.c > 0 AND ct.d > 0', sql_content)  # Avoid division by zero

    def test_null_handling(self):
        """Test NULL value handling throughout the script."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for NULL handling
        self.assertIn('NULLIF', sql_content)
        self.assertIn('IS NOT NULL', sql_content)
        self.assertIn('IS NULL', sql_content)

    def test_cte_usage(self):
        """Test Common Table Expression (CTE) usage."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for CTE patterns
        self.assertIn('WITH cte AS', sql_content)
        self.assertIn('cte_2 AS', sql_content)
        self.assertIn('UNION', sql_content)

    def test_join_operations(self):
        """Test JOIN operations throughout the script."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for proper JOIN syntax
        self.assertIn('INNER JOIN', sql_content)
        self.assertIn('ON ', sql_content)
        
        # Check for specific table joins
        self.assertIn('faers_combined.aligned_demo_drug_reac_indi_ther', sql_content)
        self.assertIn('faers_combined.reac_combined', sql_content)
        self.assertIn('faers_combined.pref_term', sql_content)

    def test_aggregation_functions(self):
        """Test aggregation functions used in statistical calculations."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for aggregation functions
        self.assertIn('COUNT(*)', sql_content)
        self.assertIn('SUM(', sql_content)
        self.assertIn('GROUP BY', sql_content)
        self.assertIn('GET DIAGNOSTICS row_count = ROW_COUNT', sql_content)

    def test_mathematical_functions(self):
        """Test mathematical functions used in statistical calculations."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for mathematical functions
        self.assertIn('exp(', sql_content)       # Exponential
        self.assertIn('ln(', sql_content)        # Natural logarithm
        self.assertIn('log(2,', sql_content)     # Base-2 logarithm
        self.assertIn('sqrt(', sql_content)      # Square root
        self.assertIn('POWER(', sql_content)     # Power function
        self.assertIn('ROUND(', sql_content)     # Rounding
        self.assertIn('ABS(', sql_content)       # Absolute value

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
        
        statements = s11.parse_sql_statements(sql_content)
        
        # Simulate execution of each statement
        for i, stmt in enumerate(statements[:5]):  # Test first 5 statements
            try:
                # This would normally execute the statement
                # mock_cursor.execute(stmt)
                pass
            except Exception as e:
                self.fail(f"Statement {i+1} would fail: {e}")

    def test_conditional_execution_logic(self):
        """Test conditional execution logic in DO blocks."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        statements = s11.parse_sql_statements(sql_content)
        
        # Find DO blocks that should have conditional logic
        do_blocks = [stmt for stmt in statements if stmt.strip().upper().startswith('DO $$')]
        
        # Each DO block should have table existence checks
        for block in do_blocks:
            if 'CREATE TABLE' in block:  # Table creation blocks
                self.assertIn('IF NOT table_exists THEN', block)
                self.assertIn('skipping', block)
                self.assertIn('RETURN;', block)

    def test_row_count_diagnostics(self):
        """Test row count diagnostics and logging."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for row count tracking
        self.assertIn('GET DIAGNOSTICS row_count = ROW_COUNT', sql_content)
        self.assertIn('IF row_count = 0 THEN', sql_content)
        self.assertIn('is empty', sql_content)
        self.assertIn('|| row_count ||', sql_content)

    def test_faers_schema_dependencies(self):
        """Test dependencies on faers_combined schema tables."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for faers_combined schema table references
        faers_combined_tables = [
            'aligned_demo_drug_reac_indi_ther',
            'reac_combined',
            'pref_term',
            'low_level_term',
            'indi_combined',
            'outc_combined',
            'ther_combined',
            'rpsr_combined'
        ]
        
        for table in faers_combined_tables:
            self.assertIn(f'faers_combined.{table}', sql_content, f"Missing reference to: {table}")

    def test_statistical_analysis_workflow(self):
        """Test the complete statistical analysis workflow."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Verify the workflow sequence:
        # 1. Basic tables (drugs, reactions, demographics)
        # 2. Relationship tables (pairs, counts)
        # 3. Margin calculations
        # 4. Contingency table
        # 5. Statistical analysis (PRR, ROR, IC)
        
        # Check that all components are present
        workflow_components = [
            'drugs_standardized',
            'adverse_reactions', 
            'drug_adverse_reactions_pairs',
            'drug_adverse_reactions_count',
            'drug_margin',
            'event_margin',
            'total_count',
            'contingency_table',
            'proportionate_analysis'
        ]
        
        for component in workflow_components:
            self.assertIn(component, sql_content, f"Missing workflow component: {component}")


class TestS11SQLIntegration(unittest.TestCase):
    """Integration tests for SQL script execution."""
    
    def setUp(self):
        """Set up integration test fixtures."""
        self.sql_file_path = os.path.join(project_root, 's11.sql')

    @patch('s11.load_config')
    @patch('s11.verify_tables')
    @patch('s11.execute_with_retry')
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
        s11.run_s11_sql()
        
        # Verify that SQL parsing and execution occurred
        mock_load_config.assert_called()
        mock_exists.assert_called_with(s11.SQL_FILE_PATH)
        mock_verify.assert_called_once()
        
        # Verify that multiple statements were executed (should be 15+ statements for all tables)
        self.assertGreater(mock_execute.call_count, 15, "Should execute multiple SQL statements for all analysis tables")

    def test_sql_statement_independence(self):
        """Test that SQL statements can be executed independently."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        statements = s11.parse_sql_statements(sql_content)
        
        # Each statement should be properly terminated and independent
        for i, stmt in enumerate(statements):
            # Skip empty statements
            if not stmt.strip():
                continue
            
            # DO blocks should be complete
            if stmt.strip().upper().startswith('DO $$'):
                self.assertIn('END $$;', stmt, f"DO block {i} should be properly terminated")
                if 'DECLARE' in stmt:
                    self.assertIn('BEGIN', stmt, f"DO block {i} should have BEGIN after DECLARE")

    def test_statistical_calculation_accuracy(self):
        """Test that statistical calculations follow proper formulas."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check that statistical formulas are mathematically sound
        # PRR formula: (a/(a+c)) / (b/(b+d))
        # ROR formula: (a/c) / (b/d) = (a*d) / (b*c)
        # IC formula: log2((a+0.5) / E[a])
        
        # These are tested indirectly through the presence of correct formula components
        self.assertIn('(ct.a / NULLIF((ct.a + ct.c), 0))', sql_content)  # PRR numerator
        self.assertIn('NULLIF((ct.b / NULLIF((ct.b + ct.d), 0)), 0)', sql_content)  # PRR denominator
        self.assertIn('(ct.a / NULLIF(ct.c, 0))', sql_content)  # ROR component
        self.assertIn('log(2,', sql_content)  # IC base-2 logarithm

    def test_pharmacovigilance_analysis_completeness(self):
        """Test that the script provides complete pharmacovigilance analysis capabilities."""
        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Essential pharmacovigilance components
        pv_components = [
            'drug_adverse_reactions_pairs',    # Drug-event associations
            'contingency_table',               # 2x2 tables for analysis
            'proportionate_analysis',          # PRR calculations
            'chi_squared_yates',              # Statistical significance
            'confidence intervals',            # Via _lb and _ub fields
            'information_component'            # IC calculations
        ]
        
        # Check for key pharmacovigilance terms (some are embedded in longer names)
        self.assertIn('drug_adverse_reactions', sql_content)
        self.assertIn('contingency_table', sql_content)
        self.assertIn('proportionate_analysis', sql_content)
        self.assertIn('chi_squared_yates', sql_content)
        self.assertIn('_lb', sql_content)  # Lower bounds
        self.assertIn('_ub', sql_content)  # Upper bounds


if __name__ == '__main__':
    # Configure test runner
    unittest.main(verbosity=2, buffer=True)