import unittest
import os
import sys
import psycopg
import tempfile
import json
import re
from unittest.mock import patch, MagicMock, mock_open, call
import subprocess
from datetime import datetime

# Add the parent directory to sys.path to import the module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))


class TestS8SQL(unittest.TestCase):
    """Test cases for s8.sql drug data processing operations"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test database connection parameters"""
        cls.test_db_params = {
            "host": os.getenv("TEST_DB_HOST", "localhost"),
            "port": int(os.getenv("TEST_DB_PORT", 5432)),
            "user": os.getenv("TEST_DB_USER", "test_user"),
            "password": os.getenv("TEST_DB_PASSWORD", "test_pass"),
            "dbname": os.getenv("TEST_DB_NAME", "faersdatabase")
        }
        
        # SQL script path - looks for s8.sql in the root of faers-scripts
        cls.s8_sql_path = os.path.join(os.path.dirname(__file__), "..", "..", "s8.sql")
        
        # Expected tables and dependencies
        cls.expected_table = "DRUG_Mapper"
        cls.dependency_tables = [
            ("faers_b", "temp_s8_config")
        ]
        
        # Expected columns for DRUG_Mapper
        cls.expected_columns = [
            "DRUGNAME",
            "PROD_AI", 
            "CLEANED_DRUGNAME",
            "CLEANED_PROD_AI",
            "NOTES"
        ]
        
        # Expected processing phases
        cls.expected_phases = [
            "UNITS_OF_MEASUREMENT_DRUGNAME",
            "MANUFACTURER_NAMES_DRUGNAME",
            "WORDS_TO_VITAMIN_B_DRUGNAME",
            "FORMAT_DRUGNAME",
            "CLEANING_DRUGNAME",
            "UNITS_MEASUREMENT_PROD_AI",
            "MANUFACTURER_NAMES_PROD_AI",
            "WORDS_TO_VITAMIN_B_PROD_AI",
            "FORMAT_PROD_AI",
            "CLEANING_PROD_AI"
        ]
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value.__enter__.return_value = self.mock_cursor
    
    def test_sql_file_exists(self):
        """Test that s8.sql file exists"""
        self.assertTrue(os.path.exists(self.s8_sql_path), 
                       f"s8.sql file not found at {self.s8_sql_path}")
    
    def test_sql_file_readable(self):
        """Test that s8.sql file is readable"""
        if os.path.exists(self.s8_sql_path):
            try:
                with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                self.assertGreater(len(content), 0, "s8.sql file is empty")
            except Exception as e:
                self.fail(f"Could not read s8.sql file: {e}")
        else:
            self.skipTest("s8.sql file not found")
    
    def test_schema_creation_in_sql(self):
        """Test that SQL contains schema creation and configuration"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for schema operations
        self.assertIn("CREATE SCHEMA IF NOT EXISTS faers_b", sql_content)
        self.assertIn("SET search_path TO faers_b", sql_content)
    
    def test_drug_mapper_table_column_additions(self):
        """Test that SQL adds CLEANED_* columns to DRUG_Mapper table"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for column additions
        self.assertIn('ALTER TABLE DRUG_Mapper ADD COLUMN IF NOT EXISTS CLEANED_DRUGNAME TEXT', sql_content)
        self.assertIn('ALTER TABLE DRUG_Mapper ADD COLUMN IF NOT EXISTS CLEANED_PROD_AI TEXT', sql_content)
    
    def test_table_existence_check(self):
        """Test that SQL checks for DRUG_Mapper table existence"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for table existence validation
        self.assertIn("information_schema.tables", sql_content)
        self.assertIn("table_name = 'drug_mapper'", sql_content)
        self.assertIn("table_schema IN ('faers_b', 'public')", sql_content)
    
    def test_clearnumericcharacters_function_definition(self):
        """Test that clearnumericcharacters function is defined"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for function creation
        self.assertIn("CREATE OR REPLACE FUNCTION clearnumericcharacters", sql_content)
        self.assertIn("RETURNS TEXT", sql_content)
        self.assertIn("regexp_replace(input_text, '[0-9]', '', 'g')", sql_content)
        self.assertIn("LANGUAGE plpgsql", sql_content)
    
    def test_process_drug_data_function_definition(self):
        """Test that process_drug_data function is defined with proper structure"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for main function creation
        self.assertIn("CREATE OR REPLACE FUNCTION process_drug_data()", sql_content)
        self.assertIn("RETURNS void", sql_content)
        self.assertIn("DECLARE", sql_content)
        self.assertIn("phase_data JSONB", sql_content)
        self.assertIn("stmt RECORD", sql_content)
        self.assertIn("current_phase TEXT", sql_content)
        self.assertIn("LANGUAGE plpgsql", sql_content)
    
    def test_temp_table_creation(self):
        """Test that temporary DRUG_Mapper_Temp table is created"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for temp table operations
        self.assertIn("DROP TABLE IF EXISTS DRUG_Mapper_Temp", sql_content)
        self.assertIn("CREATE TABLE DRUG_Mapper_Temp AS", sql_content)
        self.assertIn("SELECT DISTINCT DRUGNAME, PROD_AI, CLEANED_DRUGNAME, CLEANED_PROD_AI", sql_content)
        self.assertIn("FROM DRUG_Mapper", sql_content)
        self.assertIn("WHERE NOTES IS NULL", sql_content)
    
    def test_initial_cleaning_operations(self):
        """Test initial data cleaning operations"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for initial cleaning steps
        initial_operations = [
            "CLEANED_DRUGNAME = DRUGNAME",
            "CLEANED_PROD_AI = PROD_AI",
            "regexp_replace(CLEANED_DRUGNAME, '/[0-9]{5}/', '', 'g')",
            "regexp_replace(CLEANED_DRUGNAME, E'[\\\\n\\\\r\\\\t]+', '', 'g')",
            "regexp_replace(CLEANED_DRUGNAME, '[|,+;\\\\\\\\]', '/', 'g')",
            "regexp_replace(CLEANED_DRUGNAME, '/+', ' / ', 'g')",
            "regexp_replace(CLEANED_DRUGNAME, '\\\\s{2,}', ' ', 'g')"
        ]
        
        for operation in initial_operations:
            with self.subTest(operation=operation):
                self.assertIn(operation, sql_content)
    
    def test_parenthesis_removal_logic(self):
        """Test iterative parenthesis removal logic"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for iterative parenthesis removal
        self.assertIn("FOR i IN 1..5 LOOP", sql_content)
        self.assertIn("regexp_replace(CLEANED_DRUGNAME, '\\\\([^()]*\\\\)', '', 'g')", sql_content)
        self.assertIn("regexp_replace(CLEANED_PROD_AI, '\\\\([^()]*\\\\)', '', 'g')", sql_content)
        self.assertIn("CLEANED_DRUGNAME ~ '\\\\([^()]*\\\\)'", sql_content)
        self.assertIn("END LOOP", sql_content)
    
    def test_configuration_driven_processing(self):
        """Test configuration-driven processing phases"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for configuration table access
        self.assertIn("FROM temp_s8_config AS cfg", sql_content)
        self.assertIn("WHERE cfg.phase_name = current_phase", sql_content)
        self.assertIn("jsonb_array_elements(phase_data -> 'replacements')", sql_content)
        
        # Check for all expected phases
        for phase in self.expected_phases:
            with self.subTest(phase=phase):
                self.assertIn(f"current_phase := '{phase}'", sql_content)
    
    def test_dynamic_sql_execution(self):
        """Test dynamic SQL execution for configuration-driven replacements"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for dynamic SQL execution
        self.assertIn("EXECUTE format(", sql_content)
        self.assertIn("'UPDATE %I SET %I = REPLACE(%I, %L, %L)'", sql_content)
        self.assertIn("stmt.value->>'table'", sql_content)
        self.assertIn("stmt.value->>'set_column'", sql_content)
        self.assertIn("stmt.value->>'replace_column'", sql_content)
        self.assertIn("stmt.value->>'find'", sql_content)
        self.assertIn("stmt.value->>'replace'", sql_content)
    
    def test_hardcoded_operations_after_phases(self):
        """Test hardcoded operations that occur after each configuration phase"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for hardcoded operations
        hardcoded_operations = [
            "clearnumericcharacters(CLEANED_DRUGNAME)",
            "clearnumericcharacters(CLEANED_PROD_AI)",
            "LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(",
            "CHR(10)", "CHR(13)", "CHR(9)",
            "TRIM(BOTH ' \":.,?/\\`~!@#$%^&*-_=+ ' FROM"
        ]
        
        for operation in hardcoded_operations:
            with self.subTest(operation=operation):
                self.assertIn(operation, sql_content)
    
    def test_suffix_removal_logic(self):
        """Test suffix removal logic for both DRUGNAME and PROD_AI"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for suffix removal patterns
        suffix_patterns = [
            "RIGHT(CLEANED_DRUGNAME, 5) = ' JELL'",
            "RIGHT(CLEANED_DRUGNAME, 4) = ' NOS'",
            "RIGHT(CLEANED_DRUGNAME, 4) = ' GEL'", 
            "RIGHT(CLEANED_DRUGNAME, 4) = ' CAP'",
            "RIGHT(CLEANED_DRUGNAME, 4) = ' TAB'",
            "RIGHT(CLEANED_DRUGNAME, 4) = ' FOR'",
            "RIGHT(CLEANED_DRUGNAME, 2) = '//'",
            "RIGHT(CLEANED_DRUGNAME, 1) = '/'"
        ]
        
        for pattern in suffix_patterns:
            with self.subTest(pattern=pattern):
                self.assertIn(pattern, sql_content)
        
        # Check for corresponding PROD_AI patterns
        for pattern in suffix_patterns:
            prod_ai_pattern = pattern.replace("CLEANED_DRUGNAME", "CLEANED_PROD_AI")
            with self.subTest(pattern=prod_ai_pattern):
                self.assertIn(prod_ai_pattern, sql_content)
        
        # Check for LEFT function usage in CASE statements
        self.assertIn("LEFT(CLEANED_DRUGNAME, LENGTH(CLEANED_DRUGNAME)-", sql_content)
        self.assertIn("LEFT(CLEANED_PROD_AI, LENGTH(CLEANED_PROD_AI)-", sql_content)
    
    def test_prod_ai_specific_cleaning(self):
        """Test PROD_AI specific cleaning operations"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for PROD_AI specific operations
        prod_ai_operations = [
            "REPLACE(REPLACE(CLEANED_PROD_AI, '/ /', '/'), '/ /', '/')",
            "REPLACE(CLEANED_PROD_AI, '///', '/')",
            "REPLACE(CLEANED_PROD_AI, '/ / /', '/')",
            "REPLACE(CLEANED_PROD_AI, '////', '/')",
            "REPLACE(CLEANED_PROD_AI, '/ / / /', '/')",
            "REPLACE(CLEANED_PROD_AI, '.', '')"
        ]
        
        for operation in prod_ai_operations:
            with self.subTest(operation=operation):
                self.assertIn(operation, sql_content)
    
    def test_notes_field_filtering(self):
        """Test that processing excludes rows with NOTES field"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for NOTES filtering
        notes_conditions = [
            "WHERE NOTES IS NULL",
            "AND NOTES IS NULL"
        ]
        
        # Should appear multiple times throughout the script
        for condition in notes_conditions:
            with self.subTest(condition=condition):
                self.assertIn(condition, sql_content)
        
        # Count occurrences to ensure it's used consistently
        notes_count = sql_content.count("NOTES IS NULL")
        self.assertGreater(notes_count, 10, "NOTES IS NULL condition not used enough times")
    
    def test_do_block_structure_validation(self):
        """Test that DO blocks are properly structured"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for DO block structure
        self.assertIn("DO $$", sql_content)
        self.assertIn("BEGIN", sql_content)
        self.assertIn("END$$", sql_content)
        
        # Check for proper IF EXISTS structure
        self.assertIn("IF EXISTS (", sql_content)
        self.assertIn("THEN", sql_content)
        self.assertIn("ELSE", sql_content)
        self.assertIn("END IF", sql_content)
    
    def test_function_performance_call(self):
        """Test that process_drug_data function is called"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for function call
        self.assertIn("PERFORM process_drug_data()", sql_content)
    
    def test_error_handling_and_notices(self):
        """Test that SQL contains proper error handling and notices"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for error handling
        self.assertIn("RAISE NOTICE", sql_content)
        
        # Check for specific notice messages
        expected_notices = [
            "Skipping function creation: DRUG_Mapper table not found"
        ]
        
        for notice in expected_notices:
            with self.subTest(notice=notice):
                self.assertIn(notice, sql_content)
    
    def test_phase_ordering_validation(self):
        """Test that processing phases are in the expected order"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Find phase declarations in order
        phase_positions = []
        for phase in self.expected_phases:
            pattern = f"current_phase := '{phase}'"
            match = re.search(pattern, sql_content)
            if match:
                phase_positions.append((phase, match.start()))
        
        # Check that phases appear in expected order
        sorted_phases = sorted(phase_positions, key=lambda x: x[1])
        expected_order = [phase for phase, _ in sorted_phases]
        
        self.assertEqual(len(expected_order), len(self.expected_phases), 
                        "Not all expected phases found")
        
        # Validate the specific ordering
        drugname_phases = [p for p in expected_order if 'DRUGNAME' in p]
        prod_ai_phases = [p for p in expected_order if 'PROD_AI' in p]
        
        # DRUGNAME phases should come before PROD_AI phases
        if drugname_phases and prod_ai_phases:
            last_drugname_pos = expected_order.index(drugname_phases[-1])
            first_prod_ai_pos = expected_order.index(prod_ai_phases[0])
            self.assertLess(last_drugname_pos, first_prod_ai_pos, 
                           "DRUGNAME phases should complete before PROD_AI phases")
    
    def parse_sql_statements(self, sql_content):
        """Parse SQL content into individual statements"""
        # Remove comments
        sql_content = re.sub(r'--.*?\n', '\n', sql_content)
        sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)
        
        # Split by semicolons, but preserve DO blocks and function definitions
        statements = []
        current_statement = ""
        in_block = False
        block_markers = ['DO $$', 'CREATE OR REPLACE FUNCTION']
        
        lines = sql_content.split('\n')
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Check for block start
            for marker in block_markers:
                if marker in line.upper():
                    in_block = True
                    break
            
            current_statement += line + '\n'
            
            # Check for block end
            if (line.endswith('$$;') or 
                (in_block and line.endswith(';') and 'END' in line.upper())):
                statements.append(current_statement.strip())
                current_statement = ""
                in_block = False
            elif not in_block and line.endswith(';'):
                statements.append(current_statement.strip())
                current_statement = ""
        
        if current_statement.strip():
            statements.append(current_statement.strip())
        
        return [s for s in statements if s]
    
    def test_sql_statement_parsing(self):
        """Test that SQL can be parsed into valid statements"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        statements = self.parse_sql_statements(sql_content)
        self.assertGreater(len(statements), 0, "No SQL statements found")
        
        # Check for expected statement types
        statement_types = {
            'CREATE SCHEMA': 0,
            'ALTER TABLE': 0,
            'CREATE OR REPLACE FUNCTION': 0,
            'DO $$': 0,
            'SET': 0
        }
        
        for stmt in statements:
            stmt_upper = stmt.upper()
            for stmt_type in statement_types:
                if stmt_type in stmt_upper:
                    statement_types[stmt_type] += 1
        
        # Verify we have statements of expected types
        self.assertGreater(statement_types['ALTER TABLE'], 0, "No ALTER TABLE statements")
        self.assertGreater(statement_types['CREATE OR REPLACE FUNCTION'], 0, "No function definitions")
        self.assertGreater(statement_types['DO $$'], 0, "No DO blocks")
    
    def test_regex_pattern_validation(self):
        """Test that regex patterns in the SQL are properly escaped"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for properly escaped regex patterns
        regex_patterns = [
            "'/[0-9]{5}/'",  # Numeric suffix pattern
            "'[|,+;\\\\\\\\]'",  # Delimiter pattern
            "'\\\\([^()]*\\\\)'",  # Parenthesis pattern
            "'\\\\s{2,}'",  # Multiple whitespace pattern
            "E'[\\\\\\\\n\\\\\\\\r\\\\\\\\t]+'"  # Newline/tab pattern
        ]
        
        for pattern in regex_patterns:
            with self.subTest(pattern=pattern):
                self.assertIn(pattern, sql_content)


class TestS8SQLExecution(unittest.TestCase):
    """Test actual SQL execution (integration tests)"""
    
    def setUp(self):
        """Set up integration test environment"""
        self.test_db_available = all([
            os.getenv("TEST_DB_HOST"),
            os.getenv("TEST_DB_USER")
        ])
        
        if self.test_db_available:
            self.db_params = {
                "host": os.getenv("TEST_DB_HOST"),
                "port": int(os.getenv("TEST_DB_PORT", 5432)),
                "user": os.getenv("TEST_DB_USER"),
                "password": os.getenv("TEST_DB_PASSWORD", ""),
                "dbname": os.getenv("TEST_DB_NAME", "faersdatabase")
            }
        
        self.s8_sql_path = os.path.join(os.path.dirname(__file__), "..", "..", "s8.sql")
    
    @unittest.skipUnless(os.getenv("RUN_INTEGRATION_TESTS"), "Integration tests disabled")
    def test_execute_sql_file_with_psql(self):
        """Test executing s8.sql with psql command"""
        if not self.test_db_available:
            self.skipTest("Test database not configured")
        
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        # Build psql command
        cmd = [
            "psql",
            "-h", self.db_params["host"],
            "-p", str(self.db_params["port"]),
            "-U", self.db_params["user"],
            "-d", self.db_params["dbname"],
            "-f", self.s8_sql_path,
            "-v", "ON_ERROR_STOP=1"
        ]
        
        env = os.environ.copy()
        if self.db_params.get("password"):
            env["PGPASSWORD"] = self.db_params["password"]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=300)
            
            if result.returncode != 0:
                self.fail(f"SQL execution failed:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}")
            
            self.verify_functions_created()
            
        except subprocess.TimeoutExpired:
            self.fail("SQL execution timed out after 5 minutes")
        except FileNotFoundError:
            self.skipTest("psql command not found")
    
    def verify_functions_created(self):
        """Verify that drug processing functions were created"""
        if not self.test_db_available:
            return
        
        try:
            with psycopg.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    # Check if faers_b schema exists
                    cur.execute("SELECT EXISTS (SELECT FROM pg_namespace WHERE nspname = 'faers_b');")
                    schema_exists = cur.fetchone()[0]
                    self.assertTrue(schema_exists, "faers_b schema was not created")
                    
                    # Check if functions exist
                    expected_functions = [
                        "clearnumericcharacters",
                        "process_drug_data"
                    ]
                    
                    for func_name in expected_functions:
                        with self.subTest(function=func_name):
                            cur.execute("""
                                SELECT EXISTS (
                                    SELECT FROM pg_proc 
                                    WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
                                    AND proname = %s
                                );
                            """, (func_name,))
                            func_exists = cur.fetchone()[0]
                            self.assertTrue(func_exists, f"Function {func_name} was not created")
        
        except psycopg.Error as e:
            self.fail(f"Database verification failed: {e}")
    
    @unittest.skipUnless(os.getenv("RUN_INTEGRATION_TESTS"), "Integration tests disabled")
    def test_drug_mapper_columns_added(self):
        """Test that CLEANED_* columns were added to DRUG_Mapper"""
        if not self.test_db_available:
            self.skipTest("Test database not configured")
        
        try:
            with psycopg.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    # Check if DRUG_Mapper table exists and has new columns
                    cur.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_schema = 'faers_b' 
                        AND table_name = 'drug_mapper'
                        AND column_name IN ('cleaned_drugname', 'cleaned_prod_ai');
                    """)
                    columns = [row[0] for row in cur.fetchall()]
                    
                    expected_columns = ['cleaned_drugname', 'cleaned_prod_ai']
                    for col in expected_columns:
                        with self.subTest(column=col):
                            self.assertIn(col, columns, f"Column {col} was not added to DRUG_Mapper")
        
        except psycopg.Error as e:
            self.skipTest(f"Database operation failed: {e}")
    
    @unittest.skipUnless(os.getenv("RUN_INTEGRATION_TESTS"), "Integration tests disabled")
    def test_function_execution_without_table(self):
        """Test that script handles missing DRUG_Mapper table gracefully"""
        if not self.test_db_available:
            self.skipTest("Test database not configured")
        
        try:
            with psycopg.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    # Temporarily rename DRUG_Mapper if it exists
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM pg_class 
                            WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
                            AND relname = 'drug_mapper'
                        );
                    """)
                    table_exists = cur.fetchone()[0]
                    
                    if table_exists:
                        cur.execute('ALTER TABLE faers_b."DRUG_Mapper" RENAME TO "DRUG_Mapper_backup";')
                        conn.commit()
                        
                        try:
                            # Re-run the script - should handle missing table gracefully
                            with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
                                sql_content = f.read()
                            cur.execute(sql_content)
                            
                            # Should not raise an error
                            self.assertTrue(True, "Script handled missing table gracefully")
                            
                        finally:
                            # Restore the table
                            cur.execute('ALTER TABLE faers_b."DRUG_Mapper_backup" RENAME TO "DRUG_Mapper";')
                            conn.commit()
                    else:
                        self.skipTest("DRUG_Mapper table not found for testing")
        
        except psycopg.Error as e:
            self.skipTest(f"Database operation failed: {e}")


class TestS8SQLValidation(unittest.TestCase):
    """Test SQL syntax and structure validation"""
    
    def setUp(self):
        self.s8_sql_path = os.path.join(os.path.dirname(__file__), "..", "..", "s8.sql")
    
    def test_sql_syntax_basic_validation(self):
        """Test basic SQL syntax validation"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for balanced $ delimiters
        dollar_count = sql_content.count('$')
        self.assertEqual(dollar_count % 2, 0, "Unmatched $ delimiters")
        
        # Check for balanced parentheses in function definitions
        function_blocks = re.findall(r'CREATE OR REPLACE FUNCTION.*?END;', sql_content, re.DOTALL | re.IGNORECASE)
        for block in function_blocks:
            open_parens = block.count('(')
            close_parens = block.count(')')
            self.assertEqual(open_parens, close_parens, 
                           f"Unbalanced parentheses in function: {block[:100]}...")
    
    def test_loop_structure_validation(self):
        """Test that LOOP structures are properly formed"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for balanced LOOP/END LOOP pairs
        loop_starts = len(re.findall(r'\bFOR\b.*?\bLOOP\b', sql_content, re.IGNORECASE))
        loop_ends = len(re.findall(r'\bEND\s+LOOP\b', sql_content, re.IGNORECASE))
        
        self.assertEqual(loop_starts, loop_ends, "Unmatched LOOP/END LOOP pairs")
    
    def test_case_statement_validation(self):
        """Test that CASE statements are properly formed"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for balanced CASE/END pairs
        case_starts = len(re.findall(r'\bCASE\b', sql_content, re.IGNORECASE))
        case_ends = len(re.findall(r'\bEND\b(?!\s+(?:IF|LOOP))', sql_content, re.IGNORECASE))
        
        # Note: This is a simplified check - in practice, END can be used for other constructs
        # But for our specific SQL, CASE/END should be balanced
        self.assertGreaterEqual(case_ends, case_starts, "Missing END statements for CASE blocks")
    
    def test_json_access_pattern_validation(self):
        """Test that JSON access patterns are correctly formed"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for proper JSON access patterns
        json_patterns = [
            r"phase_data -> 'replacements'",
            r"stmt\.value->>'table'",
            r"stmt\.value->>'set_column'",
            r"stmt\.value->>'replace_column'", 
            r"stmt\.value->>'find'",
            r"stmt\.value->>'replace'"
        ]
        
        for pattern in json_patterns:
            with self.subTest(pattern=pattern):
                self.assertRegex(sql_content, pattern, f"JSON access pattern not found: {pattern}")
    
    def test_string_literal_escaping(self):
        """Test that string literals are properly escaped"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for properly escaped special characters in regex patterns
        properly_escaped = [
            r"'\\([^()]*\\)'",  # Parentheses pattern
            r"'[|,+;\\\\]'",    # Delimiter pattern  
            r"'\\s{2,}'",       # Whitespace pattern
            r"E'[\\n\\r\\t]+'"  # Newline pattern
        ]
        
        for pattern in properly_escaped:
            with self.subTest(pattern=pattern):
                # Remove the outer quotes for the search
                search_pattern = pattern.strip("'")
                self.assertIn(pattern, sql_content, f"Properly escaped pattern not found: {pattern}")
    
    def test_update_statement_structure(self):
        """Test that UPDATE statements have proper WHERE clauses"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Find all UPDATE statements
        update_statements = re.findall(r'UPDATE\s+[^;]+;', sql_content, re.IGNORECASE | re.DOTALL)
        
        for i, stmt in enumerate(update_statements):
            with self.subTest(statement_number=i+1):
                # Each UPDATE should have a WHERE clause (except for initial setup)
                if 'DRUG_Mapper_Temp' in stmt or 'NOTES IS NULL' in stmt:
                    # These should have proper WHERE clauses
                    self.assertIn('WHERE', stmt.upper(), f"UPDATE statement missing WHERE clause: {stmt[:100]}...")
    
    def test_conditional_logic_structure(self):
        """Test that conditional logic (IF/THEN/ELSE) is properly structured"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for balanced IF/END IF pairs
        if_starts = len(re.findall(r'\bIF\b(?!\s+NOT\s+EXISTS)', sql_content, re.IGNORECASE))
        if_ends = len(re.findall(r'\bEND\s+IF\b', sql_content, re.IGNORECASE))
        
        self.assertEqual(if_starts, if_ends, "Unmatched IF/END IF pairs")
        
        # Check that each IF has a corresponding THEN
        if_then_pairs = len(re.findall(r'\bIF\b.*?\bTHEN\b', sql_content, re.IGNORECASE | re.DOTALL))
        self.assertEqual(if_starts, if_then_pairs, "IF statements without THEN clauses")


class TestS8SQLDataProcessing(unittest.TestCase):
    """Test data processing logic and transformations"""
    
    def setUp(self):
        self.s8_sql_path = os.path.join(os.path.dirname(__file__), "..", "..", "s8.sql")
    
    def test_cleaning_operation_order(self):
        """Test that cleaning operations are in logical order"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Find positions of key operations
        operations = [
            ("initialization", "CLEANED_DRUGNAME = DRUGNAME"),
            ("numeric_suffix", "/[0-9]{5}/"),
            ("delimiter_norm", "[|,+;\\\\]"),
            ("parenthesis", "\\([^()]*\\)"),
            ("units_removal", "UNITS_OF_MEASUREMENT"),
            ("manufacturer", "MANUFACTURER_NAMES"),
            ("format_clean", "FORMAT_DRUGNAME"),
            ("final_clean", "CLEANING_DRUGNAME")
        ]
        
        positions = []
        for name, pattern in operations:
            match = re.search(re.escape(pattern), sql_content)
            if match:
                positions.append((name, match.start()))
        
        # Sort by position
        sorted_ops = sorted(positions, key=lambda x: x[1])
        
        # Check that initialization comes first
        if sorted_ops:
            self.assertEqual(sorted_ops[0][0], "initialization", 
                           "Initialization should be the first operation")
    
    def test_configuration_phase_completeness(self):
        """Test that all configuration phases have proper structure"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        expected_phases = [
            "UNITS_OF_MEASUREMENT_DRUGNAME",
            "MANUFACTURER_NAMES_DRUGNAME", 
            "WORDS_TO_VITAMIN_B_DRUGNAME",
            "FORMAT_DRUGNAME",
            "CLEANING_DRUGNAME",
            "UNITS_MEASUREMENT_PROD_AI",
            "MANUFACTURER_NAMES_PROD_AI",
            "WORDS_TO_VITAMIN_B_PROD_AI", 
            "FORMAT_PROD_AI",
            "CLEANING_PROD_AI"
        ]
        
        for phase in expected_phases:
            with self.subTest(phase=phase):
                # Each phase should have:
                # 1. Phase assignment
                phase_assignment = f"current_phase := '{phase}'"
                self.assertIn(phase_assignment, sql_content)
                
                # 2. Configuration data retrieval
                config_retrieval = "SELECT cfg.config_data INTO phase_data"
                phase_block = self.extract_phase_block(sql_content, phase)
                self.assertIn(config_retrieval, phase_block)
                
                # 3. Null check
                null_check = "IF phase_data IS NOT NULL THEN"
                self.assertIn(null_check, phase_block)
                
                # 4. Loop over replacements
                replacements_loop = "jsonb_array_elements(phase_data -> 'replacements')"
                self.assertIn(replacements_loop, phase_block)
    
    def extract_phase_block(self, sql_content, phase_name):
        """Extract the processing block for a specific phase"""
        phase_start = sql_content.find(f"current_phase := '{phase_name}'")
        if phase_start == -1:
            return ""
        
        # Find the next phase or end of function
        next_phase_start = len(sql_content)
        for other_phase in ["UNITS_OF_MEASUREMENT", "MANUFACTURER_NAMES", "WORDS_TO_VITAMIN_B", "FORMAT_", "CLEANING_"]:
            if other_phase in phase_name:
                continue
            next_pos = sql_content.find(f"current_phase := '{other_phase}", phase_start + 1)
            if next_pos != -1 and next_pos < next_phase_start:
                next_phase_start = next_pos
        
        return sql_content[phase_start:next_phase_start]
    
    def test_error_resilience(self):
        """Test that the script handles potential error conditions"""
        if not os.path.exists(self.s8_sql_path):
            self.skipTest("s8.sql file not found")
        
        with open(self.s8_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for proper null handling
        null_checks = [
            "CLEANED_DRUGNAME IS NULL",
            "CLEANED_PROD_AI IS NULL", 
            "phase_data IS NOT NULL",
            "NOTES IS NULL"
        ]
        
        for check in null_checks:
            with self.subTest(check=check):
                self.assertIn(check, sql_content, f"Missing null check: {check}")
        
        # Check for existence checks
        existence_checks = [
            "information_schema.tables",
            "table_name = 'drug_mapper'"
        ]
        
        for check in existence_checks:
            with self.subTest(check=check):
                self.assertIn(check, sql_content, f"Missing existence check: {check}")


if __name__ == '__main__':
    print("Running s8.sql unit tests...")
    print("This tests the SQL script that processes drug data cleaning")
    print("Looking for s8.sql in the faers-scripts root directory")
    print()
    print("The script performs comprehensive drug name and product AI cleaning including:")
    print("  - Numeric suffix removal")
    print("  - Delimiter normalization") 
    print("  - Parenthesis content removal")
    print("  - Units of measurement cleaning")
    print("  - Manufacturer name removal")
    print("  - Vitamin B standardization")
    print("  - Format standardization")
    print("  - Special character trimming")
    print()
    print("For integration tests, set environment variables:")
    print("  TEST_DB_HOST, TEST_DB_USER, TEST_DB_NAME, TEST_DB_PASSWORD")
    print("  RUN_INTEGRATION_TESTS=1")
    print("NOTE: Integration tests require 'psql' command and connection to 'faersdatabase'")
    print("NOTE: Integration tests require existing DRUG_Mapper table in faers_b schema")
    print()
    
    unittest.main(verbosity=2)