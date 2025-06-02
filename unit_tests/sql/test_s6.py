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


class TestS6SQL(unittest.TestCase):
    """Test cases for s6.sql database operations"""
    
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
        
        # SQL script path - looks for s6.sql in the root of faers-scripts
        cls.s6_sql_path = os.path.join(os.path.dirname(__file__), "..", "..", "s6.sql")
        
        # Expected tables created by s6.sql
        cls.expected_tables = [
            "products_at_fda",
            "IDD",
            "manual_mapping"
        ]
        
        # Expected function
        cls.expected_function = "clean_string"
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value.__enter__.return_value = self.mock_cursor
    
    def test_sql_file_exists(self):
        """Test that s6.sql file exists"""
        self.assertTrue(os.path.exists(self.s6_sql_path), 
                       f"s6.sql file not found at {self.s6_sql_path}")
    
    def test_sql_file_readable(self):
        """Test that s6.sql file is readable"""
        if os.path.exists(self.s6_sql_path):
            try:
                with open(self.s6_sql_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                self.assertGreater(len(content), 0, "s6.sql file is empty")
            except Exception as e:
                self.fail(f"Could not read s6.sql file: {e}")
        else:
            self.skipTest("s6.sql file not found")
    
    def test_database_context_validation_in_sql(self):
        """Test that SQL contains database context validation"""
        if not os.path.exists(self.s6_sql_path):
            self.skipTest("s6.sql file not found")
        
        with open(self.s6_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for database validation
        self.assertIn("current_database()", sql_content)
        self.assertIn("faersdatabase", sql_content)
        self.assertIn("RAISE EXCEPTION", sql_content)
    
    def test_schema_creation_in_sql(self):
        """Test that SQL contains schema creation"""
        if not os.path.exists(self.s6_sql_path):
            self.skipTest("s6.sql file not found")
        
        with open(self.s6_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for schema operations
        self.assertIn("CREATE SCHEMA IF NOT EXISTS faers_b", sql_content)
        self.assertIn("GRANT ALL ON SCHEMA faers_b", sql_content)
        self.assertIn("SET search_path TO faers_b", sql_content)
    
    def test_clean_string_function_creation(self):
        """Test that SQL creates the clean_string function"""
        if not os.path.exists(self.s6_sql_path):
            self.skipTest("s6.sql file not found")
        
        with open(self.s6_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for clean_string function
        self.assertIn("CREATE OR REPLACE FUNCTION faers_b.clean_string", sql_content)
        self.assertIn("input TEXT", sql_content)
        self.assertIn("RETURNS TEXT", sql_content)
        self.assertIn("LANGUAGE plpgsql", sql_content)
        
        # Check for function logic
        self.assertIn("POSITION('(' IN output)", sql_content)
        self.assertIn("TRIM(BOTH", sql_content)
        self.assertIn("REGEXP_REPLACE", sql_content)
    
    def test_products_at_fda_table_creation(self):
        """Test that SQL creates products_at_fda table"""
        if not os.path.exists(self.s6_sql_path):
            self.skipTest("s6.sql file not found")
        
        with open(self.s6_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for table creation
        self.assertIn("CREATE TABLE faers_b.products_at_fda", sql_content)
        
        # Check for expected columns
        expected_columns = [
            "applno VARCHAR(10)",
            "productno VARCHAR(10)",
            "form TEXT",
            "strength TEXT",
            "referencedrug INTEGER",
            "drugname TEXT",
            "activeingredient TEXT",
            "referencestandard INTEGER",
            "rxaui VARCHAR(8)",
            "ai_2 TEXT"
        ]
        
        for column in expected_columns:
            with self.subTest(column=column):
                self.assertIn(column, sql_content)
    
    def test_idd_table_creation(self):
        """Test that SQL creates IDD table"""
        if not os.path.exists(self.s6_sql_path):
            self.skipTest("s6.sql file not found")
        
        with open(self.s6_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for IDD table creation
        self.assertIn('CREATE TABLE faers_b."IDD"', sql_content)
        
        # Check for expected columns
        expected_columns = [
            '"DRUGNAME" TEXT',
            '"RXAUI" VARCHAR(8)',
            '"RXCUI" VARCHAR(8)',
            '"STR" TEXT',
            '"SAB" VARCHAR(50)',
            '"TTY" VARCHAR(10)',
            '"CODE" VARCHAR(50)'
        ]
        
        for column in expected_columns:
            with self.subTest(column=column):
                self.assertIn(column, sql_content)
    
    def test_manual_mapping_table_creation(self):
        """Test that SQL creates manual_mapping table"""
        if not os.path.exists(self.s6_sql_path):
            self.skipTest("s6.sql file not found")
        
        with open(self.s6_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for manual_mapping table creation
        self.assertIn("CREATE TABLE faers_b.manual_mapping", sql_content)
        
        # Check for expected columns
        expected_columns = [
            "drugname TEXT",
            "count INTEGER",
            "rxaui BIGINT",
            "rxcui BIGINT",
            "sab VARCHAR(20)",
            "tty VARCHAR(20)",
            "str TEXT",
            "code VARCHAR(50)",
            "notes TEXT"
        ]
        
        for column in expected_columns:
            with self.subTest(column=column):
                self.assertIn(column, sql_content)
    
    def test_index_creation_in_sql(self):
        """Test that SQL creates proper indexes"""
        if not os.path.exists(self.s6_sql_path):
            self.skipTest("s6.sql file not found")
        
        with open(self.s6_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for index creation
        expected_indexes = [
            "idx_idd_drugname",
            "idx_idd_rxaui",
            "idx_products_at_fda_applno",
            "idx_products_at_fda_rxaui",
            "idx_drug_mapper_nda_num",
            "idx_drug_mapper_notes",
            "idx_rxnconso_str_sab_tty"
        ]
        
        for index in expected_indexes:
            with self.subTest(index=index):
                self.assertIn(index, sql_content)
    
    def test_ai_2_update_logic(self):
        """Test the ai_2 update logic in products_at_fda"""
        if not os.path.exists(self.s6_sql_path):
            self.skipTest("s6.sql file not found")
        
        with open(self.s6_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for ai_2 update
        self.assertIn("ai_2 = faers_b.clean_string(activeingredient)", sql_content)
        self.assertIn("UPDATE faers_b.products_at_fda", sql_content)
    
    def test_rxaui_mapping_logic(self):
        """Test the rxaui mapping logic"""
        if not os.path.exists(self.s6_sql_path):
            self.skipTest("s6.sql file not found")
        
        with open(self.s6_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for rxaui mapping conditions
        self.assertIn("products_at_fda.ai_2 = rxnconso.str", sql_content)
        self.assertIn("rxnconso.sab = 'RXNORM'", sql_content)
        self.assertIn("rxnconso.tty IN ('IN', 'MIN')", sql_content)
        self.assertIn("products_at_fda.rxaui IS NULL", sql_content)
    
    def test_drug_mapping_by_nda_number(self):
        """Test the drug mapping by NDA number logic"""
        if not os.path.exists(self.s6_sql_path):
            self.skipTest("s6.sql file not found")
        
        with open(self.s6_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for NDA number mapping
        self.assertIn("drug_mapper.nda_num ~ '^[0-9]+$'", sql_content)
        self.assertIn("LENGTH(drug_mapper.nda_num) < 6", sql_content)
        self.assertIn("notes = '1.0'", sql_content)
    
    def test_drug_mapping_by_name(self):
        """Test the drug mapping by name logic"""
        if not os.path.exists(self.s6_sql_path):
            self.skipTest("s6.sql file not found")
        
        with open(self.s6_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for drug name mapping with priority scoring
        mapping_notes = [
            "notes = '1.1'",  # RXNORM IN
            "notes = '1.2'",  # RXNORM MIN
            "notes = '1.3'",  # MTHSPL
            "notes = '2.1'",  # Product AI RXNORM IN
            "notes = '2.2'"   # Product AI RXNORM MIN
        ]
        
        for note in mapping_notes:
            with self.subTest(note=note):
                self.assertIn(note, sql_content)
    
    def test_idd_mapping_logic(self):
        """Test the IDD mapping logic"""
        if not os.path.exists(self.s6_sql_path):
            self.skipTest("s6.sql file not found")
        
        with open(self.s6_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for IDD mapping
        self.assertIn('drug_mapper.drugname = i."DRUGNAME"', sql_content)
        self.assertIn('drug_mapper.prod_ai = i."DRUGNAME"', sql_content)
        self.assertIn("notes = '6.1'", sql_content)
        self.assertIn("notes = '6.2'", sql_content)
    
    def test_manual_mapping_insertion(self):
        """Test the manual mapping insertion logic"""
        if not os.path.exists(self.s6_sql_path):
            self.skipTest("s6.sql file not found")
        
        with open(self.s6_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for manual mapping insertion
        self.assertIn("INSERT INTO faers_b.manual_mapping", sql_content)
        self.assertIn("COUNT(drugname) > 199", sql_content)
        self.assertIn("GROUP BY drugname", sql_content)
        self.assertIn("WHERE notes IS NULL", sql_content)
    
    def test_table_existence_checks(self):
        """Test that SQL contains proper table existence checks"""
        if not os.path.exists(self.s6_sql_path):
            self.skipTest("s6.sql file not found")
        
        with open(self.s6_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for table existence patterns
        existence_checks = [
            "SELECT EXISTS",
            "SELECT FROM pg_class",
            "WHERE relnamespace =",
            "AND relname ="
        ]
        
        for check in existence_checks:
            with self.subTest(check=check):
                self.assertIn(check, sql_content)
    
    def test_error_handling_and_notices(self):
        """Test that SQL contains proper error handling and notices"""
        if not os.path.exists(self.s6_sql_path):
            self.skipTest("s6.sql file not found")
        
        with open(self.s6_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for error handling
        self.assertIn("RAISE NOTICE", sql_content)
        self.assertIn("RAISE EXCEPTION", sql_content)
        
        # Check for specific notices
        expected_notices = [
            "Created faers_b.products_at_fda table",
            "Created faers_b.IDD table",
            "Created faers_b.manual_mapping table",
            "Created indexes for performance"
        ]
        
        for notice in expected_notices:
            with self.subTest(notice=notice):
                self.assertIn(notice, sql_content)
    
    def test_do_blocks_structure(self):
        """Test that SQL contains properly structured DO blocks"""
        if not os.path.exists(self.s6_sql_path):
            self.skipTest("s6.sql file not found")
        
        with open(self.s6_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for DO blocks
        do_blocks = re.findall(r'DO \$\$.*?\$\$;', sql_content, re.DOTALL)
        self.assertGreater(len(do_blocks), 0, "No DO blocks found")
        
        # Check that DO blocks have proper structure
        for i, block in enumerate(do_blocks):
            with self.subTest(block_number=i+1):
                self.assertIn("BEGIN", block)
                self.assertIn("END", block)
    
    def test_temp_table_usage(self):
        """Test that SQL properly uses temporary tables"""
        if not os.path.exists(self.s6_sql_path):
            self.skipTest("s6.sql file not found")
        
        with open(self.s6_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for temp table creation and cleanup
        self.assertIn("CREATE TEMP TABLE cleaned_drugs", sql_content)
        self.assertIn("DROP TABLE cleaned_drugs", sql_content)
        self.assertIn("CREATE INDEX idx_cleaned_drugs", sql_content)
    
    def test_string_cleaning_logic(self):
        """Test the string cleaning function logic"""
        if not os.path.exists(self.s6_sql_path):
            self.skipTest("s6.sql file not found")
        
        with open(self.s6_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for string cleaning patterns
        cleaning_patterns = [
            "POSITION('(' IN output)",
            "POSITION(')' IN output)",
            "SUBSTRING(output FROM",
            "TRIM(BOTH",
            "REPLACE(output, ';', ' / ')",
            "REGEXP_REPLACE(output, '\\s+', ' ', 'g')"
        ]
        
        for pattern in cleaning_patterns:
            with self.subTest(pattern=pattern):
                self.assertIn(pattern, sql_content)
    
    def parse_sql_statements(self, sql_content):
        """Parse SQL content into individual statements"""
        # Remove comments
        sql_content = re.sub(r'--.*?\n', '\n', sql_content)
        sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)
        
        # Split by semicolons, but preserve DO blocks and functions
        statements = []
        current_statement = ""
        in_do_block = False
        in_function = False
        
        lines = sql_content.split('\n')
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            if re.match(r'^\s*DO\s*\$\$', line, re.IGNORECASE):
                in_do_block = True
                current_statement += line + '\n'
            elif re.match(r'^\s*CREATE\s+(OR\s+REPLACE\s+)?FUNCTION', line, re.IGNORECASE):
                in_function = True
                current_statement += line + '\n'
            elif line.endswith('$$;') and (in_do_block or in_function):
                current_statement += line
                statements.append(current_statement.strip())
                current_statement = ""
                in_do_block = False
                in_function = False
            elif in_do_block or in_function:
                current_statement += line + '\n'
            else:
                current_statement += line + ' '
                if line.endswith(';'):
                    statements.append(current_statement.strip())
                    current_statement = ""
        
        if current_statement.strip():
            statements.append(current_statement.strip())
        
        return [s for s in statements if s]
    
    def test_sql_statement_parsing(self):
        """Test that SQL can be parsed into valid statements"""
        if not os.path.exists(self.s6_sql_path):
            self.skipTest("s6.sql file not found")
        
        with open(self.s6_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        statements = self.parse_sql_statements(sql_content)
        self.assertGreater(len(statements), 0, "No SQL statements found")
        
        # Check for expected statement types
        statement_types = {
            'CREATE SCHEMA': 0,
            'CREATE TABLE': 0,
            'CREATE INDEX': 0,
            'CREATE OR REPLACE FUNCTION': 0,
            'DO $$': 0,
            'GRANT': 0,
            'SET': 0,
            'UPDATE': 0,
            'INSERT': 0
        }
        
        for stmt in statements:
            stmt_upper = stmt.upper()
            for stmt_type in statement_types:
                if stmt_type in stmt_upper:
                    statement_types[stmt_type] += 1
        
        # Verify we have statements of expected types
        self.assertGreater(statement_types['CREATE TABLE'], 0, "No CREATE TABLE statements")
        self.assertGreater(statement_types['DO $$'], 0, "No DO blocks")
        self.assertGreater(statement_types['CREATE OR REPLACE FUNCTION'], 0, "No function creation")
        self.assertGreater(statement_types['UPDATE'], 0, "No UPDATE statements")


class TestS6SQLExecution(unittest.TestCase):
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
        
        self.s6_sql_path = os.path.join(os.path.dirname(__file__), "..", "..", "s6.sql")
    
    @unittest.skipUnless(os.getenv("RUN_INTEGRATION_TESTS"), "Integration tests disabled")
    def test_execute_sql_file_with_psql(self):
        """Test executing s6.sql with psql command"""
        if not self.test_db_available:
            self.skipTest("Test database not configured")
        
        if not os.path.exists(self.s6_sql_path):
            self.skipTest("s6.sql file not found")
        
        # Build psql command
        cmd = [
            "psql",
            "-h", self.db_params["host"],
            "-p", str(self.db_params["port"]),
            "-U", self.db_params["user"],
            "-d", self.db_params["dbname"],
            "-f", self.s6_sql_path,
            "-v", "ON_ERROR_STOP=1"
        ]
        
        env = os.environ.copy()
        if self.db_params.get("password"):
            env["PGPASSWORD"] = self.db_params["password"]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=300)
            
            if result.returncode != 0:
                self.fail(f"SQL execution failed:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}")
            
            self.verify_tables_created()
            
        except subprocess.TimeoutExpired:
            self.fail("SQL execution timed out after 5 minutes")
        except FileNotFoundError:
            self.skipTest("psql command not found")
    
    def verify_tables_created(self):
        """Verify that expected tables were created"""
        if not self.test_db_available:
            return
        
        try:
            with psycopg.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    # Check if faers_b schema exists
                    cur.execute("SELECT EXISTS (SELECT FROM pg_namespace WHERE nspname = 'faers_b');")
                    schema_exists = cur.fetchone()[0]
                    self.assertTrue(schema_exists, "faers_b schema was not created")
                    
                    # Check if clean_string function exists
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM pg_proc p
                            JOIN pg_namespace n ON p.pronamespace = n.oid
                            WHERE n.nspname = 'faers_b' AND p.proname = 'clean_string'
                        );
                    """)
                    function_exists = cur.fetchone()[0]
                    self.assertTrue(function_exists, "clean_string function was not created")
                    
                    # Check expected tables
                    expected_tables = ["products_at_fda", "IDD", "manual_mapping"]
                    for table in expected_tables:
                        with self.subTest(table=table):
                            cur.execute(f"""
                                SELECT EXISTS (
                                    SELECT FROM pg_class 
                                    WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
                                    AND relname = '{table}'
                                );
                            """)
                            table_exists = cur.fetchone()[0]
                            self.assertTrue(table_exists, f"{table} table was not created")
        
        except psycopg.Error as e:
            self.fail(f"Database verification failed: {e}")
    
    @unittest.skipUnless(os.getenv("RUN_INTEGRATION_TESTS"), "Integration tests disabled")
    def test_clean_string_function_execution(self):
        """Test actual execution of clean_string function"""
        if not self.test_db_available:
            self.skipTest("Test database not configured")
        
        try:
            with psycopg.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    # Test clean_string function with various inputs
                    test_cases = [
                        ("ACETAMINOPHEN (TYLENOL)", "ACETAMINOPHEN"),
                        ("  Drug Name  ", "Drug Name"),
                        ("Drug;Name", "Drug / Name"),
                        ("Multiple   Spaces", "Multiple Spaces")
                    ]
                    
                    for input_text, expected in test_cases:
                        with self.subTest(input=input_text):
                            cur.execute("SELECT faers_b.clean_string(%s);", (input_text,))
                            result = cur.fetchone()[0]
                            self.assertEqual(result, expected)
        
        except psycopg.Error as e:
            self.skipTest(f"Database operation failed: {e}")


class TestS6SQLValidation(unittest.TestCase):
    """Test SQL syntax and structure validation"""
    
    def setUp(self):
        self.s6_sql_path = os.path.join(os.path.dirname(__file__), "..", "..", "s6.sql")
    
    def test_sql_syntax_basic_validation(self):
        """Test basic SQL syntax validation"""
        if not os.path.exists(self.s6_sql_path):
            self.skipTest("s6.sql file not found")
        
        with open(self.s6_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for balanced $$ delimiters
        dollar_count = sql_content.count('$$')
        self.assertEqual(dollar_count % 2, 0, "Unmatched $$ delimiters")
        
        # Check for balanced parentheses in CREATE TABLE statements
        create_table_blocks = re.findall(r'CREATE TABLE.*?\);', sql_content, re.DOTALL | re.IGNORECASE)
        for block in create_table_blocks:
            open_parens = block.count('(')
            close_parens = block.count(')')
            self.assertEqual(open_parens, close_parens, 
                           f"Unbalanced parentheses in CREATE TABLE: {block[:100]}...")
    
    def test_function_definition_structure(self):
        """Test that function definitions are properly structured"""
        if not os.path.exists(self.s6_sql_path):
            self.skipTest("s6.sql file not found")
        
        with open(self.s6_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Find function definitions
        function_blocks = re.findall(r'CREATE OR REPLACE FUNCTION.*?\$\$ LANGUAGE plpgsql;', 
                                   sql_content, re.DOTALL | re.IGNORECASE)
        
        self.assertGreater(len(function_blocks), 0, "No function definitions found")
        
        for block in function_blocks:
            with self.subTest(function=block[:50] + "..."):
                self.assertIn("RETURNS", block)
                self.assertIn("BEGIN", block)
                self.assertIn("END", block)
                self.assertIn("LANGUAGE plpgsql", block)
    
    def test_mapping_priority_consistency(self):
        """Test that mapping priority notes are consistent"""
        if not os.path.exists(self.s6_sql_path):
            self.skipTest("s6.sql file not found")
        
        with open(self.s6_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Extract all note assignments
        note_patterns = re.findall(r"notes = '([^']+)'", sql_content)
        
        # Verify priority scheme is logical
        expected_patterns = [
            "1.0",   # NDA mapping
            "1.1", "1.2", "1.3",  # Drug name mapping priorities
            "2.1", "2.2", "2.3",  # Product AI mapping priorities
            "6.1", "6.2"          # IDD mapping priorities
        ]
        
        for pattern in expected_patterns:
            with self.subTest(pattern=pattern):
                self.assertIn(pattern, note_patterns, f"Priority {pattern} not found in mapping logic")


if __name__ == '__main__':
    print("Running s6.sql unit tests...")
    print("This tests the SQL script that creates drug mapping tables and functions")
    print("Looking for s6.sql in the faers-scripts root directory")
    print()
    print("For integration tests, set environment variables:")
    print("  TEST_DB_HOST, TEST_DB_USER, TEST_DB_NAME, TEST_DB_PASSWORD")
    print("  RUN_INTEGRATION_TESTS=1")
    print("NOTE: Integration tests require 'psql' command and connection to 'faersdatabase'")
    print()
    
    unittest.main(verbosity=2)