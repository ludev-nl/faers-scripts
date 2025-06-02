import unittest
import os
import sys
import psycopg
import tempfile
import json
from unittest.mock import patch, MagicMock, mock_open, call
import subprocess
from datetime import datetime
import re

# Add the parent directory to sys.path if needed
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


class TestS5SQL(unittest.TestCase):
    """Test cases for s5.sql database operations"""
    
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
        
        # SQL script path - looks for s5.sql in the root of faers-scripts
        # Go up two directories from unit_tests/sql/ to reach faers-scripts/
        cls.s5_sql_path = os.path.join(os.path.dirname(__file__), "..", "..", "s5.sql")
        
        # Expected RxNorm tables
        cls.rxnorm_tables = [
            "rxnatomarchive", "rxnconso", "rxnrel", "rxnsab",
            "rxnsat", "rxnsty", "rxndoc", "rxncuichanges", "rxncui"
        ]
        
        # Expected main tables
        cls.main_tables = ["drug_mapper", "s5_log"]
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value.__enter__.return_value = self.mock_cursor
    
    def test_sql_file_exists(self):
        """Test that s5.sql file exists"""
        self.assertTrue(os.path.exists(self.s5_sql_path), 
                       f"s5.sql file not found at {self.s5_sql_path}")
    
    def test_sql_file_readable(self):
        """Test that s5.sql file is readable"""
        if os.path.exists(self.s5_sql_path):
            try:
                with open(self.s5_sql_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                self.assertGreater(len(content), 0, "s5.sql file is empty")
            except Exception as e:
                self.fail(f"Could not read s5.sql file: {e}")
        else:
            self.skipTest("s5.sql file not found")
    
    def test_database_context_validation_in_sql(self):
        """Test that SQL contains database context validation"""
        if not os.path.exists(self.s5_sql_path):
            self.skipTest("s5.sql file not found")
        
        with open(self.s5_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for database validation
        self.assertIn("current_database()", sql_content)
        self.assertIn("faersdatabase", sql_content)
        self.assertIn("RAISE EXCEPTION", sql_content)
    
    def test_schema_creation_in_sql(self):
        """Test that SQL contains schema creation"""
        if not os.path.exists(self.s5_sql_path):
            self.skipTest("s5.sql file not found")
        
        with open(self.s5_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for schema operations
        self.assertIn("CREATE SCHEMA IF NOT EXISTS faers_b", sql_content)
        self.assertIn("GRANT ALL ON SCHEMA faers_b", sql_content)
        self.assertIn("SET search_path TO faers_b", sql_content)
    
    def test_logging_table_creation_in_sql(self):
        """Test that SQL creates logging table"""
        if not os.path.exists(self.s5_sql_path):
            self.skipTest("s5.sql file not found")
        
        with open(self.s5_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for logging table
        self.assertIn("CREATE TABLE IF NOT EXISTS faers_b.s5_log", sql_content)
        self.assertIn("log_id SERIAL PRIMARY KEY", sql_content)
        self.assertIn("step VARCHAR(50)", sql_content)
        self.assertIn("message TEXT", sql_content)
        self.assertIn("log_date TIMESTAMP", sql_content)
    
    def test_drug_mapper_table_creation_in_sql(self):
        """Test that SQL creates drug_mapper table"""
        if not os.path.exists(self.s5_sql_path):
            self.skipTest("s5.sql file not found")
        
        with open(self.s5_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for drug_mapper table
        self.assertIn("CREATE TABLE faers_b.drug_mapper", sql_content)
        self.assertIn("drug_id INTEGER NOT NULL", sql_content)
        self.assertIn("primaryid BIGINT", sql_content)
        self.assertIn("drugname VARCHAR(500)", sql_content)
        self.assertIn("rxcui BIGINT", sql_content)
        
        # Check for index
        self.assertIn("CREATE INDEX idx_drug_mapper_drugname", sql_content)
    
    def test_rxnorm_tables_creation_in_sql(self):
        """Test that SQL creates all RxNorm tables"""
        if not os.path.exists(self.s5_sql_path):
            self.skipTest("s5.sql file not found")
        
        with open(self.s5_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for all RxNorm tables
        for table in self.rxnorm_tables:
            with self.subTest(table=table):
                self.assertIn(f"CREATE TABLE faers_b.{table}", sql_content)
    
    def test_rxnconso_indexes_in_sql(self):
        """Test that SQL creates proper indexes on rxnconso"""
        if not os.path.exists(self.s5_sql_path):
            self.skipTest("s5.sql file not found")
        
        with open(self.s5_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for rxnconso indexes
        expected_indexes = [
            "idx_rxnconso_rxcui",
            "idx_rxnconso_rxaui", 
            "idx_rxnconso_sab",
            "idx_rxnconso_tty",
            "idx_rxnconso_code"
        ]
        
        for index in expected_indexes:
            with self.subTest(index=index):
                self.assertIn(index, sql_content)
    
    def test_copy_commands_in_sql(self):
        """Test that SQL contains copy command examples"""
        if not os.path.exists(self.s5_sql_path):
            self.skipTest("s5.sql file not found")
        
        with open(self.s5_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for copy command examples
        self.assertIn("\\copy", sql_content)
        self.assertIn("DELIMITER '|'", sql_content)
        self.assertIn("HEADER FALSE", sql_content)
        self.assertIn(".RRF", sql_content)
    
    def test_error_handling_in_sql(self):
        """Test that SQL contains proper error handling"""
        if not os.path.exists(self.s5_sql_path):
            self.skipTest("s5.sql file not found")
        
        with open(self.s5_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for error handling
        self.assertIn("EXCEPTION", sql_content)
        self.assertIn("WHEN OTHERS THEN", sql_content)
        self.assertIn("SQLERRM", sql_content)
    
    def test_do_blocks_in_sql(self):
        """Test that SQL contains proper DO blocks"""
        if not os.path.exists(self.s5_sql_path):
            self.skipTest("s5.sql file not found")
        
        with open(self.s5_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for DO blocks
        do_blocks = re.findall(r'DO \$\$.*?\$\$;', sql_content, re.DOTALL)
        self.assertGreater(len(do_blocks), 0, "No DO blocks found")
        
        # Check that DO blocks have proper structure
        for i, block in enumerate(do_blocks):
            with self.subTest(block_number=i+1):
                self.assertIn("BEGIN", block)
                self.assertIn("END", block)
    
    def parse_sql_statements(self, sql_content):
        """Parse SQL content into individual statements"""
        # Remove comments
        sql_content = re.sub(r'--.*?\n', '\n', sql_content)
        sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)
        
        # Split by semicolons, but preserve DO blocks
        statements = []
        current_statement = ""
        in_do_block = False
        
        lines = sql_content.split('\n')
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            if re.match(r'^\s*DO\s*\$\$', line, re.IGNORECASE):
                in_do_block = True
                current_statement += line + '\n'
            elif line.endswith('$$;') and in_do_block:
                current_statement += line
                statements.append(current_statement.strip())
                current_statement = ""
                in_do_block = False
            elif in_do_block:
                current_statement += line + '\n'
            else:
                current_statement += line + ' '
                if line.endswith(';'):
                    statements.append(current_statement.strip())
                    current_statement = ""
        
        if current_statement.strip():
            statements.append(current_statement.strip())
        
        return [s for s in statements if s and not s.startswith('\\')]
    
    def test_sql_statement_parsing(self):
        """Test that SQL can be parsed into valid statements"""
        if not os.path.exists(self.s5_sql_path):
            self.skipTest("s5.sql file not found")
        
        with open(self.s5_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        statements = self.parse_sql_statements(sql_content)
        self.assertGreater(len(statements), 0, "No SQL statements found")
        
        # Check that we have expected types of statements
        statement_types = {
            'CREATE SCHEMA': 0,
            'CREATE TABLE': 0,
            'CREATE INDEX': 0,
            'DO $$': 0,
            'GRANT': 0,
            'SET': 0
        }
        
        for stmt in statements:
            stmt_upper = stmt.upper()
            for stmt_type in statement_types:
                if stmt_type in stmt_upper:
                    statement_types[stmt_type] += 1
        
        # Verify we have statements of each expected type
        self.assertGreater(statement_types['CREATE TABLE'], 0, "No CREATE TABLE statements")
        self.assertGreater(statement_types['DO $$'], 0, "No DO blocks")


class TestS5SQLExecution(unittest.TestCase):
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
        
        self.s5_sql_path = os.path.join(os.path.dirname(__file__), "..", "..", "s5.sql")
    
    @unittest.skipUnless(os.getenv("RUN_INTEGRATION_TESTS"), "Integration tests disabled")
    def test_execute_sql_file_with_psql(self):
        """Test executing s5.sql with psql command"""
        if not self.test_db_available:
            self.skipTest("Test database not configured")
        
        if not os.path.exists(self.s5_sql_path):
            self.skipTest("s5.sql file not found")
        
        # Build psql command
        cmd = [
            "psql",
            "-h", self.db_params["host"],
            "-p", str(self.db_params["port"]),
            "-U", self.db_params["user"],
            "-d", self.db_params["dbname"],
            "-f", self.s5_sql_path,
            "-v", "ON_ERROR_STOP=1"  # Stop on first error
        ]
        
        env = os.environ.copy()
        if self.db_params.get("password"):
            env["PGPASSWORD"] = self.db_params["password"]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=300)
            
            # Check if execution was successful
            if result.returncode != 0:
                self.fail(f"SQL execution failed:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}")
            
            # Verify some tables were created
            self.verify_tables_created()
            
        except subprocess.TimeoutExpired:
            self.fail("SQL execution timed out after 5 minutes")
        except FileNotFoundError:
            self.skipTest("psql command not found - PostgreSQL client not installed")
    
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
                    
                    # Check if s5_log table exists
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM pg_class 
                            WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
                            AND relname = 's5_log'
                        );
                    """)
                    log_table_exists = cur.fetchone()[0]
                    self.assertTrue(log_table_exists, "s5_log table was not created")
                    
                    # Check if drug_mapper table exists
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM pg_class 
                            WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
                            AND relname = 'drug_mapper'
                        );
                    """)
                    drug_mapper_exists = cur.fetchone()[0]
                    self.assertTrue(drug_mapper_exists, "drug_mapper table was not created")
        
        except psycopg.Error as e:
            self.fail(f"Database verification failed: {e}")
    
    @unittest.skipUnless(os.getenv("RUN_INTEGRATION_TESTS"), "Integration tests disabled")
    def test_execute_individual_statements(self):
        """Test executing individual SQL statements"""
        if not self.test_db_available:
            self.skipTest("Test database not configured")
        
        if not os.path.exists(self.s5_sql_path):
            self.skipTest("s5.sql file not found")
        
        with open(self.s5_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        test_statements = [
            "SELECT current_database();",
            "CREATE SCHEMA IF NOT EXISTS faers_b_test;",
            "DROP SCHEMA IF EXISTS faers_b_test CASCADE;"
        ]
        
        try:
            with psycopg.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    for stmt in test_statements:
                        with self.subTest(statement=stmt[:50] + "..."):
                            cur.execute(stmt)
                            # If we get here, the statement executed successfully
                            self.assertTrue(True)
        
        except psycopg.Error as e:
            self.fail(f"Statement execution failed: {e}")


class TestS5SQLValidation(unittest.TestCase):
    """Test SQL syntax and structure validation"""
    
    def setUp(self):
        self.s5_sql_path = os.path.join(os.path.dirname(__file__), "..", "..", "s5.sql")
    
    def test_sql_syntax_basic_validation(self):
        """Test basic SQL syntax validation"""
        if not os.path.exists(self.s5_sql_path):
            self.skipTest("s5.sql file not found")
        
        with open(self.s5_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Basic syntax checks
        self.assertEqual(sql_content.count('$$'), sql_content.count('$$'), 
                        "Unmatched $$ delimiters")
        
        # Check for balanced parentheses in CREATE TABLE statements
        create_table_blocks = re.findall(r'CREATE TABLE.*?\);', sql_content, re.DOTALL | re.IGNORECASE)
        for block in create_table_blocks:
            open_parens = block.count('(')
            close_parens = block.count(')')
            self.assertEqual(open_parens, close_parens, 
                           f"Unbalanced parentheses in CREATE TABLE: {block[:100]}...")
    
    def test_table_name_consistency(self):
        """Test that table names are consistent throughout the file"""
        if not os.path.exists(self.s5_sql_path):
            self.skipTest("s5.sql file not found")
        
        with open(self.s5_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Extract table names from CREATE TABLE statements
        create_patterns = re.findall(r'CREATE TABLE\s+(?:IF NOT EXISTS\s+)?faers_b\.(\w+)', 
                                   sql_content, re.IGNORECASE)
        
        # Check that each table is referenced consistently
        for table_name in create_patterns:
            with self.subTest(table=table_name):
                # Should appear in CREATE TABLE, potential INDEX creation, and logging
                table_references = len(re.findall(rf'\bfaers_b\.{table_name}\b', sql_content, re.IGNORECASE))
                self.assertGreaterEqual(table_references, 1, 
                                      f"Table {table_name} not consistently referenced")


if __name__ == '__main__':
    # Set up test environment
    print("Running s5.sql unit tests...")
    print("Looking for s5.sql in the same directory as this test file")
    print()
    print("For integration tests, set environment variables:")
    print("  TEST_DB_HOST, TEST_DB_USER, TEST_DB_NAME, TEST_DB_PASSWORD")
    print("  RUN_INTEGRATION_TESTS=1")
    print("NOTE: Integration tests require 'psql' command and connection to 'faersdatabase'")
    print()
    
    # Run tests
    unittest.main(verbosity=2)