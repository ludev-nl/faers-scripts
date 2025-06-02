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


class TestS7SQL(unittest.TestCase):
    """Test cases for s7.sql database operations"""
    
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
        
        # SQL script path - looks for s7.sql in the root of faers-scripts
        cls.s7_sql_path = os.path.join(os.path.dirname(__file__), "..", "..", "s7.sql")
        
        # Expected tables and dependencies
        cls.expected_table = "FAERS_Analysis_Summary"
        cls.dependency_tables = [
            ("faers_b", "DRUG_RxNorm_Mapping"),
            ("faers_combined", "REAC_Combined"),
            ("faers_combined", "OUTC_Combined")
        ]
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value.__enter__.return_value = self.mock_cursor
    
    def test_sql_file_exists(self):
        """Test that s7.sql file exists"""
        self.assertTrue(os.path.exists(self.s7_sql_path), 
                       f"s7.sql file not found at {self.s7_sql_path}")
    
    def test_sql_file_readable(self):
        """Test that s7.sql file is readable"""
        if os.path.exists(self.s7_sql_path):
            try:
                with open(self.s7_sql_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                self.assertGreater(len(content), 0, "s7.sql file is empty")
            except Exception as e:
                self.fail(f"Could not read s7.sql file: {e}")
        else:
            self.skipTest("s7.sql file not found")
    
    def test_database_context_validation_in_sql(self):
        """Test that SQL contains database context validation"""
        if not os.path.exists(self.s7_sql_path):
            self.skipTest("s7.sql file not found")
        
        with open(self.s7_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for database validation
        self.assertIn("current_database()", sql_content)
        self.assertIn("faersdatabase", sql_content)
        self.assertIn("RAISE EXCEPTION", sql_content)
    
    def test_schema_creation_in_sql(self):
        """Test that SQL contains schema creation"""
        if not os.path.exists(self.s7_sql_path):
            self.skipTest("s7.sql file not found")
        
        with open(self.s7_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for schema operations
        self.assertIn("CREATE SCHEMA IF NOT EXISTS faers_b", sql_content)
        self.assertIn("GRANT ALL ON SCHEMA faers_b", sql_content)
        self.assertIn("SET search_path TO faers_b", sql_content)
    
    def test_faers_analysis_summary_table_creation(self):
        """Test that SQL creates FAERS_Analysis_Summary table"""
        if not os.path.exists(self.s7_sql_path):
            self.skipTest("s7.sql file not found")
        
        with open(self.s7_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for table creation
        self.assertIn('CREATE TABLE faers_b."FAERS_Analysis_Summary"', sql_content)
        self.assertIn('DROP TABLE IF EXISTS faers_b."FAERS_Analysis_Summary"', sql_content)
        
        # Check for expected columns
        expected_columns = [
            '"SUMMARY_ID" SERIAL PRIMARY KEY',
            '"RXCUI" VARCHAR(8)',
            '"DRUGNAME" TEXT',
            '"REACTION_PT" VARCHAR(100)',
            '"OUTCOME_CODE" VARCHAR(20)',
            '"EVENT_COUNT" BIGINT',
            '"REPORTING_PERIOD" VARCHAR(10)',
            '"ANALYSIS_DATE" TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
        ]
        
        for column in expected_columns:
            with self.subTest(column=column):
                self.assertIn(column, sql_content)
    
    def test_dependency_table_checks(self):
        """Test that SQL checks for required dependency tables"""
        if not os.path.exists(self.s7_sql_path):
            self.skipTest("s7.sql file not found")
        
        with open(self.s7_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for dependency table existence checks
        dependency_checks = [
            "faers_b.DRUG_RxNorm_Mapping",
            "faers_combined.REAC_Combined", 
            "faers_combined.OUTC_Combined"
        ]
        
        for table in dependency_checks:
            with self.subTest(table=table):
                self.assertIn(table, sql_content)
                self.assertIn(f"Table {table} does not exist", sql_content)
    
    def test_data_insertion_query_structure(self):
        """Test the data insertion query structure"""
        if not os.path.exists(self.s7_sql_path):
            self.skipTest("s7.sql file not found")
        
        with open(self.s7_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for INSERT statement
        self.assertIn('INSERT INTO faers_b."FAERS_Analysis_Summary"', sql_content)
        
        # Check for expected SELECT columns
        expected_select_columns = [
            'drm."RXCUI"',
            'drm."DRUGNAME"',
            'rc.pt AS "REACTION_PT"',
            'oc.outc_cod AS "OUTCOME_CODE"',
            'COUNT(*) AS "EVENT_COUNT"',
            'rc."PERIOD" AS "REPORTING_PERIOD"'
        ]
        
        for column in expected_select_columns:
            with self.subTest(column=column):
                self.assertIn(column, sql_content)
    
    def test_join_conditions(self):
        """Test the JOIN conditions in the INSERT statement"""
        if not os.path.exists(self.s7_sql_path):
            self.skipTest("s7.sql file not found")
        
        with open(self.s7_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for JOIN conditions
        expected_joins = [
            'FROM faers_b."DRUG_RxNorm_Mapping" drm',
            'INNER JOIN faers_combined."REAC_Combined" rc',
            'ON drm."primaryid" = rc."primaryid"',
            'INNER JOIN faers_combined."OUTC_Combined" oc',
            'ON drm."primaryid" = oc."primaryid"'
        ]
        
        for join in expected_joins:
            with self.subTest(join=join):
                self.assertIn(join, sql_content)
    
    def test_group_by_clause(self):
        """Test the GROUP BY clause in the aggregation"""
        if not os.path.exists(self.s7_sql_path):
            self.skipTest("s7.sql file not found")
        
        with open(self.s7_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for GROUP BY clause
        self.assertIn("GROUP BY", sql_content)
        
        # Check for expected grouping columns
        expected_group_columns = [
            'drm."RXCUI"',
            'drm."DRUGNAME"',
            'rc.pt',
            'oc.outc_cod',
            'rc."PERIOD"'
        ]
        
        # Find the GROUP BY clause
        group_by_match = re.search(r'GROUP BY([^;]+)', sql_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(group_by_match, "GROUP BY clause not found")
        
        group_by_clause = group_by_match.group(1)
        for column in expected_group_columns:
            with self.subTest(column=column):
                self.assertIn(column, group_by_clause)
    
    def test_conflict_handling(self):
        """Test the ON CONFLICT DO NOTHING clause"""
        if not os.path.exists(self.s7_sql_path):
            self.skipTest("s7.sql file not found")
        
        with open(self.s7_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for conflict handling
        self.assertIn("ON CONFLICT DO NOTHING", sql_content)
    
    def test_index_creation(self):
        """Test that proper indexes are created"""
        if not os.path.exists(self.s7_sql_path):
            self.skipTest("s7.sql file not found")
        
        with open(self.s7_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for index creation
        expected_indexes = [
            '"idx_analysis_rxcui"',
            '"idx_analysis_reaction"',
            '"idx_analysis_outcome"'
        ]
        
        for index in expected_indexes:
            with self.subTest(index=index):
                self.assertIn(index, sql_content)
                self.assertIn(f"CREATE INDEX IF NOT EXISTS {index}", sql_content)
    
    def test_index_columns(self):
        """Test that indexes are created on correct columns"""
        if not os.path.exists(self.s7_sql_path):
            self.skipTest("s7.sql file not found")
        
        with open(self.s7_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for specific index definitions
        index_definitions = [
            ('idx_analysis_rxcui', '"RXCUI"'),
            ('idx_analysis_reaction', '"REACTION_PT"'),
            ('idx_analysis_outcome', '"OUTCOME_CODE"')
        ]
        
        for index_name, column_name in index_definitions:
            with self.subTest(index=index_name, column=column_name):
                # Look for the index creation with the specific column
                index_pattern = rf'CREATE INDEX IF NOT EXISTS "{index_name}".*{re.escape(column_name)}'
                self.assertRegex(sql_content, index_pattern)
    
    def test_export_functionality_commented(self):
        """Test that export functionality is present but commented"""
        if not os.path.exists(self.s7_sql_path):
            self.skipTest("s7.sql file not found")
        
        with open(self.s7_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for commented export command
        self.assertIn("-- \\copy", sql_content)
        self.assertIn("analysis_summary.csv", sql_content)
        self.assertIn("FORMAT CSV", sql_content)
    
    def test_do_block_structure_validation(self):
        """Test that DO blocks are properly structured"""
        if not os.path.exists(self.s7_sql_path):
            self.skipTest("s7.sql file not found")
        
        with open(self.s7_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for DO blocks
        do_blocks = re.findall(r'DO \$\$.*?\$\$;', sql_content, re.DOTALL)
        self.assertGreater(len(do_blocks), 0, "No DO blocks found")
        
        # Check that DO blocks have proper structure
        for i, block in enumerate(do_blocks):
            with self.subTest(block_number=i+1):
                self.assertIn("BEGIN", block)
                self.assertIn("END", block)
    
    def test_declare_block_in_dependency_check(self):
        """Test the DECLARE block for table existence checking"""
        if not os.path.exists(self.s7_sql_path):
            self.skipTest("s7.sql file not found")
        
        with open(self.s7_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for DECLARE block
        self.assertIn("DECLARE", sql_content)
        self.assertIn("table_exists BOOLEAN", sql_content)
        
        # Check for proper SELECT EXISTS pattern
        self.assertIn("SELECT EXISTS", sql_content)
        self.assertIn("pg_class", sql_content)
        self.assertIn("relnamespace", sql_content)
        self.assertIn("pg_namespace", sql_content)
    
    def test_error_handling_and_notices(self):
        """Test that SQL contains proper error handling and notices"""
        if not os.path.exists(self.s7_sql_path):
            self.skipTest("s7.sql file not found")
        
        with open(self.s7_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for error handling
        self.assertIn("RAISE NOTICE", sql_content)
        self.assertIn("RAISE EXCEPTION", sql_content)
        
        # Check for specific notice messages
        expected_notices = [
            "does not exist, skipping INSERT into FAERS_Analysis_Summary"
        ]
        
        for notice in expected_notices:
            with self.subTest(notice=notice):
                self.assertIn(notice, sql_content)
    
    def test_table_aliases_consistency(self):
        """Test that table aliases are used consistently"""
        if not os.path.exists(self.s7_sql_path):
            self.skipTest("s7.sql file not found")
        
        with open(self.s7_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for consistent table aliases
        aliases = [
            ("DRUG_RxNorm_Mapping", "drm"),
            ("REAC_Combined", "rc"),
            ("OUTC_Combined", "oc")
        ]
        
        for table, alias in aliases:
            with self.subTest(table=table, alias=alias):
                # Check that alias is defined
                self.assertIn(f"{table}\" {alias}", sql_content)
                # Check that alias is used in SELECT
                alias_usage_pattern = rf'{alias}\."[A-Z_]+"'
                self.assertRegex(sql_content, alias_usage_pattern)
    
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
        
        return [s for s in statements if s]
    
    def test_sql_statement_parsing(self):
        """Test that SQL can be parsed into valid statements"""
        if not os.path.exists(self.s7_sql_path):
            self.skipTest("s7.sql file not found")
        
        with open(self.s7_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        statements = self.parse_sql_statements(sql_content)
        self.assertGreater(len(statements), 0, "No SQL statements found")
        
        # Check for expected statement types
        statement_types = {
            'CREATE SCHEMA': 0,
            'CREATE TABLE': 0,
            'CREATE INDEX': 0,
            'DO $$': 0,
            'GRANT': 0,
            'SET': 0,
            'INSERT': 0,
            'DROP TABLE': 0
        }
        
        for stmt in statements:
            stmt_upper = stmt.upper()
            for stmt_type in statement_types:
                if stmt_type in stmt_upper:
                    statement_types[stmt_type] += 1
        
        # Verify we have statements of expected types
        self.assertGreater(statement_types['CREATE TABLE'], 0, "No CREATE TABLE statements")
        self.assertGreater(statement_types['DO $$'], 0, "No DO blocks")
        self.assertGreater(statement_types['INSERT'], 0, "No INSERT statements")
        self.assertGreater(statement_types['CREATE INDEX'], 0, "No CREATE INDEX statements")
    
    def test_aggregation_logic_validation(self):
        """Test the aggregation logic in the INSERT statement"""
        if not os.path.exists(self.s7_sql_path):
            self.skipTest("s7.sql file not found")
        
        with open(self.s7_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for COUNT aggregation
        self.assertIn("COUNT(*) AS", sql_content)
        self.assertIn('"EVENT_COUNT"', sql_content)
        
        # Check that GROUP BY includes all non-aggregate columns
        # This ensures the aggregation is logically correct
        non_aggregate_columns = [
            'drm."RXCUI"',
            'drm."DRUGNAME"',
            'rc.pt',
            'oc.outc_cod',
            'rc."PERIOD"'
        ]
        
        # Find GROUP BY clause
        group_by_match = re.search(r'GROUP BY([^;]+)', sql_content, re.IGNORECASE | re.DOTALL)
        if group_by_match:
            group_by_clause = group_by_match.group(1)
            for column in non_aggregate_columns:
                with self.subTest(column=column):
                    self.assertIn(column, group_by_clause)


class TestS7SQLExecution(unittest.TestCase):
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
        
        self.s7_sql_path = os.path.join(os.path.dirname(__file__), "..", "..", "s7.sql")
    
    @unittest.skipUnless(os.getenv("RUN_INTEGRATION_TESTS"), "Integration tests disabled")
    def test_execute_sql_file_with_psql(self):
        """Test executing s7.sql with psql command"""
        if not self.test_db_available:
            self.skipTest("Test database not configured")
        
        if not os.path.exists(self.s7_sql_path):
            self.skipTest("s7.sql file not found")
        
        # Build psql command
        cmd = [
            "psql",
            "-h", self.db_params["host"],
            "-p", str(self.db_params["port"]),
            "-U", self.db_params["user"],
            "-d", self.db_params["dbname"],
            "-f", self.s7_sql_path,
            "-v", "ON_ERROR_STOP=1"
        ]
        
        env = os.environ.copy()
        if self.db_params.get("password"):
            env["PGPASSWORD"] = self.db_params["password"]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=300)
            
            if result.returncode != 0:
                self.fail(f"SQL execution failed:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}")
            
            self.verify_table_created()
            
        except subprocess.TimeoutExpired:
            self.fail("SQL execution timed out after 5 minutes")
        except FileNotFoundError:
            self.skipTest("psql command not found")
    
    def verify_table_created(self):
        """Verify that FAERS_Analysis_Summary table was created"""
        if not self.test_db_available:
            return
        
        try:
            with psycopg.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    # Check if faers_b schema exists
                    cur.execute("SELECT EXISTS (SELECT FROM pg_namespace WHERE nspname = 'faers_b');")
                    schema_exists = cur.fetchone()[0]
                    self.assertTrue(schema_exists, "faers_b schema was not created")
                    
                    # Check if FAERS_Analysis_Summary table exists
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM pg_class 
                            WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
                            AND relname = 'FAERS_Analysis_Summary'
                        );
                    """)
                    table_exists = cur.fetchone()[0]
                    self.assertTrue(table_exists, "FAERS_Analysis_Summary table was not created")
                    
                    # Check table structure
                    cur.execute("""
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_schema = 'faers_b' 
                        AND table_name = 'FAERS_Analysis_Summary'
                        ORDER BY ordinal_position;
                    """)
                    columns = cur.fetchall()
                    
                    expected_columns = [
                        "SUMMARY_ID", "RXCUI", "DRUGNAME", "REACTION_PT", 
                        "OUTCOME_CODE", "EVENT_COUNT", "REPORTING_PERIOD", "ANALYSIS_DATE"
                    ]
                    
                    actual_columns = [col[0] for col in columns]
                    for expected_col in expected_columns:
                        self.assertIn(expected_col, actual_columns, 
                                    f"Column {expected_col} not found in table")
        
        except psycopg.Error as e:
            self.fail(f"Database verification failed: {e}")
    
    @unittest.skipUnless(os.getenv("RUN_INTEGRATION_TESTS"), "Integration tests disabled")
    def test_indexes_created(self):
        """Test that indexes were created successfully"""
        if not self.test_db_available:
            self.skipTest("Test database not configured")
        
        try:
            with psycopg.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    # Check for expected indexes
                    expected_indexes = [
                        "idx_analysis_rxcui",
                        "idx_analysis_reaction", 
                        "idx_analysis_outcome"
                    ]
                    
                    for index_name in expected_indexes:
                        with self.subTest(index=index_name):
                            cur.execute("""
                                SELECT EXISTS (
                                    SELECT FROM pg_class 
                                    WHERE relkind = 'i' 
                                    AND relname = %s
                                );
                            """, (index_name,))
                            index_exists = cur.fetchone()[0]
                            self.assertTrue(index_exists, f"Index {index_name} was not created")
        
        except psycopg.Error as e:
            self.skipTest(f"Database operation failed: {e}")


class TestS7SQLValidation(unittest.TestCase):
    """Test SQL syntax and structure validation"""
    
    def setUp(self):
        self.s7_sql_path = os.path.join(os.path.dirname(__file__), "..", "..", "s7.sql")
    
    def test_sql_syntax_basic_validation(self):
        """Test basic SQL syntax validation"""
        if not os.path.exists(self.s7_sql_path):
            self.skipTest("s7.sql file not found")
        
        with open(self.s7_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check for balanced $$ delimiters
        dollar_count = sql_content.count('$$')
        self.assertEqual(dollar_count % 2, 0, "Unmatched $$ delimiters")
        
        # Check for balanced parentheses in major statements
        create_table_blocks = re.findall(r'CREATE TABLE.*?\);', sql_content, re.DOTALL | re.IGNORECASE)
        for block in create_table_blocks:
            open_parens = block.count('(')
            close_parens = block.count(')')
            self.assertEqual(open_parens, close_parens, 
                           f"Unbalanced parentheses in CREATE TABLE: {block[:100]}...")
    
    def test_join_relationship_validation(self):
        """Test that JOIN relationships are logically sound"""
        if not os.path.exists(self.s7_sql_path):
            self.skipTest("s7.sql file not found")
        
        with open(self.s7_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check that all JOINs are on primaryid (the common key)
        join_conditions = re.findall(r'ON\s+([^;]+)', sql_content, re.IGNORECASE)
        
        for condition in join_conditions:
            with self.subTest(condition=condition.strip()):
                # Each join should involve primaryid
                self.assertIn("primaryid", condition.lower())
    
    def test_column_reference_consistency(self):
        """Test that column references are consistent with table aliases"""
        if not os.path.exists(self.s7_sql_path):
            self.skipTest("s7.sql file not found")
        
        with open(self.s7_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Find the INSERT statement
        insert_match = re.search(r'INSERT INTO.*?;', sql_content, re.DOTALL | re.IGNORECASE)
        if insert_match:
            insert_statement = insert_match.group(0)
            
            # Check that all column references use proper aliases
            column_references = re.findall(r'([a-z]+)\."?([A-Z_]+)"?', insert_statement)
            
            valid_aliases = {'drm', 'rc', 'oc'}
            for alias, column in column_references:
                with self.subTest(alias=alias, column=column):
                    self.assertIn(alias, valid_aliases, f"Invalid alias '{alias}' used")


if __name__ == '__main__':
    print("Running s7.sql unit tests...")
    print("This tests the SQL script that creates FAERS Analysis Summary")
    print("Looking for s7.sql in the faers-scripts root directory")
    print()
    print("For integration tests, set environment variables:")
    print("  TEST_DB_HOST, TEST_DB_USER, TEST_DB_NAME, TEST_DB_PASSWORD")
    print("  RUN_INTEGRATION_TESTS=1")
    print("NOTE: Integration tests require 'psql' command and connection to 'faersdatabase'")
    print()
    
    unittest.main(verbosity=2)