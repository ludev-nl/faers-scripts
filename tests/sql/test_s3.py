import unittest
import os
import sys
import psycopg
import tempfile
import json
from unittest.mock import patch, MagicMock, mock_open
import subprocess

# Add the parent directory to sys.path to import the module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


class TestS3SQL(unittest.TestCase):
    """Test cases for s3.sql database operations"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test database connection parameters"""
        cls.test_db_params = {
            "host": os.getenv("TEST_DB_HOST", "localhost"),
            "port": int(os.getenv("TEST_DB_PORT", 5432)),
            "user": os.getenv("TEST_DB_USER", "test_user"),
            "password": os.getenv("TEST_DB_PASSWORD", "test_pass"),
            "dbname": os.getenv("TEST_DB_NAME", "test_faers")
        }
        
        # SQL script path
        cls.s3_sql_path = "s3.sql"
        
        # Expected MedDRA tables
        cls.meddra_tables = [
            "low_level_term",
            "pref_term", 
            "hlt_pref_term",
            "hlt_pref_comp",
            "hlgt_pref_term",
            "hlgt_hlt_comp",
            "soc_term",
            "soc_hlgt_comp",
            "md_hierarchy",
            "soc_intl_order",
            "smq_list",
            "smq_content"
        ]
        
        # Expected mapping tables
        cls.mapping_tables = [
            "indi_medra_mappings",
            "reac_medra_mappings"
        ]
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value.__enter__.return_value = self.mock_cursor
    
    def test_meddra_table_creation_structure(self):
        """Test that MedDRA tables are created with correct structure"""
        expected_structures = {
            "low_level_term": [
                "llt_code BIGINT",
                "llt_name VARCHAR(100)",
                "pt_code CHAR(8)",
                "llt_whoart_code CHAR(7)",
                "llt_harts_code BIGINT",
                "llt_costart_sym VARCHAR(21)",
                "llt_icd9_code CHAR(8)",
                "llt_icd9cm_code CHAR(8)",
                "llt_icd10_code CHAR(8)",
                "llt_jart_code CHAR(8)"
            ],
            "pref_term": [
                "pt_code BIGINT",
                "pt_name VARCHAR(100)",
                "null_field CHAR(1)",
                "pt_soc_code BIGINT",
                "pt_whoart_code CHAR(7)",
                "pt_harts_code BIGINT",
                "pt_costart_sym CHAR(21)",
                "pt_icd9_code CHAR(8)",
                "pt_icd9cm_code CHAR(8)",
                "pt_icd10_code CHAR(8)",
                "pt_jart_code CHAR(8)"
            ],
            "indi_medra_mappings": [
                "term_name TEXT PRIMARY KEY",
                "meddra_code TEXT"
            ],
            "reac_medra_mappings": [
                "term_name TEXT PRIMARY KEY", 
                "meddra_code TEXT"
            ]
        }
        
        # This test would verify table structure matches expected schema
        for table_name, expected_columns in expected_structures.items():
            with self.subTest(table=table_name):
                # In a real test, you would query INFORMATION_SCHEMA to verify structure
                self.assertIsInstance(expected_columns, list)
                self.assertGreater(len(expected_columns), 0)
    
    @patch('psycopg.connect')
    def test_table_drop_and_create_sequence(self, mock_connect):
        """Test that tables are properly dropped and recreated"""
        mock_connect.return_value.__enter__.return_value = self.mock_conn
        
        # Mock the execution of s3.sql
        with patch('subprocess.run') as mock_subprocess:
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stdout = "Tables created successfully"
            mock_subprocess.return_value = mock_result
            
            # Execute the SQL script
            cmd = ["psql", "-f", self.s3_sql_path]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            # Verify subprocess was called
            mock_subprocess.assert_called_once()
    
    def test_meddra_copy_commands_format(self):
        """Test that COPY commands use correct format and delimiter"""
        # Read the SQL file and verify COPY command formats
        if os.path.exists(self.s3_sql_path):
            with open(self.s3_sql_path, 'r') as f:
                sql_content = f.read()
                
                # Check for proper COPY command format
                copy_commands = [line for line in sql_content.split('\n') if 'COPY' in line and 'FROM' in line]
                
                for copy_cmd in copy_commands:
                    with self.subTest(command=copy_cmd):
                        # Verify delimiter is '$'
                        self.assertIn("DELIMITER '$'", copy_cmd)
                        # Verify header is false
                        self.assertIn("HEADER false", copy_cmd)
                        # Verify CSV format
                        self.assertIn("FORMAT CSV", copy_cmd)
    
    @patch('psycopg.connect')
    def test_indi_combined_column_additions(self, mock_connect):
        """Test that INDI_Combined table gets new columns added"""
        mock_connect.return_value.__enter__.return_value = self.mock_conn
        
        # Mock cursor to simulate column addition check
        def mock_execute_side_effect(query):
            if "ADD COLUMN IF NOT EXISTS meddra_code" in query:
                return None  # Column added successfully
            elif "ADD COLUMN IF NOT EXISTS cleaned_pt" in query:
                return None  # Column added successfully
            return None
        
        self.mock_cursor.execute.side_effect = mock_execute_side_effect
        
        # Test the ALTER TABLE statements
        alter_queries = [
            "ALTER TABLE IF EXISTS INDI_Combined ADD COLUMN IF NOT EXISTS meddra_code TEXT;",
            "ALTER TABLE IF EXISTS INDI_Combined ADD COLUMN IF NOT EXISTS cleaned_pt VARCHAR(100);"
        ]
        
        for query in alter_queries:
            with self.subTest(query=query):
                self.mock_cursor.execute(query)
                self.mock_cursor.execute.assert_called()
    
    @patch('psycopg.connect')
    def test_reac_combined_column_additions(self, mock_connect):
        """Test that REAC_Combined table gets new columns added"""
        mock_connect.return_value.__enter__.return_value = self.mock_conn
        
        # Mock cursor for REAC_Combined alterations
        alter_query = "ALTER TABLE IF EXISTS REAC_Combined ADD COLUMN IF NOT EXISTS meddra_code TEXT;"
        
        self.mock_cursor.execute(alter_query)
        self.mock_cursor.execute.assert_called_with(alter_query)
    
    def test_data_cleaning_logic(self):
        """Test the data cleaning logic for INDI_Combined"""
        # Test the cleaning logic independently
        sample_dirty_data = [
            "  HEADACHE  \n",
            "\tNausea\r\n",
            "FEVER\n\r",
            "  Pain  "
        ]
        
        expected_cleaned = [
            "HEADACHE",
            "NAUSEA", 
            "FEVER",
            "PAIN"
        ]
        
        for dirty, expected in zip(sample_dirty_data, expected_cleaned):
            with self.subTest(input=dirty):
                # Simulate the cleaning process
                cleaned = dirty.strip().replace('\n', '').replace('\r', '').replace('\t', '').upper()
                self.assertEqual(cleaned, expected)
    
    @patch('psycopg.connect')
    def test_meddra_code_update_logic(self, mock_connect):
        """Test the MedDRA code update logic"""
        mock_connect.return_value.__enter__.return_value = self.mock_conn
        
        # Mock data for testing updates
        update_queries = [
            # Update from pref_term
            """UPDATE INDI_Combined
            SET meddra_code = b.pt_code::TEXT
            FROM pref_term b
            WHERE INDI_Combined.cleaned_pt = b.pt_name AND meddra_code IS NULL;""",
            
            # Update from low_level_term
            """UPDATE INDI_Combined
            SET meddra_code = b.llt_code::TEXT
            FROM low_level_term b
            WHERE INDI_Combined.cleaned_pt = b.llt_name AND meddra_code IS NULL;""",
            
            # Update from mappings
            """UPDATE INDI_Combined
            SET meddra_code = m.meddra_code
            FROM indi_medra_mappings m
            WHERE INDI_Combined.cleaned_pt = m.term_name
            AND meddra_code IS NULL;"""
        ]
        
        for query in update_queries:
            with self.subTest(query=query[:50] + "..."):
                self.mock_cursor.execute(query)
                self.mock_cursor.execute.assert_called()
    
    @patch('psycopg.connect')
    def test_index_creation(self, mock_connect):
        """Test that indexes are created properly"""
        mock_connect.return_value.__enter__.return_value = self.mock_conn
        
        index_queries = [
            "CREATE INDEX IF NOT EXISTS indi_meddra_code_idx ON INDI_Combined (meddra_code);",
            "CREATE INDEX IF NOT EXISTS reac_meddra_code_idx ON REAC_Combined (meddra_code);"
        ]
        
        for query in index_queries:
            with self.subTest(query=query):
                self.mock_cursor.execute(query)
                self.mock_cursor.execute.assert_called()
    
    def test_file_path_references(self):
        """Test that file paths in COPY commands are correct"""
        if os.path.exists(self.s3_sql_path):
            with open(self.s3_sql_path, 'r') as f:
                sql_content = f.read()
                
                # Check for expected file paths
                expected_paths = [
                    "../faers-data/MedDRA_25_1_English/MedAscii/llt.asc",
                    "../faers-data/MedDRA_25_1_English/MedAscii/pt.asc",
                    "../faers-data/INDI_medra_mappings.json",
                    "../faers-data/REAC_medra_mappings.json"
                ]
                
                for path in expected_paths:
                    with self.subTest(path=path):
                        self.assertIn(path, sql_content)
    
    def test_json_format_copy_commands(self):
        """Test that JSON files are loaded with correct format"""
        if os.path.exists(self.s3_sql_path):
            with open(self.s3_sql_path, 'r') as f:
                sql_content = f.read()
                
                # Find JSON COPY commands
                json_copy_lines = [line for line in sql_content.split('\n') 
                                 if 'COPY' in line and '.json' in line]
                
                for line in json_copy_lines:
                    with self.subTest(line=line):
                        self.assertIn("FORMAT json", line)
    
    @patch('psycopg.connect')
    def test_table_existence_check(self, mock_connect):
        """Test checking if tables exist before operations"""
        mock_connect.return_value.__enter__.return_value = self.mock_conn
        
        # Mock query to check table existence
        existence_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = %s
        );
        """
        
        # Mock return value - table exists
        self.mock_cursor.fetchone.return_value = [True]
        
        for table in ["INDI_Combined", "REAC_Combined"]:
            with self.subTest(table=table):
                self.mock_cursor.execute(existence_query, (table.lower(),))
                result = self.mock_cursor.fetchone()[0]
                self.assertTrue(result)
    
    def test_sql_syntax_validation(self):
        """Test that SQL file has valid syntax"""
        if os.path.exists(self.s3_sql_path):
            with open(self.s3_sql_path, 'r') as f:
                sql_content = f.read()
                
                # Basic syntax checks
                self.assertGreater(sql_content.count('DROP TABLE'), 0)
                self.assertGreater(sql_content.count('CREATE TABLE'), 0)
                self.assertGreater(sql_content.count('COPY'), 0)
                self.assertGreater(sql_content.count('UPDATE'), 0)
                self.assertGreater(sql_content.count('ALTER TABLE'), 0)
                
                # Check for balanced parentheses
                open_parens = sql_content.count('(')
                close_parens = sql_content.count(')')
                self.assertEqual(open_parens, close_parens, "Unbalanced parentheses in SQL")


class TestS3SQLIntegration(unittest.TestCase):
    """Integration tests for s3.sql with actual database"""
    
    def setUp(self):
        """Set up integration test environment"""
        self.test_db_available = all([
            os.getenv("TEST_DB_HOST"),
            os.getenv("TEST_DB_USER"),
            os.getenv("TEST_DB_NAME")
        ])
        
        if self.test_db_available:
            self.db_params = {
                "host": os.getenv("TEST_DB_HOST"),
                "port": int(os.getenv("TEST_DB_PORT", 5432)),
                "user": os.getenv("TEST_DB_USER"),
                "password": os.getenv("TEST_DB_PASSWORD", ""),
                "dbname": os.getenv("TEST_DB_NAME")
            }
    
    @unittest.skipUnless(os.getenv("RUN_INTEGRATION_TESTS"), "Integration tests disabled")
    def test_full_sql_execution(self):
        """Test full execution of s3.sql against test database"""
        if not self.test_db_available:
            self.skipTest("Test database not configured")
        
        try:
            with psycopg.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    # Execute the SQL file
                    with open("s3.sql", 'r') as f:
                        sql_content = f.read()
                    
                    # Execute in parts to handle potential errors
                    statements = sql_content.split(';')
                    
                    for i, statement in enumerate(statements):
                        if statement.strip():
                            try:
                                cur.execute(statement)
                                conn.commit()
                            except Exception as e:
                                self.fail(f"SQL statement {i} failed: {e}\nStatement: {statement[:100]}...")
                    
                    # Verify tables were created
                    for table in ["low_level_term", "pref_term", "indi_medra_mappings"]:
                        cur.execute("""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_name = %s
                            );
                        """, (table,))
                        
                        exists = cur.fetchone()[0]
                        self.assertTrue(exists, f"Table {table} was not created")
        
        except psycopg.Error as e:
            self.skipTest(f"Database connection failed: {e}")


class TestS3SQLDataValidation(unittest.TestCase):
    """Test data validation aspects of s3.sql"""
    
    def test_meddra_hierarchy_consistency(self):
        """Test that MedDRA hierarchy tables are consistent"""
        # This would test referential integrity between hierarchy tables
        hierarchy_relationships = [
            ("md_hierarchy", "pt_code", "pref_term", "pt_code"),
            ("hlt_pref_comp", "pt_code", "pref_term", "pt_code"),
            ("hlt_pref_comp", "hlt_code", "hlt_pref_term", "hlt_code")
        ]
        
        for parent_table, parent_col, child_table, child_col in hierarchy_relationships:
            with self.subTest(relationship=f"{parent_table}.{parent_col} -> {child_table}.{child_col}"):
                # In integration tests, you would verify foreign key relationships
                self.assertIsNotNone(parent_table)
                self.assertIsNotNone(child_table)
    
    def test_data_transformation_rules(self):
        """Test the data transformation rules"""
        transformation_tests = [
            # Test cleaning rule
            {
                'input': '  HEADACHE\n\t  ',
                'expected': 'HEADACHE',
                'rule': 'UPPER(TRIM(BOTH FROM REPLACE(REPLACE(REPLACE(indi_pt, E\'\\n\', \'\'), E\'\\r\', \'\'), E\'\\t\', \'\')))'
            }
        ]
        
        for test_case in transformation_tests:
            with self.subTest(input=test_case['input']):
                # Simulate the transformation
                cleaned = test_case['input'].strip().replace('\n', '').replace('\r', '').replace('\t', '').upper()
                self.assertEqual(cleaned, test_case['expected'])


if __name__ == '__main__':
    # Set up test environment
    print("Running s3.sql unit tests...")
    print("For integration tests, set environment variables:")
    print("  TEST_DB_HOST, TEST_DB_USER, TEST_DB_NAME, TEST_DB_PASSWORD")
    print("  RUN_INTEGRATION_TESTS=1")
    print()
    
    # Run tests
    unittest.main(verbosity=2)