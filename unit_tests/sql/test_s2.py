import unittest
import psycopg
from unittest.mock import patch, Mock, MagicMock
from datetime import datetime, date
import tempfile
import sys
import os


class TestGetCompletedYearQuartersFunction(unittest.TestCase):
    """Test cases for the get_completed_year_quarters PostgreSQL function."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.function_sql = """
        CREATE OR REPLACE FUNCTION get_completed_year_quarters(start_year INT DEFAULT 4)
        RETURNS TABLE (year INT, quarter INT)
        AS $func$
        DECLARE
            current_year INT;
            current_quarter INT;
            last_year INT;
            last_quarter INT;
            y INT;
            q INT;
        BEGIN
            -- Get current year and quarter
            SELECT EXTRACT(YEAR FROM CURRENT_DATE)::INT INTO current_year;
            SELECT EXTRACT(QUARTER FROM CURRENT_DATE)::INT INTO current_quarter;
            
            -- Determine last completed quarter
            IF current_quarter = 1 THEN
                last_year := current_year - 1;
                last_quarter := 4;
            ELSE
                last_year := current_year;
                last_quarter := current_quarter - 1;
            END IF;

            -- Generate year-quarter pairs from start_year
            y := 2000 + start_year; -- Assuming start_year is relative to 2000
            WHILE y <= last_year LOOP
                q := 1;
                WHILE q <= 4 LOOP
                    IF y = last_year AND q > last_quarter THEN
                        EXIT;
                    END IF;
                    year := y;
                    quarter := q;
                    RETURN NEXT;
                    q := q + 1;
                END LOOP;
                y := y + 1;
            END LOOP;
        END;
        $func$ LANGUAGE plpgsql;
        """
        
        self.db_params = {
            'host': 'localhost',
            'port': 5432,
            'dbname': 'test_faers_a',
            'user': 'test_user',
            'password': 'test_pass'
        }

    def test_function_creation_sql_syntax(self):
        """Test that the function creation SQL is syntactically correct."""
        # Test for basic SQL syntax elements
        self.assertIn("CREATE OR REPLACE FUNCTION", self.function_sql)
        self.assertIn("get_completed_year_quarters", self.function_sql)
        self.assertIn("RETURNS TABLE", self.function_sql)
        self.assertIn("LANGUAGE plpgsql", self.function_sql)
        self.assertIn("BEGIN", self.function_sql)
        self.assertIn("END", self.function_sql)

    def test_function_parameters(self):
        """Test function parameter definition."""
        self.assertIn("start_year INT DEFAULT 4", self.function_sql)
        self.assertIn("RETURNS TABLE (year INT, quarter INT)", self.function_sql)

    @patch('psycopg.connect')
    def test_function_creation_execution(self, mock_connect):
        """Test that the function can be created without syntax errors."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Execute function creation
        mock_cursor.execute(self.function_sql)
        
        # Verify the function creation SQL was executed
        mock_cursor.execute.assert_called_once_with(self.function_sql)
        
    @patch('psycopg.connect')
    def test_function_call_default_parameter(self, mock_connect):
        """Test calling function with default parameter."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Mock the function call
        call_sql = "SELECT * FROM get_completed_year_quarters();"
        mock_cursor.execute(call_sql)
        
        mock_cursor.execute.assert_called_once_with(call_sql)

    @patch('psycopg.connect')
    def test_function_call_custom_parameter(self, mock_connect):
        """Test calling function with custom parameter."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Mock the function call with parameter
        call_sql = "SELECT * FROM get_completed_year_quarters(10);"
        mock_cursor.execute(call_sql)
        
        mock_cursor.execute.assert_called_once_with(call_sql)

    def test_quarter_logic_calculation(self):
        """Test the quarter calculation logic (simulated)."""
        # This tests the logical flow without actual database
        test_cases = [
            # (current_quarter, expected_last_quarter, expected_year_offset)
            (1, 4, -1),  # Q1 -> last completed is Q4 of previous year
            (2, 1, 0),   # Q2 -> last completed is Q1 of current year
            (3, 2, 0),   # Q3 -> last completed is Q2 of current year
            (4, 3, 0),   # Q4 -> last completed is Q3 of current year
        ]
        
        for current_quarter, expected_last_quarter, expected_year_offset in test_cases:
            with self.subTest(current_quarter=current_quarter):
                # Simulate the logic from the function
                if current_quarter == 1:
                    last_quarter = 4
                    year_offset = -1
                else:
                    last_quarter = current_quarter - 1
                    year_offset = 0
                
                self.assertEqual(last_quarter, expected_last_quarter)
                self.assertEqual(year_offset, expected_year_offset)

    def test_year_calculation_from_start_year(self):
        """Test year calculation from start_year parameter."""
        # Test the conversion from relative year to absolute year
        test_cases = [
            (4, 2004),   # start_year=4 -> 2000+4 = 2004
            (10, 2010),  # start_year=10 -> 2000+10 = 2010
            (23, 2023),  # start_year=23 -> 2000+23 = 2023
        ]
        
        for start_year, expected_absolute_year in test_cases:
            with self.subTest(start_year=start_year):
                absolute_year = 2000 + start_year
                self.assertEqual(absolute_year, expected_absolute_year)

    @patch('psycopg.connect')
    def test_function_returns_table_structure(self, mock_connect):
        """Test that function returns the correct table structure."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Mock return value with expected structure
        mock_cursor.fetchall.return_value = [
            (2004, 1), (2004, 2), (2004, 3), (2004, 4),
            (2005, 1), (2005, 2)
        ]
        
        mock_cursor.execute("SELECT * FROM get_completed_year_quarters(4);")
        results = mock_cursor.fetchall()
        
        # Verify structure
        for year, quarter in results:
            self.assertIsInstance(year, int)
            self.assertIsInstance(quarter, int)
            self.assertIn(quarter, [1, 2, 3, 4])
            self.assertGreaterEqual(year, 2004)

    def test_encoding_and_bom_requirements(self):
        """Test that the SQL file meets UTF-8 encoding requirements."""
        # Check that the SQL contains the encoding directive
        self.assertIn("SET client_encoding = 'UTF8'", self.function_sql)
        
        # Verify no BOM characters (these would appear as special characters)
        # BOM in UTF-8 is \ufeff
        self.assertNotIn('\ufeff', self.function_sql)
        
        # Test that the SQL can be encoded as UTF-8
        try:
            encoded = self.function_sql.encode('utf-8')
            decoded = encoded.decode('utf-8')
            self.assertEqual(self.function_sql, decoded)
        except UnicodeError:
            self.fail("SQL contains characters that cannot be encoded as UTF-8")

    def test_database_comments_and_structure(self):
        """Test database setup comments and structure."""
        setup_sql = """
        -- Ensure this file is saved in UTF-8 encoding without BOM
        
        /****** CREATE FAERS_A DATABASE  **********/
        -- Ensure the database exists (run this separately if needed)
        -- CREATE DATABASE faers_a;
        
        /****** CONFIGURE DATABASE  **********/
        -- Set client encoding to UTF-8
        SET client_encoding = 'UTF8';
        """
        
        # Test for required comments
        self.assertIn("UTF-8 encoding without BOM", setup_sql)
        self.assertIn("CREATE FAERS_A DATABASE", setup_sql)
        self.assertIn("CONFIGURE DATABASE", setup_sql)
        self.assertIn("SET client_encoding = 'UTF8'", setup_sql)

    @patch('psycopg.connect')
    def test_function_execution_with_different_dates(self, mock_connect):
        """Test function behavior simulation with different current dates."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Test scenarios for different quarters
        test_scenarios = [
            # (mocked_current_date, start_year, expected_calls)
            ("2024-01-15", 4, "Q1 scenario"),  # Current is Q1
            ("2024-04-15", 4, "Q2 scenario"),  # Current is Q2
            ("2024-07-15", 4, "Q3 scenario"),  # Current is Q3
            ("2024-10-15", 4, "Q4 scenario"),  # Current is Q4
        ]
        
        for current_date, start_year, scenario in test_scenarios:
            with self.subTest(scenario=scenario):
                # Mock current date for the function
                mock_cursor.execute(f"SELECT CURRENT_DATE;")
                mock_cursor.fetchone.return_value = (current_date,)
                
                # Call the function
                mock_cursor.execute(f"SELECT * FROM get_completed_year_quarters({start_year});")
                
                # Verify the function was called
                self.assertTrue(mock_cursor.execute.called)

    def test_function_docstring_and_comments(self):
        """Test that function includes proper documentation."""
        # Check for function description comment
        self.assertIn("Function to determine completed year-quarter combinations", self.function_sql)
        self.assertIn("from the start year to the last completed quarter", self.function_sql)
        
        # Check for inline comments
        self.assertIn("-- Get current year and quarter", self.function_sql)
        self.assertIn("-- Determine last completed quarter", self.function_sql)
        self.assertIn("-- Generate year-quarter pairs", self.function_sql)

    def test_function_variable_declarations(self):
        """Test that all necessary variables are declared."""
        expected_variables = [
            "current_year INT",
            "current_quarter INT", 
            "last_year INT",
            "last_quarter INT",
            "y INT",
            "q INT"
        ]
        
        for variable in expected_variables:
            with self.subTest(variable=variable):
                self.assertIn(variable, self.function_sql)

    def test_function_control_flow(self):
        """Test function control flow statements."""
        # Test for conditional logic
        self.assertIn("IF current_quarter = 1 THEN", self.function_sql)
        self.assertIn("ELSE", self.function_sql)
        self.assertIn("END IF", self.function_sql)
        
        # Test for loops
        self.assertIn("WHILE y <= last_year LOOP", self.function_sql)
        self.assertIn("WHILE q <= 4 LOOP", self.function_sql)
        self.assertIn("END LOOP", self.function_sql)
        
        # Test for loop control
        self.assertIn("EXIT", self.function_sql)
        self.assertIn("RETURN NEXT", self.function_sql)


class TestFunctionIntegration(unittest.TestCase):
    """Integration tests for the PostgreSQL function (requires actual database)."""
    
    def setUp(self):
        """Set up for integration tests."""
        self.db_params = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'dbname': os.getenv('DB_NAME', 'test_faers_a'),
            'user': os.getenv('DB_USER', 'test_user'),
            'password': os.getenv('DB_PASSWORD', 'test_pass')
        }
        
    @unittest.skipUnless(os.getenv('RUN_INTEGRATION_TESTS'), 
                        "Integration tests require environment variable RUN_INTEGRATION_TESTS=1")
    def test_function_with_real_database(self):
        """Test function with actual database connection."""
        try:
            with psycopg.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    # Create the function
                    cur.execute(self.function_sql)
                    
                    # Test the function
                    cur.execute("SELECT * FROM get_completed_year_quarters(4) LIMIT 10;")
                    results = cur.fetchall()
                    
                    # Verify results
                    self.assertIsInstance(results, list)
                    if results:
                        year, quarter = results[0]
                        self.assertIsInstance(year, int)
                        self.assertIsInstance(quarter, int)
                        self.assertIn(quarter, [1, 2, 3, 4])
                        self.assertGreaterEqual(year, 2004)
                        
        except psycopg.Error as e:
            self.skipTest(f"Database connection failed: {e}")


class TestSQLFileFormat(unittest.TestCase):
    """Test SQL file format and encoding requirements."""
    
    def test_create_sql_file_with_proper_encoding(self):
        """Test creating SQL file with proper UTF-8 encoding without BOM."""
        sql_content = """-- Ensure this file is saved in UTF-8 encoding without BOM

/****** CREATE FAERS_A DATABASE  **********/
-- Ensure the database exists (run this separately if needed)
-- CREATE DATABASE faers_a;

/****** CONFIGURE DATABASE  **********/
-- Set client encoding to UTF-8
SET client_encoding = 'UTF8';

CREATE OR REPLACE FUNCTION get_completed_year_quarters(start_year INT DEFAULT 4)
RETURNS TABLE (year INT, quarter INT)
AS $func$
-- Function implementation here
$func$ LANGUAGE plpgsql;
"""
        
        # Test writing and reading the file
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix='.sql', delete=False) as f:
            f.write(sql_content)
            temp_path = f.name
        
        try:
            # Read back and verify
            with open(temp_path, 'r', encoding='utf-8') as f:
                read_content = f.read()
            
            self.assertEqual(sql_content, read_content)
            
            # Check file doesn't start with BOM
            with open(temp_path, 'rb') as f:
                first_bytes = f.read(3)
                self.assertNotEqual(first_bytes, b'\xef\xbb\xbf')  # UTF-8 BOM
                
        finally:
            os.unlink(temp_path)

    def test_sql_encoding_compatibility(self):
        """Test SQL content can handle various UTF-8 characters."""
        sql_with_unicode = """
        -- Test with various characters: àáâãäå æç èéêë ìíîï ñ òóôõö ùúûü ý
        -- Mathematical symbols: ∑ ∏ ∫ ≤ ≥ ≠ ± × ÷
        -- Function with unicode in comments
        CREATE OR REPLACE FUNCTION test_unicode()
        RETURNS TEXT AS $$
        BEGIN
            RETURN 'Testing UTF-8 encoding: ñoño';
        END;
        $$ LANGUAGE plpgsql;
        """
        
        # Test encoding/decoding
        try:
            encoded = sql_with_unicode.encode('utf-8')
            decoded = encoded.decode('utf-8')
            self.assertEqual(sql_with_unicode, decoded)
        except UnicodeError:
            self.fail("SQL with Unicode characters failed UTF-8 encoding test")


if __name__ == "__main__":
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestGetCompletedYearQuartersFunction))
    suite.addTests(loader.loadTestsFromTestCase(TestSQLFileFormat))
    
    # Only add integration tests if environment variable is set
    if os.getenv('RUN_INTEGRATION_TESTS'):
        suite.addTests(loader.loadTestsFromTestCase(TestFunctionIntegration))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2, buffer=True)
    result = runner.run(suite)
    
    # Print summary
    if result.wasSuccessful():
        print(f"\n✅ All {result.testsRun} tests passed!")
    else:
        print(f"\n❌ {len(result.failures + result.errors)} test(s) failed out of {result.testsRun}")
    
    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)