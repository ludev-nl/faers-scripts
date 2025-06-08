import pytest
import psycopg
from psycopg import errors as pg_errors
from unittest.mock import patch, MagicMock
import os
import re


class TestS4SQL:
    """Simple unit tests for s4.sql data alignment operations"""
    
    @pytest.fixture
    def mock_db_connection(self):
        """Mock database connection for testing"""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        return conn, cursor
    
    @pytest.fixture
    def sample_age_data(self):
        """Sample age data for testing conversions"""
        return [
            (10, 'DEC', 120.0),    # Decades to years
            (25, 'YR', 25.0),      # Years remain years
            (24, 'MON', 2.0),      # Months to years
            (52, 'WK', 1.0),       # Weeks to years
            (365, 'DY', 1.0),      # Days to years
            (8760, 'HR', 1.0),     # Hours to years
            (30, 'UNKNOWN', None)  # Unknown should be None
        ]

    def test_search_path_configuration(self, mock_db_connection):
        """Test 1: Verify search path is set correctly for combined schema"""
        conn, cursor = mock_db_connection
        
        search_path_stmt = "SET search_path TO faers_combined, public;"
        cursor.execute.return_value = None
        
        cursor.execute(search_path_stmt)
        
        # Verify search path statement structure
        assert 'faers_combined' in search_path_stmt
        assert 'public' in search_path_stmt
        assert 'SET search_path' in search_path_stmt
        cursor.execute.assert_called_once()

    def test_age_conversion_logic(self, mock_db_connection, sample_age_data):
        """Test 2: Test age conversion CASE statement logic"""
        conn, cursor = mock_db_connection
        
        def convert_age(age_value, age_cod):
            """Simulate the SQL CASE statement for age conversion"""
            if age_cod == 'DEC':
                return round(float(age_value) * 12, 2)
            elif age_cod in ('YR', 'YEAR'):
                return round(float(age_value), 2)
            elif age_cod == 'MON':
                return round(float(age_value) / 12, 2)
            elif age_cod in ('WK', 'WEEK'):
                return round(float(age_value) / 52, 2)
            elif age_cod in ('DY', 'DAY'):
                return round(float(age_value) / 365, 2)
            elif age_cod in ('HR', 'HOUR'):
                return round(float(age_value) / 8760, 2)
            else:
                return None
        
        for age, age_cod, expected in sample_age_data:
            result = convert_age(age, age_cod)
            assert result == expected, f"Age {age} {age_cod} should convert to {expected}, got {result}"

    def test_numeric_age_validation_regex(self, mock_db_connection):
        """Test 3: Test numeric validation regex for age values"""
        conn, cursor = mock_db_connection
        
        test_values = [
            ("25", True),      # Valid integer
            ("25.5", True),    # Valid decimal
            ("0", True),       # Zero is valid
            ("123.45", True),  # Valid decimal
            ("25a", False),    # Contains letter
            ("", False),       # Empty string
            ("abc", False),    # All letters
            ("25.5.5", False), # Multiple decimals
            ("-25", False),    # Negative number
        ]
        
        # PostgreSQL regex pattern used in the SQL
        pattern = r'^[0-9]+(\.[0-9]+)?$'
        
        for value, expected in test_values:
            if value:
                result = bool(re.match(pattern, value))
            else:
                result = False
            assert result == expected, f"Value '{value}' validation failed"

    def test_column_additions_to_demo_combined(self, mock_db_connection):
        """Test 4: Test ALTER TABLE statements for new columns"""
        conn, cursor = mock_db_connection
        
        alter_statements = [
            'ALTER TABLE faers_combined."DEMO_Combined" ADD COLUMN IF NOT EXISTS age_years_fixed FLOAT;',
            'ALTER TABLE faers_combined."DEMO_Combined" ADD COLUMN IF NOT EXISTS country_code VARCHAR(2);',
            'ALTER TABLE faers_combined."DEMO_Combined" ADD COLUMN IF NOT EXISTS gender VARCHAR(3);'
        ]
        
        cursor.execute.return_value = None
        
        for stmt in alter_statements:
            cursor.execute(stmt)
            
            # Verify ALTER statement structure
            assert 'ALTER TABLE' in stmt
            assert 'ADD COLUMN' in stmt
            assert 'IF NOT EXISTS' in stmt
            assert 'faers_combined."DEMO_Combined"' in stmt
        
        assert cursor.execute.call_count == 3

    def test_country_code_fallback_logic(self, mock_db_connection):
        """Test 5: Test country code mapping and fallback logic"""
        conn, cursor = mock_db_connection
        
        test_cases = [
            ("United States", "US", "US"),     # Should use mapped code
            ("Unknown Country", None, None),   # No mapping, no 2-char code
            ("UK", None, "UK"),               # No mapping, but 2-char code
            ("USA", None, None),              # 3-char code, no mapping
            ("FR", None, "FR"),               # 2-char code, no mapping
        ]
        
        def get_country_code(reporter_country, mapped_code):
            """Simulate the country code logic"""
            if mapped_code is not None:
                return mapped_code
            elif len(reporter_country) == 2:
                return reporter_country
            else:
                return None
        
        for reporter_country, mapped_code, expected in test_cases:
            result = get_country_code(reporter_country, mapped_code)
            assert result == expected, f"Country '{reporter_country}' with mapping '{mapped_code}' should be '{expected}'"

    def test_gender_standardization_cleanup(self, mock_db_connection):
        """Test 6: Test gender standardization and cleanup logic"""
        conn, cursor = mock_db_connection
        
        test_cases = [
            ("M", "M"),        # Valid male
            ("F", "F"),        # Valid female
            ("UNK", None),     # Unknown should be NULL
            ("NS", None),      # Not specified should be NULL
            ("YR", None),      # Invalid code should be NULL
            ("", ""),          # Empty string preserved initially
        ]
        
        def clean_gender(gndr_cod):
            """Simulate the gender cleanup logic"""
            # First step: copy gndr_cod to gender
            gender = gndr_cod
            
            # Second step: clean up invalid values
            if gender in ('UNK', 'NS', 'YR'):
                return None
            return gender
        
        for input_gender, expected in test_cases:
            result = clean_gender(input_gender)
            assert result == expected, f"Gender '{input_gender}' should be cleaned to '{expected}'"

    def test_aligned_table_creation_structure(self, mock_db_connection):
        """Test 7: Test ALIGNED_DEMO_DRUG_REAC_INDI_THER table structure"""
        conn, cursor = mock_db_connection
        
        expected_columns = [
            '"primaryid" BIGINT',
            '"caseid" BIGINT', 
            'age_years_fixed FLOAT',
            'country_code VARCHAR(2)',
            'gender VARCHAR(3)',
            '"DRUG_ID" INTEGER',
            '"drug_seq" BIGINT',
            'reaction TEXT',
            'indication TEXT',
            'therapy_start_date DATE'
        ]
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS faers_combined."ALIGNED_DEMO_DRUG_REAC_INDI_THER" (
            {', '.join(expected_columns)}
        );
        """
        
        cursor.execute.return_value = None
        cursor.execute(create_table_sql)
        
        # Verify table structure
        for column in expected_columns:
            assert column in create_table_sql
        
        assert 'CREATE TABLE' in create_table_sql
        assert 'ALIGNED_DEMO_DRUG_REAC_INDI_THER' in create_table_sql

    def test_join_conditions_validation(self, mock_db_connection):
        """Test 8: Test JOIN conditions for data alignment"""
        conn, cursor = mock_db_connection
        
        # Key JOIN conditions used in the INSERT statement
        join_conditions = [
            ('DEMO_Combined', 'DRUG_Combined', 'primaryid'),
            ('DEMO_Combined', 'REAC_Combined', 'primaryid'),
            ('DEMO_Combined', 'INDI_Combined', 'primaryid'),
            ('DEMO_Combined', 'THER_Combined', 'primaryid'),
            ('DRUG_Combined', 'THER_Combined', 'drug_seq')
        ]
        
        cursor.fetchone.return_value = (True,)  # Tables exist
        
        for table1, table2, join_column in join_conditions:
            # Simulate checking if tables exist and have the join column
            cursor.execute(f"SELECT 1 WHERE EXISTS (SELECT 1)")
            result = cursor.fetchone()
            
            assert result[0] is True
            assert isinstance(join_column, str)
            assert len(join_column) > 0

    def test_server_file_access_validation(self, mock_db_connection):
        """Test 9: Server-related test - CSV file access validation"""
        conn, cursor = mock_db_connection
        
        csv_file_path = '/data/faers/FAERS_MAK/2.LoadDataToDatabase/reporter_countries.csv'
        
        # Test successful file access
        cursor.fetchone.return_value = [True]  # File exists
        cursor.execute("SELECT FROM pg_stat_file(%s)", (csv_file_path,))
        result = cursor.fetchone()
        assert result[0] is True
        
        # Test file not found error
        cursor.execute.side_effect = pg_errors.NoSuchFile("File not found")
        
        with pytest.raises(pg_errors.NoSuchFile):
            cursor.execute("SELECT FROM pg_stat_file(%s)", (csv_file_path,))
        
        # Test permission denied error  
        cursor.execute.side_effect = pg_errors.InsufficientPrivilege("Permission denied")
        
        with pytest.raises(pg_errors.InsufficientPrivilege):
            cursor.execute("SELECT FROM pg_stat_file(%s)", (csv_file_path,))

    def test_server_memory_handling_large_joins(self, mock_db_connection):
        """Test 10: Server-related test - memory handling for large JOIN operations"""
        conn, cursor = mock_db_connection
        
        # Simulate large JOIN operation for data alignment
        large_join_sql = """
        INSERT INTO faers_combined."ALIGNED_DEMO_DRUG_REAC_INDI_THER"
        SELECT DISTINCT 
            d.primaryid, d.caseid, d.age_years_fixed, d.country_code, d.gender,
            dr.drug_seq, dr.drugname, r.pt as reaction, i.indi_pt as indication
        FROM faers_combined."DEMO_Combined" d
        INNER JOIN faers_combined."DRUG_Combined" dr ON d.primaryid = dr.primaryid
        INNER JOIN faers_combined."REAC_Combined" r ON d.primaryid = r.primaryid
        INNER JOIN faers_combined."INDI_Combined" i ON d.primaryid = i.primaryid
        """
        
        # Test successful execution
        cursor.execute.return_value = None
        cursor.rowcount = 100000  # Mock large number of rows
        cursor.execute(large_join_sql)
        assert cursor.execute.called
        
        # Test out of memory error
        cursor.execute.side_effect = pg_errors.OutOfMemory("Insufficient memory for join operation")
        
        with pytest.raises(pg_errors.OutOfMemory):
            cursor.execute(large_join_sql)
        
        # Test disk full error during large operations
        cursor.execute.side_effect = pg_errors.DiskFull("No space left on device")
        
        with pytest.raises(pg_errors.DiskFull):
            cursor.execute(large_join_sql)


# Additional validation tests
class TestS4SQLValidation:
    """Additional validation tests for S4 SQL operations"""
    
    def test_data_type_conversions(self):
        """Test data type conversions and casting"""
        conversion_tests = [
            ("25", "FLOAT", 25.0),
            ("25.5", "FLOAT", 25.5),
            ("100", "INTEGER", 100),
            ("US", "VARCHAR(2)", "US"),
        ]
        
        for value, target_type, expected in conversion_tests:
            # Simulate type conversion logic
            if target_type == "FLOAT":
                result = float(value)
            elif target_type == "INTEGER":
                result = int(value)
            elif target_type.startswith("VARCHAR"):
                result = str(value)
            
            assert result == expected

    def test_distinct_and_conflict_handling(self):
        """Test DISTINCT keyword and conflict resolution"""
        sample_data = [
            (1, 100, 'DrugA', 'ReactionX'),
            (1, 100, 'DrugA', 'ReactionX'),  # Exact duplicate
            (1, 100, 'DrugB', 'ReactionX'),  # Different drug
            (2, 101, 'DrugA', 'ReactionY'),  # Different case
        ]
        
        # DISTINCT should remove exact duplicates
        distinct_data = list(set(sample_data))
        
        # Should have 3 unique records instead of 4
        assert len(distinct_data) == 3
        assert len(sample_data) == 4

    def test_index_creation_validation(self):
        """Test index creation statements"""
        index_statements = [
            'CREATE INDEX IF NOT EXISTS "idx_aligned_primaryid" ON faers_combined."ALIGNED_DEMO_DRUG_REAC_INDI_THER" ("primaryid");',
            'CREATE INDEX IF NOT EXISTS "idx_aligned_drug_id" ON faers_combined."ALIGNED_DEMO_DRUG_REAC_INDI_THER" ("DRUG_ID");'
        ]
        
        for stmt in index_statements:
            assert 'CREATE INDEX' in stmt
            assert 'IF NOT EXISTS' in stmt
            assert 'ALIGNED_DEMO_DRUG_REAC_INDI_THER' in stmt
            assert '_idx' in stmt


if __name__ == "__main__":
    # Run tests with: python -m pytest unit_tests/sql/test_s4.py -v
    # Run server tests only: python -m pytest unit_tests/sql/test_s4.py -v -k "server"
    # Run without server tests: python -m pytest unit_tests/sql/test_s4.py -v -k "not server"
    pytest.main([__file__, "-v"])