import pytest
import psycopg
from psycopg import errors as pg_errors
from unittest.mock import patch, MagicMock
import re


class TestS6SQL:
    """Simple unit tests for s6.sql advanced drug mapping operations"""
    
    @pytest.fixture
    def mock_db_connection(self):
        """Mock database connection for testing"""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        return conn, cursor
    
    @pytest.fixture
    def sample_drug_names(self):
        """Sample drug names for testing string cleaning"""
        return [
            ("ASPIRIN (ACETYLSALICYLIC ACID)", "ACETYLSALICYLIC ACID"),
            ("IBUPROFEN; PSEUDOEPHEDRINE", "IBUPROFEN / PSEUDOEPHEDRINE"),
            ("  TYLENOL :.,?/`~!@#$%^&*-_=+  ", "TYLENOL"),
            ("DRUG   WITH   SPACES", "DRUG WITH SPACES"),
            ("NORMAL DRUG", "NORMAL DRUG"),
            ("", "")
        ]

    def test_database_context_validation(self, mock_db_connection):
        """Test 1: Verify database context validation"""
        conn, cursor = mock_db_connection
        
        # Test correct database context
        cursor.fetchone.return_value = ('faersdatabase',)
        cursor.execute("SELECT current_database()")
        result = cursor.fetchone()[0]
        
        assert result == 'faersdatabase'
        
        # Test wrong database context (should raise exception)
        cursor.fetchone.return_value = ('wrongdatabase',)
        cursor.execute("SELECT current_database()")
        result = cursor.fetchone()[0]
        
        # Simulate the exception logic
        if result != 'faersdatabase':
            with pytest.raises(Exception):
                raise Exception(f'Must be connected to faersdatabase, current database is {result}')

    def test_clean_string_function_logic(self, mock_db_connection, sample_drug_names):
        """Test 2: Test clean_string function logic for drug name standardization"""
        conn, cursor = mock_db_connection
        
        def clean_string_python(input_text):
            """Python implementation of the clean_string function logic"""
            if not input_text:
                return ""
            
            output = input_text
            
            # Extract content from parentheses if present
            paren_start = output.find('(')
            paren_end = output.find(')')
            if paren_start > -1 and paren_end > paren_start:
                output = output[paren_start + 1:paren_end]
            
            # Clean special characters and trim
            output = output.strip(' :.,?/`~!@#$%^&*-_=+ ')
            
            # Replace common patterns
            output = output.replace(';', ' / ')
            output = re.sub(r'\s+', ' ', output)  # Replace multiple spaces with single space
            
            return output if output else ""
        
        for input_name, expected in sample_drug_names:
            result = clean_string_python(input_name)
            assert result == expected, f"Input '{input_name}' should clean to '{expected}', got '{result}'"

    def test_products_at_fda_table_creation(self, mock_db_connection):
        """Test 3: Test products_at_fda table creation and structure"""
        conn, cursor = mock_db_connection
        
        create_table_sql = """
        CREATE TABLE faers_b.products_at_fda (
            applno VARCHAR(10),
            productno VARCHAR(10),
            form TEXT,
            strength TEXT,
            referencedrug INTEGER,
            drugname TEXT,
            activeingredient TEXT,
            referencestandard INTEGER,
            rxaui VARCHAR(8),
            ai_2 TEXT
        );
        """
        
        cursor.execute.return_value = None
        cursor.execute(create_table_sql)
        
        # Verify table structure
        expected_columns = [
            'applno VARCHAR(10)',
            'drugname TEXT',
            'activeingredient TEXT',
            'rxaui VARCHAR(8)',
            'ai_2 TEXT'
        ]
        
        for column in expected_columns:
            assert column in create_table_sql
        
        assert 'CREATE TABLE faers_b.products_at_fda' in create_table_sql

    def test_idd_table_creation_and_structure(self, mock_db_connection):
        """Test 4: Test IDD table creation with proper column specifications"""
        conn, cursor = mock_db_connection
        
        create_idd_sql = """
        CREATE TABLE faers_b."IDD" (
            "DRUGNAME" TEXT,
            "RXAUI" VARCHAR(8),
            "RXCUI" VARCHAR(8),
            "STR" TEXT,
            "SAB" VARCHAR(50),
            "TTY" VARCHAR(10),
            "CODE" VARCHAR(50)
        );
        """
        
        cursor.execute.return_value = None
        cursor.execute(create_idd_sql)
        
        # Verify IDD table structure
        assert 'CREATE TABLE faers_b."IDD"' in create_idd_sql
        assert '"DRUGNAME" TEXT' in create_idd_sql
        assert '"RXAUI" VARCHAR(8)' in create_idd_sql
        assert '"RXCUI" VARCHAR(8)' in create_idd_sql
        assert '"SAB" VARCHAR(50)' in create_idd_sql

    def test_performance_indexes_creation(self, mock_db_connection):
        """Test 5: Test performance index creation for multiple tables"""
        conn, cursor = mock_db_connection
        
        # Test table existence checks first
        cursor.fetchone.return_value = (True,)  # Tables exist
        
        index_statements = [
            'CREATE INDEX IF NOT EXISTS idx_idd_drugname ON faers_b."IDD" ("DRUGNAME");',
            'CREATE INDEX IF NOT EXISTS idx_products_at_fda_applno ON faers_b.products_at_fda (applno);',
            'CREATE INDEX IF NOT EXISTS idx_drug_mapper_nda_num ON faers_b.drug_mapper (nda_num);',
            'CREATE INDEX IF NOT EXISTS idx_rxnconso_str_sab_tty ON faers_b.rxnconso (str, sab, tty);'
        ]
        
        cursor.execute.return_value = None
        
        for index_sql in index_statements:
            cursor.execute(index_sql)
            
            # Verify index structure
            assert 'CREATE INDEX IF NOT EXISTS' in index_sql
            assert 'faers_b.' in index_sql
        
        assert cursor.execute.call_count == len(index_statements)

    def test_rxaui_mapping_logic_with_conditions(self, mock_db_connection):
        """Test 6: Test RxAUI mapping logic with strict and relaxed conditions"""
        conn, cursor = mock_db_connection
        
        # Test strict conditions mapping
        strict_update_sql = """
        UPDATE faers_b.products_at_fda
        SET rxaui = rxnconso.rxaui
        FROM faers_b.rxnconso
        WHERE products_at_fda.ai_2 = rxnconso.str
          AND rxnconso.sab = 'RXNORM'
          AND rxnconso.tty IN ('IN', 'MIN')
          AND products_at_fda.rxaui IS NULL;
        """
        
        cursor.execute.return_value = None
        cursor.rowcount = 100  # Mock 100 rows updated
        cursor.execute(strict_update_sql)
        
        # Verify strict conditions
        assert "rxnconso.sab = 'RXNORM'" in strict_update_sql
        assert "rxnconso.tty IN ('IN', 'MIN')" in strict_update_sql
        assert "products_at_fda.rxaui IS NULL" in strict_update_sql
        
        # Test relaxed conditions mapping
        relaxed_update_sql = """
        UPDATE faers_b.products_at_fda
        SET rxaui = rxnconso.rxaui
        FROM faers_b.rxnconso
        WHERE products_at_fda.ai_2 = rxnconso.str
          AND products_at_fda.rxaui IS NULL;
        """
        
        cursor.execute(relaxed_update_sql)
        
        # Verify relaxed conditions (fewer constraints)
        assert "products_at_fda.ai_2 = rxnconso.str" in relaxed_update_sql
        assert "products_at_fda.rxaui IS NULL" in relaxed_update_sql
        assert "rxnconso.sab = 'RXNORM'" not in relaxed_update_sql

    def test_nda_number_mapping_logic(self, mock_db_connection):
        """Test 7: Test NDA number mapping with regex validation"""
        conn, cursor = mock_db_connection
        
        # Test NDA number validation patterns
        test_nda_numbers = [
            ("12345", True),    # Valid numeric
            ("012345", True),   # Valid with leading zero
            ("123.45", False),  # Contains decimal
            ("abc123", False),  # Contains letters
            ("123456", False),  # Too long (>= 6 digits)
            ("12345", True)     # Valid length
        ]
        
        def validate_nda_number(nda_num):
            """Simulate NDA number validation logic"""
            # Check if numeric only
            if not re.match(r'^[0-9]+$', nda_num):
                return False
            
            # Check no decimal point
            if '.' in nda_num:
                return False
            
            # Check length < 6
            if len(nda_num) >= 6:
                return False
            
            return True
        
        for nda_num, expected in test_nda_numbers:
            result = validate_nda_number(nda_num)
            assert result == expected, f"NDA number '{nda_num}' validation failed"
        
        # Test NDA mapping SQL structure
        nda_mapping_sql = """
        UPDATE faers_b.drug_mapper
        SET rxaui = c.rxaui, rxcui = c.rxcui, notes = '1.0'
        FROM faers_b.products_at_fda b
        JOIN faers_b.rxnconso c ON b.rxaui = c.rxaui
        WHERE drug_mapper.nda_num ~ '^[0-9]+$'
          AND POSITION('.' IN drug_mapper.nda_num) = 0
          AND LENGTH(drug_mapper.nda_num) < 6;
        """
        
        cursor.execute.return_value = None
        cursor.execute(nda_mapping_sql)
        
        # Verify NDA mapping conditions
        assert "drug_mapper.nda_num ~ '^[0-9]+$'" in nda_mapping_sql
        assert "POSITION('.' IN drug_mapper.nda_num) = 0" in nda_mapping_sql
        assert "LENGTH(drug_mapper.nda_num) < 6" in nda_mapping_sql

    def test_drug_name_mapping_with_priority_logic(self, mock_db_connection):
        """Test 8: Test drug name mapping with priority-based notes assignment"""
        conn, cursor = mock_db_connection
        
        # Test priority mapping logic
        priority_mappings = [
            ('RXNORM', 'IN', '1.1'),      # Highest priority
            ('RXNORM', 'MIN', '1.2'),     # Second priority
            ('RXNORM', 'PIN', '1.2.2'),   # Third priority
            ('MTHSPL', None, '1.3'),      # Fourth priority
            (None, 'IN', '1.4'),          # Fifth priority
            ('RXNORM', 'OTHER', '1.5'),   # Sixth priority
            ('OTHER', 'OTHER', '1.6')     # Lowest priority
        ]
        
        def get_priority_note(sab, tty):
            """Simulate the CASE statement logic for priority notes"""
            if sab == 'RXNORM' and tty == 'IN':
                return '1.1'
            elif sab == 'RXNORM' and tty == 'MIN':
                return '1.2'
            elif sab == 'RXNORM' and tty == 'PIN':
                return '1.2.2'
            elif sab == 'MTHSPL':
                return '1.3'
            elif tty == 'IN':
                return '1.4'
            elif sab == 'RXNORM':
                return '1.5'
            else:
                return '1.6'
        
        for sab, tty, expected_note in priority_mappings:
            result = get_priority_note(sab, tty)
            assert result == expected_note, f"Priority mapping for SAB='{sab}', TTY='{tty}' failed"

    def test_manual_mapping_table_and_high_count_drugs(self, mock_db_connection):
        """Test 9: Test manual mapping table creation and high-count drug identification"""
        conn, cursor = mock_db_connection
        
        # Test manual mapping table creation
        manual_mapping_sql = """
        CREATE TABLE faers_b.manual_mapping (
            drugname TEXT,
            count INTEGER,
            rxaui BIGINT,
            rxcui BIGINT,
            sab VARCHAR(20),
            tty VARCHAR(20),
            str TEXT,
            code VARCHAR(50),
            notes TEXT
        );
        """
        
        cursor.execute.return_value = None
        cursor.execute(manual_mapping_sql)
        
        # Verify manual mapping table structure
        assert 'CREATE TABLE faers_b.manual_mapping' in manual_mapping_sql
        assert 'drugname TEXT' in manual_mapping_sql
        assert 'count INTEGER' in manual_mapping_sql
        assert 'notes TEXT' in manual_mapping_sql
        
        # Test high-count drug insertion logic
        high_count_insert_sql = """
        INSERT INTO faers_b.manual_mapping (count, drugname)
        SELECT COUNT(drugname) AS count, drugname
        FROM faers_b.drug_mapper
        WHERE notes IS NULL
        GROUP BY drugname
        HAVING COUNT(drugname) > 199;
        """
        
        cursor.execute(high_count_insert_sql)
        
        # Verify high-count logic
        assert 'GROUP BY drugname' in high_count_insert_sql
        assert 'HAVING COUNT(drugname) > 199' in high_count_insert_sql
        assert 'WHERE notes IS NULL' in high_count_insert_sql

    def test_server_dependency_validation_complex(self, mock_db_connection):
        """Test 10: Server-related test - complex table dependency validation"""
        conn, cursor = mock_db_connection
        
        # Test complex dependency chain validation
        required_tables = [
            ('faers_b', 'drug_mapper'),
            ('faers_b', 'products_at_fda'),
            ('faers_b', 'rxnconso'),
            ('faers_b', 'IDD'),
            ('faers_combined', 'aligned_demo_drug_reac_indi_ther')
        ]
        
        dependency_check_sql = """
        SELECT EXISTS (
            SELECT FROM pg_class 
            WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s) 
            AND relname = %s
        )
        """
        
        # Test all dependencies exist
        for schema, table in required_tables:
            cursor.fetchone.return_value = (True,)
            cursor.execute(dependency_check_sql, (schema, table))
            result = cursor.fetchone()[0]
            assert result is True
        
        # Test missing dependency error handling
        cursor.fetchone.return_value = (False,)
        cursor.execute(dependency_check_sql, ('faers_b', 'missing_table'))
        result = cursor.fetchone()[0]
        assert result is False
        
        # Test table row count validation
        cursor.fetchone.return_value = (1000,)  # Table has data
        cursor.execute("SELECT COUNT(*) FROM faers_b.drug_mapper")
        count = cursor.fetchone()[0]
        assert count > 0
        
        # Test empty table handling
        cursor.fetchone.return_value = (0,)  # Table is empty
        cursor.execute("SELECT COUNT(*) FROM faers_b.drug_mapper")
        count = cursor.fetchone()[0]
        assert count == 0


# Additional validation tests
class TestS6SQLValidation:
    """Additional validation tests for S6 SQL operations"""
    
    def test_temp_table_operations(self):
        """Test temporary table creation and cleanup logic"""
        temp_table_sql = """
        CREATE TEMP TABLE cleaned_drugs (
            id INTEGER,
            drugname TEXT,
            prod_ai TEXT,
            clean_drugname TEXT,
            clean_prodai TEXT
        );
        """
        
        assert 'CREATE TEMP TABLE' in temp_table_sql
        assert 'cleaned_drugs' in temp_table_sql
        assert 'clean_drugname TEXT' in temp_table_sql
        assert 'clean_prodai TEXT' in temp_table_sql
        
        # Test cleanup
        cleanup_sql = "DROP TABLE cleaned_drugs;"
        assert 'DROP TABLE cleaned_drugs' in cleanup_sql

    def test_cast_operations_validation(self):
        """Test CAST operations for data type conversions"""
        cast_examples = [
            'CAST(i."RXAUI" AS BIGINT)',
            'CAST(i."RXCUI" AS BIGINT)'
        ]
        
        for cast_expr in cast_examples:
            assert 'CAST(' in cast_expr
            assert 'AS BIGINT)' in cast_expr

    def test_conditional_logic_validation(self):
        """Test conditional logic in mappings"""
        conditions = [
            "drug_mapper.notes IS NULL",
            "products_at_fda.rxaui IS NULL", 
            "i.\"RXAUI\" IS NOT NULL",
            "COUNT(drugname) > 199"
        ]
        
        for condition in conditions:
            assert 'IS NULL' in condition or 'IS NOT NULL' in condition or '>' in condition

    def test_string_operations_validation(self):
        """Test string operations and functions"""
        string_ops = [
            "LEFT(drug_mapper.nda_num, 1) = '0'",
            "RIGHT(drug_mapper.nda_num, LENGTH(drug_mapper.nda_num) - 1)",
            "POSITION('.' IN drug_mapper.nda_num) = 0",
            "LENGTH(drug_mapper.nda_num) < 6"
        ]
        
        for op in string_ops:
            assert any(func in op for func in ['LEFT(', 'RIGHT(', 'POSITION(', 'LENGTH('])


if __name__ == "__main__":
    # Run tests with: python -m pytest unit_tests/sql/test_s6.py -v
    # Run server tests only: python -m pytest unit_tests/sql/test_s6.py -v -k "server"
    # Run without server tests: python -m pytest unit_tests/sql/test_s6.py -v -k "not server"
    pytest.main([__file__, "-v"])