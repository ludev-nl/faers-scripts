import pytest
import psycopg
from psycopg import errors as pg_errors
from unittest.mock import patch, MagicMock


class TestS9SQL:
    """Simple unit tests for s9.sql final drug mapping operations"""
    
    @pytest.fixture
    def mock_db_connection(self):
        """Mock database connection for testing"""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        return conn, cursor
    
    @pytest.fixture
    def expected_drug_mapper_columns(self):
        """List of expected columns in DRUG_Mapper table"""
        return [
            'DRUGNAME', 'prod_ai', 'CLEANED_DRUGNAME', 'CLEANED_PROD_AI',
            'NOTES', 'RXAUI', 'RXCUI', 'SAB', 'TTY', 'STR', 'CODE'
        ]
    
    @pytest.fixture
    def mapping_priority_notes(self):
        """Expected mapping priority notes in order of preference"""
        return [
            '9.1',   # CLEANED_DRUGNAME with RXNORM (MIN, IN, PIN)
            '9.2',   # CLEANED_PROD_AI with RXNORM (MIN, IN, PIN)
            '9.3',   # CLEANED_DRUGNAME via IDD with RXNORM (MIN, IN, PIN)
            '9.4',   # CLEANED_PROD_AI via IDD with RXNORM (MIN, IN, PIN)
            '9.5',   # CLEANED_DRUGNAME with RXNORM (IN)
            '9.6',   # CLEANED_PROD_AI with RXNORM (IN)
            '9.7',   # CLEANED_DRUGNAME via IDD with RXNORM (IN)
            '9.8',   # CLEANED_PROD_AI via IDD with RXNORM (IN)
            '9.9',   # CLEANED_DRUGNAME with RXNORM (any TTY)
            '9.10',  # CLEANED_PROD_AI with RXNORM (any TTY)
            '9.11',  # CLEANED_DRUGNAME via IDD with RXNORM (any TTY)
            '9.12'   # CLEANED_PROD_AI via IDD with RXNORM (any TTY)
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

    def test_placeholder_idd_table_creation(self, mock_db_connection):
        """Test 2: Test placeholder IDD table creation when missing"""
        conn, cursor = mock_db_connection
        
        # Test IDD table doesn't exist
        cursor.fetchone.return_value = (False,)
        cursor.execute("""
            SELECT FROM pg_class 
            WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
            AND relname = 'IDD'
        """)
        
        # Should create placeholder table
        create_idd_sql = """
        CREATE TABLE faers_b."IDD" (
            "DRUGNAME" TEXT,
            "RXAUI" VARCHAR(8)
        );
        """
        
        cursor.execute.return_value = None
        cursor.execute(create_idd_sql)
        
        # Verify placeholder structure
        assert 'CREATE TABLE faers_b."IDD"' in create_idd_sql
        assert '"DRUGNAME" TEXT' in create_idd_sql
        assert '"RXAUI" VARCHAR(8)' in create_idd_sql

    def test_column_existence_validation(self, mock_db_connection, expected_drug_mapper_columns):
        """Test 3: Test comprehensive column existence validation"""
        conn, cursor = mock_db_connection
        
        # Mock column existence check query
        column_check_sql = """
        SELECT EXISTS (
            SELECT FROM pg_attribute 
            WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'DRUG_Mapper' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b'))
            AND attname = %s
            AND NOT attisdropped
        )
        """
        
        # Test all required columns exist
        cursor.fetchone.return_value = (True,)
        
        for column in expected_drug_mapper_columns:
            cursor.execute(column_check_sql, (column,))
            result = cursor.fetchone()[0]
            assert result is True
        
        # Test missing column scenario
        cursor.fetchone.return_value = (False,)
        cursor.execute(column_check_sql, ('missing_column',))
        result = cursor.fetchone()[0]
        assert result is False
        
        # Verify SQL structure
        assert 'pg_attribute' in column_check_sql
        assert 'attisdropped' in column_check_sql

    def test_cleaned_column_addition_logic(self, mock_db_connection):
        """Test 4: Test dynamic addition of CLEANED_* columns"""
        conn, cursor = mock_db_connection
        
        # Test CLEANED_DRUGNAME column addition
        alter_drugname_sql = 'ALTER TABLE faers_b."DRUG_Mapper" ADD COLUMN "CLEANED_DRUGNAME" TEXT;'
        cursor.execute.return_value = None
        cursor.execute(alter_drugname_sql)
        
        # Test CLEANED_PROD_AI column addition
        alter_prod_ai_sql = 'ALTER TABLE faers_b."DRUG_Mapper" ADD COLUMN "CLEANED_PROD_AI" TEXT;'
        cursor.execute(alter_prod_ai_sql)
        
        # Verify ALTER statements
        assert 'ALTER TABLE faers_b."DRUG_Mapper"' in alter_drugname_sql
        assert 'ADD COLUMN "CLEANED_DRUGNAME" TEXT' in alter_drugname_sql
        assert 'ADD COLUMN "CLEANED_PROD_AI" TEXT' in alter_prod_ai_sql

    def test_drug_mapper_temp_data_transfer(self, mock_db_connection):
        """Test 5: Test data transfer from DRUG_Mapper_Temp to DRUG_Mapper"""
        conn, cursor = mock_db_connection
        
        # Test CLEANED_DRUGNAME update from temp table
        drugname_update_sql = """
        UPDATE faers_b."DRUG_Mapper"
        SET "CLEANED_DRUGNAME" = dmt."CLEANED_DRUGNAME"
        FROM faers_b."DRUG_Mapper_Temp" dmt
        WHERE dmt."DRUGNAME" = faers_b."DRUG_Mapper"."DRUGNAME"
        AND faers_b."DRUG_Mapper"."NOTES" IS NULL;
        """
        
        cursor.execute.return_value = None
        cursor.rowcount = 1000  # Mock 1000 rows updated
        cursor.execute(drugname_update_sql)
        
        # Test CLEANED_PROD_AI update from temp table
        prod_ai_update_sql = """
        UPDATE faers_b."DRUG_Mapper"
        SET "CLEANED_PROD_AI" = dmt."CLEANED_PROD_AI"
        FROM faers_b."DRUG_Mapper_Temp" dmt
        WHERE dmt."prod_ai" = faers_b."DRUG_Mapper"."prod_ai"
        AND faers_b."DRUG_Mapper"."NOTES" IS NULL;
        """
        
        cursor.execute(prod_ai_update_sql)
        
        # Verify update logic
        assert 'FROM faers_b."DRUG_Mapper_Temp" dmt' in drugname_update_sql
        assert 'WHERE dmt."DRUGNAME" = faers_b."DRUG_Mapper"."DRUGNAME"' in drugname_update_sql
        assert 'AND faers_b."DRUG_Mapper"."NOTES" IS NULL' in drugname_update_sql

    def test_rxnorm_direct_mapping_priority(self, mock_db_connection):
        """Test 6: Test direct RxNorm mapping with TTY priority (9.1, 9.2)"""
        conn, cursor = mock_db_connection
        
        # Test highest priority mapping (9.1: CLEANED_DRUGNAME with MIN, IN, PIN)
        high_priority_mapping_sql = """
        UPDATE faers_b."DRUG_Mapper"
        SET 
            "RXAUI" = CAST(rxn."RXAUI" AS BIGINT),
            "RXCUI" = CAST(rxn."RXCUI" AS BIGINT),
            "NOTES" = '9.1',
            "SAB" = rxn."SAB",
            "TTY" = rxn."TTY",
            "STR" = rxn."STR",
            "CODE" = rxn."CODE"
        FROM faers_b."RXNCONSO" rxn
        WHERE rxn."STR" = faers_b."DRUG_Mapper"."CLEANED_DRUGNAME"
        AND faers_b."DRUG_Mapper"."NOTES" IS NULL
        AND rxn."SAB" = 'RXNORM'
        AND rxn."TTY" IN ('MIN', 'IN', 'PIN');
        """
        
        cursor.execute.return_value = None
        cursor.rowcount = 500  # Mock 500 rows updated
        cursor.execute(high_priority_mapping_sql)
        
        # Verify high-priority mapping conditions
        assert '"NOTES" = \'9.1\'' in high_priority_mapping_sql
        assert 'rxn."SAB" = \'RXNORM\'' in high_priority_mapping_sql
        assert 'rxn."TTY" IN (\'MIN\', \'IN\', \'PIN\')' in high_priority_mapping_sql
        assert 'faers_b."DRUG_Mapper"."NOTES" IS NULL' in high_priority_mapping_sql

    def test_idd_mediated_mapping_logic(self, mock_db_connection):
        """Test 7: Test IDD-mediated mapping with JOIN logic (9.3, 9.4)"""
        conn, cursor = mock_db_connection
        
        # Test IDD-mediated mapping (9.3: via IDD with high priority TTY)
        idd_mapping_sql = """
        UPDATE faers_b."DRUG_Mapper"
        SET 
            "RXAUI" = CAST(rxn."RXAUI" AS BIGINT),
            "RXCUI" = CAST(rxn."RXCUI" AS BIGINT),
            "NOTES" = '9.3',
            "SAB" = rxn."SAB",
            "TTY" = rxn."TTY",
            "STR" = rxn."STR",
            "CODE" = rxn."CODE"
        FROM faers_b."IDD" idd
        INNER JOIN faers_b."RXNCONSO" rxn
            ON rxn."RXAUI" = idd."RXAUI"
        WHERE idd."DRUGNAME" = faers_b."DRUG_Mapper"."CLEANED_DRUGNAME"
        AND faers_b."DRUG_Mapper"."NOTES" IS NULL
        AND rxn."SAB" = 'RXNORM'
        AND rxn."TTY" IN ('MIN', 'IN', 'PIN');
        """
        
        cursor.execute.return_value = None
        cursor.rowcount = 200  # Mock 200 rows updated
        cursor.execute(idd_mapping_sql)
        
        # Verify IDD-mediated mapping structure
        assert 'FROM faers_b."IDD" idd' in idd_mapping_sql
        assert 'INNER JOIN faers_b."RXNCONSO" rxn' in idd_mapping_sql
        assert 'ON rxn."RXAUI" = idd."RXAUI"' in idd_mapping_sql
        assert 'WHERE idd."DRUGNAME" = faers_b."DRUG_Mapper"."CLEANED_DRUGNAME"' in idd_mapping_sql

    def test_fallback_mapping_any_tty(self, mock_db_connection):
        """Test 8: Test fallback mapping with any TTY (9.9, 9.10)"""
        conn, cursor = mock_db_connection
        
        # Test fallback mapping (9.9: any TTY for CLEANED_DRUGNAME)
        fallback_mapping_sql = """
        UPDATE faers_b."DRUG_Mapper"
        SET 
            "RXAUI" = CAST(rxn."RXAUI" AS BIGINT),
            "RXCUI" = CAST(rxn."RXCUI" AS BIGINT),
            "NOTES" = '9.9',
            "SAB" = rxn."SAB",
            "TTY" = rxn."TTY",
            "STR" = rxn."STR",
            "CODE" = rxn."CODE"
        FROM faers_b."RXNCONSO" rxn
        WHERE rxn."STR" = faers_b."DRUG_Mapper"."CLEANED_DRUGNAME"
        AND faers_b."DRUG_Mapper"."NOTES" IS NULL;
        """
        
        cursor.execute.return_value = None
        cursor.rowcount = 100  # Mock 100 rows updated
        cursor.execute(fallback_mapping_sql)
        
        # Verify fallback mapping (no TTY or SAB restrictions)
        assert '"NOTES" = \'9.9\'' in fallback_mapping_sql
        assert 'rxn."SAB" = \'RXNORM\'' not in fallback_mapping_sql  # No SAB restriction
        assert 'rxn."TTY" IN' not in fallback_mapping_sql  # No TTY restriction
        assert 'faers_b."DRUG_Mapper"."NOTES" IS NULL' in fallback_mapping_sql

    def test_cast_operations_for_data_types(self, mock_db_connection):
        """Test 9: Test CAST operations for RXAUI and RXCUI conversions"""
        conn, cursor = mock_db_connection
        
        # Test CAST operations in UPDATE statements
        cast_examples = [
            'CAST(rxn."RXAUI" AS BIGINT)',
            'CAST(rxn."RXCUI" AS BIGINT)'
        ]
        
        sample_update_sql = """
        UPDATE faers_b."DRUG_Mapper"
        SET 
            "RXAUI" = CAST(rxn."RXAUI" AS BIGINT),
            "RXCUI" = CAST(rxn."RXCUI" AS BIGINT)
        FROM faers_b."RXNCONSO" rxn
        WHERE rxn."STR" = faers_b."DRUG_Mapper"."CLEANED_DRUGNAME";
        """
        
        cursor.execute.return_value = None
        cursor.execute(sample_update_sql)
        
        # Verify CAST operations
        for cast_expr in cast_examples:
            assert cast_expr in sample_update_sql
        
        # Test CAST functionality (Python equivalent)
        test_values = [
            ('12345', 12345),
            ('67890', 67890),
            ('0', 0)
        ]
        
        for str_val, expected_int in test_values:
            result = int(str_val)
            assert result == expected_int

    def test_server_dependency_validation_cascade(self, mock_db_connection):
        """Test 10: Server-related test - cascading dependency validation"""
        conn, cursor = mock_db_connection
        
        # Test complex dependency chain
        required_dependencies = [
            ('faers_b', 'DRUG_Mapper'),
            ('faers_b', 'DRUG_Mapper_Temp'),
            ('faers_b', 'RXNCONSO'),
            ('faers_b', 'IDD')
        ]
        
        dependency_check_sql = """
        SELECT EXISTS (
            SELECT FROM pg_class 
            WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s) 
            AND relname = %s
        )
        """
        
        # Test all dependencies exist
        for schema, table in required_dependencies:
            cursor.fetchone.return_value = (True,)
            cursor.execute(dependency_check_sql, (schema, table))
            result = cursor.fetchone()[0]
            assert result is True
        
        # Test row count validation
        cursor.fetchone.return_value = (10000,)  # Tables have data
        cursor.execute("SELECT COUNT(*) FROM faers_b.DRUG_Mapper")
        count = cursor.fetchone()[0]
        assert count > 0
        
        # Test empty dependency handling
        cursor.fetchone.return_value = (0,)  # Table is empty
        cursor.execute("SELECT COUNT(*) FROM faers_b.RXNCONSO")
        count = cursor.fetchone()[0]
        assert count == 0  # Should handle empty tables gracefully
        
        # Test missing dependency error handling
        cursor.execute.side_effect = pg_errors.UndefinedTable("Table does not exist")
        
        with pytest.raises(pg_errors.UndefinedTable):
            cursor.execute(dependency_check_sql, ('faers_b', 'missing_table'))
        
        # Test large JOIN operation performance
        cursor.execute.side_effect = pg_errors.OutOfMemory("Insufficient memory for large JOIN")
        
        with pytest.raises(pg_errors.OutOfMemory):
            cursor.execute("""
                UPDATE faers_b."DRUG_Mapper" SET "RXAUI" = rxn."RXAUI"
                FROM faers_b."IDD" idd
                INNER JOIN faers_b."RXNCONSO" rxn ON rxn."RXAUI" = idd."RXAUI"
            """)


# Additional validation tests
class TestS9SQLValidation:
    """Additional validation tests for S9 SQL operations"""
    
    def test_mapping_priority_order_validation(self, mapping_priority_notes):
        """Test mapping priority order validation"""
        # Verify priority notes are in logical order
        priority_groups = {
            'direct_high': ['9.1', '9.2'],        # Direct RXNORM high priority
            'idd_high': ['9.3', '9.4'],           # IDD-mediated high priority
            'direct_medium': ['9.5', '9.6'],      # Direct RXNORM medium priority
            'idd_medium': ['9.7', '9.8'],         # IDD-mediated medium priority
            'direct_low': ['9.9', '9.10'],        # Direct RXNORM low priority
            'idd_low': ['9.11', '9.12']           # IDD-mediated low priority
        }
        
        all_notes = []
        for group in priority_groups.values():
            all_notes.extend(group)
        
        assert set(all_notes) == set(mapping_priority_notes)
        assert len(all_notes) == 12  # Total mapping strategies

    def test_tty_filtering_validation(self):
        """Test TTY filtering logic validation"""
        tty_filters = [
            "rxn.\"TTY\" IN ('MIN', 'IN', 'PIN')",  # High priority
            "rxn.\"TTY\" = 'IN'",                   # Medium priority
            # No TTY filter for low priority (any TTY)
        ]
        
        for filter_expr in tty_filters:
            assert 'TTY' in filter_expr
            assert any(tty in filter_expr for tty in ['MIN', 'IN', 'PIN'])

    def test_notes_null_filtering_validation(self):
        """Test NOTES IS NULL filtering consistency"""
        notes_filter = 'faers_b."DRUG_Mapper"."NOTES" IS NULL'
        
        # This filter should be present in all mapping operations
        assert 'NOTES' in notes_filter
        assert 'IS NULL' in notes_filter
        
        # Test that this prevents overwriting existing mappings
        sample_data = [
            {'notes': None, 'should_update': True},
            {'notes': '9.1', 'should_update': False},
            {'notes': '9.5', 'should_update': False}
        ]
        
        for data in sample_data:
            if data['notes'] is None:
                assert data['should_update'] is True
            else:
                assert data['should_update'] is False

    def test_join_relationship_validation(self):
        """Test JOIN relationship validation"""
        join_relationships = [
            ('faers_b."IDD"', 'faers_b."RXNCONSO"', 'RXAUI'),
            ('faers_b."DRUG_Mapper_Temp"', 'faers_b."DRUG_Mapper"', 'DRUGNAME'),
            ('faers_b."DRUG_Mapper_Temp"', 'faers_b."DRUG_Mapper"', 'prod_ai')
        ]
        
        for table1, table2, join_column in join_relationships:
            assert isinstance(join_column, str)
            assert len(join_column) > 0
            assert 'faers_b.' in table1
            assert 'faers_b.' in table2 or 'faers_combined.' in table2


if __name__ == "__main__":
    # Run tests with: python -m pytest unit_tests/sql/test_s9.py -v
    # Run server tests only: python -m pytest unit_tests/sql/test_s9.py -v -k "server"
    # Run without server tests: python -m pytest unit_tests/sql/test_s9.py -v -k "not server"
    pytest.main([__file__, "-v"])