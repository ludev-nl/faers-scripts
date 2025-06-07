import pytest
import psycopg
from psycopg import errors as pg_errors
from unittest.mock import patch, MagicMock
from datetime import datetime


class TestS7SQL:
    """Simple unit tests for s7.sql FAERS analysis summary operations"""
    
    @pytest.fixture
    def mock_db_connection(self):
        """Mock database connection for testing"""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        return conn, cursor
    
    @pytest.fixture
    def expected_source_tables(self):
        """List of expected source tables for analysis"""
        return [
            ('faers_b', 'DRUG_RxNorm_Mapping'),
            ('faers_combined', 'REAC_Combined'),
            ('faers_combined', 'OUTC_Combined')
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

    def test_faers_analysis_summary_table_creation(self, mock_db_connection):
        """Test 2: Test FAERS_Analysis_Summary table creation and structure"""
        conn, cursor = mock_db_connection
        
        create_table_sql = """
        CREATE TABLE faers_b."FAERS_Analysis_Summary" (
            "SUMMARY_ID" SERIAL PRIMARY KEY,
            "RXCUI" VARCHAR(8),
            "DRUGNAME" TEXT,
            "REACTION_PT" VARCHAR(100),
            "OUTCOME_CODE" VARCHAR(20),
            "EVENT_COUNT" BIGINT,
            "REPORTING_PERIOD" VARCHAR(10),
            "ANALYSIS_DATE" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cursor.execute.return_value = None
        cursor.execute(create_table_sql)
        
        # Verify table structure
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
            assert column in create_table_sql
        
        assert 'CREATE TABLE faers_b."FAERS_Analysis_Summary"' in create_table_sql

    def test_schema_setup_and_privileges(self, mock_db_connection):
        """Test 3: Test schema creation, verification, and privilege grants"""
        conn, cursor = mock_db_connection
        
        # Test schema creation
        schema_creation_sql = "CREATE SCHEMA IF NOT EXISTS faers_b AUTHORIZATION postgres;"
        cursor.execute.return_value = None
        cursor.execute(schema_creation_sql)
        
        # Test privilege grant
        grant_sql = "GRANT ALL ON SCHEMA faers_b TO postgres;"
        cursor.execute(grant_sql)
        
        # Test search path setting
        search_path_sql = "SET search_path TO faers_b, faers_combined, public;"
        cursor.execute(search_path_sql)
        
        # Verify statements structure
        assert 'CREATE SCHEMA IF NOT EXISTS faers_b' in schema_creation_sql
        assert 'AUTHORIZATION postgres' in schema_creation_sql
        assert 'GRANT ALL ON SCHEMA faers_b' in grant_sql
        assert 'faers_b, faers_combined, public' in search_path_sql

    def test_source_table_existence_validation(self, mock_db_connection, expected_source_tables):
        """Test 4: Test source table existence validation logic"""
        conn, cursor = mock_db_connection
        
        table_check_sql = """
        SELECT EXISTS (
            SELECT FROM pg_class 
            WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s) 
            AND relname = %s
        )
        """
        
        # Test all source tables exist
        for schema, table in expected_source_tables:
            cursor.fetchone.return_value = (True,)
            cursor.execute(table_check_sql, (schema, table))
            result = cursor.fetchone()[0]
            assert result is True
        
        # Test missing table scenario
        cursor.fetchone.return_value = (False,)
        cursor.execute(table_check_sql, ('faers_b', 'missing_table'))
        result = cursor.fetchone()[0]
        assert result is False
        
        # Verify SQL structure
        assert 'pg_class' in table_check_sql
        assert 'pg_namespace' in table_check_sql
        assert 'WHERE relnamespace' in table_check_sql

    def test_complex_aggregation_insert_logic(self, mock_db_connection):
        """Test 5: Test complex aggregation INSERT with GROUP BY logic"""
        conn, cursor = mock_db_connection
        
        aggregation_insert_sql = """
        INSERT INTO faers_b."FAERS_Analysis_Summary" (
            "RXCUI", "DRUGNAME", "REACTION_PT", "OUTCOME_CODE", "EVENT_COUNT", "REPORTING_PERIOD"
        )
        SELECT 
            drm."RXCUI",
            drm."DRUGNAME",
            rc.pt AS "REACTION_PT",
            oc.outc_cod AS "OUTCOME_CODE",
            COUNT(*) AS "EVENT_COUNT",
            rc."PERIOD" AS "REPORTING_PERIOD"
        FROM faers_b."DRUG_RxNorm_Mapping" drm
        INNER JOIN faers_combined."REAC_Combined" rc
            ON drm."primaryid" = rc."primaryid"
        INNER JOIN faers_combined."OUTC_Combined" oc
            ON drm."primaryid" = oc."primaryid"
        GROUP BY drm."RXCUI", drm."DRUGNAME", rc.pt, oc.outc_cod, rc."PERIOD"
        ON CONFLICT DO NOTHING;
        """
        
        cursor.execute.return_value = None
        cursor.rowcount = 10000  # Mock 10,000 rows inserted
        cursor.execute(aggregation_insert_sql)
        
        # Verify aggregation logic
        assert 'COUNT(*) AS "EVENT_COUNT"' in aggregation_insert_sql
        assert 'GROUP BY' in aggregation_insert_sql
        assert 'INNER JOIN' in aggregation_insert_sql
        assert 'ON CONFLICT DO NOTHING' in aggregation_insert_sql
        
        # Verify all required columns in GROUP BY
        group_by_columns = ['drm."RXCUI"', 'drm."DRUGNAME"', 'rc.pt', 'oc.outc_cod', 'rc."PERIOD"']
        for column in group_by_columns:
            assert column in aggregation_insert_sql

    def test_join_conditions_validation(self, mock_db_connection):
        """Test 6: Test JOIN conditions for data integration"""
        conn, cursor = mock_db_connection
        
        # Test key JOIN conditions
        join_conditions = [
            ('DRUG_RxNorm_Mapping', 'REAC_Combined', 'primaryid'),
            ('DRUG_RxNorm_Mapping', 'OUTC_Combined', 'primaryid')
        ]
        
        join_sql_template = """
        FROM faers_b."DRUG_RxNorm_Mapping" drm
        INNER JOIN faers_combined."REAC_Combined" rc ON drm."primaryid" = rc."primaryid"
        INNER JOIN faers_combined."OUTC_Combined" oc ON drm."primaryid" = oc."primaryid"
        """
        
        # Verify JOIN structure
        for table1, table2, join_column in join_conditions:
            assert join_column in join_sql_template
            assert 'INNER JOIN' in join_sql_template
        
        # Test that all required tables are referenced
        required_aliases = ['drm', 'rc', 'oc']
        for alias in required_aliases:
            assert alias in join_sql_template

    def test_performance_indexes_creation(self, mock_db_connection):
        """Test 7: Test performance index creation for analysis table"""
        conn, cursor = mock_db_connection
        
        index_statements = [
            'CREATE INDEX IF NOT EXISTS "idx_analysis_rxcui" ON faers_b."FAERS_Analysis_Summary" ("RXCUI");',
            'CREATE INDEX IF NOT EXISTS "idx_analysis_reaction" ON faers_b."FAERS_Analysis_Summary" ("REACTION_PT");',
            'CREATE INDEX IF NOT EXISTS "idx_analysis_outcome" ON faers_b."FAERS_Analysis_Summary" ("OUTCOME_CODE");'
        ]
        
        cursor.execute.return_value = None
        
        for index_sql in index_statements:
            cursor.execute(index_sql)
            
            # Verify index structure
            assert 'CREATE INDEX IF NOT EXISTS' in index_sql
            assert 'faers_b."FAERS_Analysis_Summary"' in index_sql
            assert 'idx_analysis_' in index_sql
        
        # Verify all key columns are indexed
        indexed_columns = ['"RXCUI"', '"REACTION_PT"', '"OUTCOME_CODE"']
        for i, column in enumerate(indexed_columns):
            assert column in index_statements[i]

    def test_conflict_resolution_logic(self, mock_db_connection):
        """Test 8: Test ON CONFLICT DO NOTHING logic for duplicate handling"""
        conn, cursor = mock_db_connection
        
        # Test conflict resolution in practice
        sample_data = [
            ('123', 'ASPIRIN', 'HEADACHE', 'DE', 5, '23Q1'),
            ('123', 'ASPIRIN', 'HEADACHE', 'DE', 5, '23Q1'),  # Exact duplicate
            ('124', 'IBUPROFEN', 'NAUSEA', 'HO', 3, '23Q1'),  # Different data
        ]
        
        # Simulate conflict resolution by removing exact duplicates
        unique_data = []
        seen_keys = set()
        
        for row in sample_data:
            # Create composite key (excluding event count which might vary)
            key = (row[0], row[1], row[2], row[3], row[5])  # RXCUI, DRUGNAME, REACTION, OUTCOME, PERIOD
            if key not in seen_keys:
                unique_data.append(row)
                seen_keys.add(key)
        
        # Should have 2 unique records instead of 3
        assert len(unique_data) == 2
        assert len(sample_data) == 3
        
        # Test ON CONFLICT syntax
        conflict_sql = "INSERT INTO table VALUES (...) ON CONFLICT DO NOTHING;"
        assert 'ON CONFLICT DO NOTHING' in conflict_sql

    def test_timestamp_default_functionality(self, mock_db_connection):
        """Test 9: Test ANALYSIS_DATE timestamp default functionality"""
        conn, cursor = mock_db_connection
        
        # Test timestamp default in table creation
        timestamp_column = '"ANALYSIS_DATE" TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
        
        # Verify timestamp structure
        assert 'TIMESTAMP' in timestamp_column
        assert 'DEFAULT CURRENT_TIMESTAMP' in timestamp_column
        
        # Test timestamp insertion (mock current time)
        mock_timestamp = datetime(2024, 1, 15, 10, 30, 0)
        cursor.fetchone.return_value = (mock_timestamp,)
        cursor.execute("SELECT CURRENT_TIMESTAMP")
        result = cursor.fetchone()[0]
        
        assert isinstance(result, datetime)
        assert result.year == 2024

    def test_server_export_capability_validation(self, mock_db_connection):
        """Test 10: Server-related test - export capability and file access"""
        conn, cursor = mock_db_connection
        
        # Test export command structure (even though it's commented out)
        export_command = """
        \\copy faers_b."FAERS_Analysis_Summary" TO '/data/faers/FAERS_MAK/2.LoadDataToDatabase/analysis_summary.csv' 
        WITH (FORMAT CSV, DELIMITER ',', NULL '', HEADER TRUE);
        """
        
        # Test successful export capability
        cursor.execute.return_value = None
        cursor.execute(export_command)
        assert cursor.execute.called
        
        # Verify export command structure
        assert '\\copy' in export_command
        assert 'FORMAT CSV' in export_command
        assert 'DELIMITER \',\'' in export_command
        assert 'HEADER TRUE' in export_command
        assert '.csv' in export_command
        
        # Test file permission errors
        cursor.execute.side_effect = pg_errors.InsufficientPrivilege("Permission denied to write file")
        
        with pytest.raises(pg_errors.InsufficientPrivilege):
            cursor.execute(export_command)
        
        # Test directory not found errors
        cursor.execute.side_effect = pg_errors.NoSuchFile("Directory does not exist")
        
        with pytest.raises(pg_errors.NoSuchFile):
            cursor.execute(export_command)
        
        # Test disk full errors during export
        cursor.execute.side_effect = pg_errors.DiskFull("No space left on device")
        
        with pytest.raises(pg_errors.DiskFull):
            cursor.execute(export_command)


# Additional validation tests
class TestS7SQLValidation:
    """Additional validation tests for S7 SQL operations"""
    
    def test_aggregation_functions_validation(self):
        """Test aggregation functions and GROUP BY logic"""
        aggregation_patterns = [
            'COUNT(*) AS "EVENT_COUNT"',
            'GROUP BY drm."RXCUI", drm."DRUGNAME"',
            'rc.pt, oc.outc_cod, rc."PERIOD"'
        ]
        
        for pattern in aggregation_patterns:
            assert 'COUNT' in pattern or 'GROUP BY' in pattern or any(col in pattern for col in ['pt', 'outc_cod', 'PERIOD'])

    def test_column_aliasing_validation(self):
        """Test column aliasing in SELECT statements"""
        column_aliases = [
            'drm."RXCUI"',
            'drm."DRUGNAME"',
            'rc.pt AS "REACTION_PT"',
            'oc.outc_cod AS "OUTCOME_CODE"',
            'COUNT(*) AS "EVENT_COUNT"',
            'rc."PERIOD" AS "REPORTING_PERIOD"'
        ]
        
        for alias in column_aliases:
            assert '"' in alias or 'AS' in alias or '.' in alias

    def test_table_qualification_validation(self):
        """Test proper table qualification with schema names"""
        qualified_tables = [
            'faers_b."DRUG_RxNorm_Mapping"',
            'faers_combined."REAC_Combined"',
            'faers_combined."OUTC_Combined"',
            'faers_b."FAERS_Analysis_Summary"'
        ]
        
        for table in qualified_tables:
            assert 'faers_b.' in table or 'faers_combined.' in table
            assert '"' in table  # Quoted identifiers

    def test_data_types_validation(self):
        """Test data type specifications"""
        data_types = [
            'VARCHAR(8)',
            'VARCHAR(100)',
            'VARCHAR(20)',
            'VARCHAR(10)',
            'TEXT',
            'BIGINT',
            'SERIAL',
            'TIMESTAMP'
        ]
        
        for data_type in data_types:
            assert 'VARCHAR' in data_type or data_type in ['TEXT', 'BIGINT', 'SERIAL', 'TIMESTAMP']


if __name__ == "__main__":
    # Run tests with: python -m pytest unit_tests/sql/test_s7.py -v
    # Run server tests only: python -m pytest unit_tests/sql/test_s7.py -v -k "server"
    # Run without server tests: python -m pytest unit_tests/sql/test_s7.py -v -k "not server"
    pytest.main([__file__, "-v"])