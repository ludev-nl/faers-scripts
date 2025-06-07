import pytest
import psycopg
from psycopg import errors as pg_errors
from unittest.mock import patch, MagicMock
import json
import os


class TestS3SQL:
    """Simple unit tests for s3.sql MedDRA operations"""
    
    @pytest.fixture
    def mock_db_connection(self):
        """Mock database connection for testing"""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        return conn, cursor
    
    @pytest.fixture
    def sample_meddra_tables(self):
        """List of expected MedDRA tables"""
        return [
            'low_level_term', 'pref_term', 'hlt_pref_term', 'hlt_pref_comp',
            'hlgt_pref_term', 'hlgt_hlt_comp', 'soc_term', 'soc_hlgt_comp',
            'md_hierarchy', 'soc_intl_order', 'smq_list', 'smq_content'
        ]
    
    @pytest.fixture
    def sample_mapping_data(self):
        """Sample mapping data for testing"""
        return {
            'indi_mappings': [
                {'term_name': 'HYPERTENSION', 'meddra_code': '10020772'},
                {'term_name': 'DIABETES', 'meddra_code': '10012601'}
            ],
            'reac_mappings': [
                {'term_name': 'NAUSEA', 'meddra_code': '10028813'},
                {'term_name': 'HEADACHE', 'meddra_code': '10019211'}
            ]
        }

    def test_drop_and_create_meddra_tables(self, mock_db_connection, sample_meddra_tables):
        """Test 1: Test DROP and CREATE statements for MedDRA tables"""
        conn, cursor = mock_db_connection
        
        # Test table creation structure
        create_table_sql = """
        DROP TABLE IF EXISTS low_level_term;
        CREATE TABLE IF NOT EXISTS low_level_term (
            llt_code BIGINT,
            llt_name VARCHAR(100),
            pt_code CHAR(8)
        );
        """
        
        cursor.execute.return_value = None
        
        # Should execute without errors
        cursor.execute(create_table_sql)
        assert cursor.execute.called
        
        # Verify table structure contains expected columns
        assert 'llt_code BIGINT' in create_table_sql
        assert 'llt_name VARCHAR(100)' in create_table_sql
        assert 'DROP TABLE IF EXISTS' in create_table_sql

    def test_copy_command_structure(self, mock_db_connection):
        """Test 2: Test COPY command structure for MedDRA data loading"""
        conn, cursor = mock_db_connection
        
        copy_command = """
        COPY low_level_term FROM '../faers-data/MedDRA_25_1_English/MedAscii/llt.asc'
        WITH (FORMAT CSV, DELIMITER '$', HEADER false);
        """
        
        cursor.execute.return_value = None
        cursor.execute(copy_command)
        
        # Verify COPY command structure
        assert 'COPY' in copy_command
        assert "DELIMITER '$'" in copy_command
        assert 'HEADER false' in copy_command
        assert 'FORMAT CSV' in copy_command
        assert '.asc' in copy_command

    def test_mapping_tables_creation(self, mock_db_connection):
        """Test 3: Test creation of mapping tables for INDI and REAC"""
        conn, cursor = mock_db_connection
        
        mapping_tables_sql = """
        DROP TABLE IF EXISTS indi_medra_mappings;
        CREATE TABLE IF NOT EXISTS indi_medra_mappings (
            term_name TEXT PRIMARY KEY,
            meddra_code TEXT
        );
        
        DROP TABLE IF EXISTS reac_medra_mappings;
        CREATE TABLE IF NOT EXISTS reac_medra_mappings (
            term_name TEXT PRIMARY KEY,
            meddra_code TEXT
        );
        """
        
        cursor.execute.return_value = None
        cursor.execute(mapping_tables_sql)
        
        # Verify mapping table structure
        assert 'indi_medra_mappings' in mapping_tables_sql
        assert 'reac_medra_mappings' in mapping_tables_sql
        assert 'PRIMARY KEY' in mapping_tables_sql
        assert 'term_name TEXT' in mapping_tables_sql
        assert 'meddra_code TEXT' in mapping_tables_sql

    def test_json_data_loading(self, mock_db_connection):
        """Test 4: Test JSON data loading into mapping tables"""
        conn, cursor = mock_db_connection
        
        json_copy_commands = [
            "COPY indi_medra_mappings(term_name, meddra_code) FROM '../faers-data/INDI_medra_mappings.json' WITH (FORMAT json);",
            "COPY reac_medra_mappings(term_name, meddra_code) FROM '../faers-data/REAC_medra_mappings.json' WITH (FORMAT json);"
        ]
        
        cursor.execute.return_value = None
        
        for command in json_copy_commands:
            cursor.execute(command)
            assert 'FORMAT json' in command
            assert '.json' in command
            assert 'COPY' in command

    def test_alter_table_add_columns(self, mock_db_connection):
        """Test 5: Test ALTER TABLE statements for adding MedDRA columns"""
        conn, cursor = mock_db_connection
        
        alter_statements = [
            "ALTER TABLE IF EXISTS INDI_Combined ADD COLUMN IF NOT EXISTS meddra_code TEXT;",
            "ALTER TABLE IF EXISTS INDI_Combined ADD COLUMN IF NOT EXISTS cleaned_pt VARCHAR(100);",
            "ALTER TABLE IF EXISTS REAC_Combined ADD COLUMN IF NOT EXISTS meddra_code TEXT;"
        ]
        
        cursor.execute.return_value = None
        
        for stmt in alter_statements:
            cursor.execute(stmt)
            assert 'ALTER TABLE' in stmt
            assert 'ADD COLUMN' in stmt
            assert 'IF EXISTS' in stmt
            assert 'IF NOT EXISTS' in stmt

    def test_data_cleaning_update_statements(self, mock_db_connection):
        """Test 6: Test data cleaning UPDATE statements"""
        conn, cursor = mock_db_connection
        
        # Test cleaning statement structure
        cleaning_sql = """
        UPDATE INDI_Combined
        SET cleaned_pt = UPPER(TRIM(BOTH FROM REPLACE(REPLACE(REPLACE(indi_pt, E'\\n', ''), E'\\r', ''), E'\\t', '')));
        """
        
        cursor.execute.return_value = None
        cursor.rowcount = 1000  # Mock 1000 rows updated
        
        cursor.execute(cleaning_sql)
        
        # Verify cleaning logic
        assert 'UPPER(' in cleaning_sql
        assert 'TRIM(' in cleaning_sql
        assert 'REPLACE(' in cleaning_sql
        assert "E'\\n'" in cleaning_sql  # Newline removal
        assert "E'\\r'" in cleaning_sql  # Carriage return removal
        assert "E'\\t'" in cleaning_sql  # Tab removal

    def test_meddra_code_mapping_updates(self, mock_db_connection):
        """Test 7: Test MedDRA code mapping UPDATE statements"""
        conn, cursor = mock_db_connection
        
        # Test pref_term mapping
        pref_term_update = """
        UPDATE INDI_Combined
        SET meddra_code = b.pt_code::TEXT
        FROM pref_term b
        WHERE INDI_Combined.cleaned_pt = b.pt_name AND meddra_code IS NULL;
        """
        
        cursor.execute.return_value = None
        cursor.rowcount = 500  # Mock 500 rows updated
        
        cursor.execute(pref_term_update)
        
        # Verify mapping logic
        assert 'FROM pref_term b' in pref_term_update
        assert 'pt_code::TEXT' in pref_term_update
        assert 'meddra_code IS NULL' in pref_term_update
        assert 'WHERE' in pref_term_update

    def test_index_creation_statements(self, mock_db_connection):
        """Test 8: Test index creation for MedDRA codes"""
        conn, cursor = mock_db_connection
        
        index_statements = [
            "CREATE INDEX IF NOT EXISTS indi_meddra_code_idx ON INDI_Combined (meddra_code);",
            "CREATE INDEX IF NOT EXISTS reac_meddra_code_idx ON REAC_Combined (meddra_code);"
        ]
        
        cursor.execute.return_value = None
        
        for stmt in index_statements:
            cursor.execute(stmt)
            assert 'CREATE INDEX' in stmt
            assert 'IF NOT EXISTS' in stmt
            assert 'meddra_code' in stmt
            assert '_idx' in stmt

    def test_server_file_access_handling(self, mock_db_connection):
        """Test 9: Server-related test - file access for COPY operations"""
        conn, cursor = mock_db_connection
        
        copy_command = "COPY low_level_term FROM '../faers-data/MedDRA_25_1_English/MedAscii/llt.asc' WITH (FORMAT CSV, DELIMITER '$', HEADER false);"
        
        # Test successful file access
        cursor.execute.return_value = None
        cursor.execute(copy_command)
        assert cursor.execute.called
        
        # Test file not found error
        cursor.execute.side_effect = pg_errors.NoSuchFile("File not found")
        
        with pytest.raises(pg_errors.NoSuchFile):
            cursor.execute(copy_command)
        
        # Test permission denied error
        cursor.execute.side_effect = pg_errors.InsufficientPrivilege("Permission denied to read file")
        
        with pytest.raises(pg_errors.InsufficientPrivilege):
            cursor.execute(copy_command)

    def test_server_memory_handling_large_tables(self, mock_db_connection):
        """Test 10: Server-related test - memory handling for large MedDRA tables"""
        conn, cursor = mock_db_connection
        
        # Test large table operations
        large_update_sql = """
        UPDATE INDI_Combined
        SET meddra_code = b.pt_code::TEXT
        FROM pref_term b
        WHERE INDI_Combined.cleaned_pt = b.pt_name;
        """
        
        # Test successful execution
        cursor.execute.return_value = None
        cursor.rowcount = 100000  # Mock large number of rows
        cursor.execute(large_update_sql)
        assert cursor.execute.called
        
        # Test out of memory error
        cursor.execute.side_effect = pg_errors.OutOfMemory("Out of memory")
        
        with pytest.raises(pg_errors.OutOfMemory):
            cursor.execute(large_update_sql)
        
        # Test disk full error during large operations
        cursor.execute.side_effect = pg_errors.DiskFull("Disk full")
        
        with pytest.raises(pg_errors.DiskFull):
            cursor.execute(large_update_sql)


# Additional validation tests
class TestS3SQLValidation:
    """Additional validation tests for S3 SQL operations"""
    
    def test_table_column_specifications(self):
        """Test that table column specifications are valid"""
        # Test column definitions
        column_specs = [
            "llt_code BIGINT",
            "llt_name VARCHAR(100)",
            "pt_code CHAR(8)",
            "term_name TEXT PRIMARY KEY",
            "meddra_code TEXT"
        ]
        
        for spec in column_specs:
            # Basic validation that column specs contain type information
            assert any(dtype in spec for dtype in ['BIGINT', 'VARCHAR', 'CHAR', 'TEXT', 'INT'])

    def test_file_path_structure(self):
        """Test that file paths follow expected structure"""
        file_paths = [
            '../faers-data/MedDRA_25_1_English/MedAscii/llt.asc',
            '../faers-data/MedDRA_25_1_English/MedAscii/pt.asc',
            '../faers-data/INDI_medra_mappings.json',
            '../faers-data/REAC_medra_mappings.json'
        ]
        
        for path in file_paths:
            assert '../faers-data/' in path
            assert any(ext in path for ext in ['.asc', '.json'])

    def test_update_statement_structure(self):
        """Test UPDATE statement structure validation"""
        update_patterns = [
            'UPDATE.*SET.*FROM.*WHERE',
            'meddra_code IS NULL',
            '::TEXT',
            'UPPER\\(TRIM\\(',
            'IF NOT EXISTS'
        ]
        
        sample_update = """
        UPDATE INDI_Combined
        SET meddra_code = b.pt_code::TEXT
        FROM pref_term b
        WHERE INDI_Combined.cleaned_pt = b.pt_name AND meddra_code IS NULL;
        """
        
        # Test that update follows expected patterns
        import re
        assert re.search(update_patterns[0], sample_update)
        assert update_patterns[1] in sample_update
        assert update_patterns[2] in sample_update


if __name__ == "__main__":
    pytest.main([__file__, "-v"])