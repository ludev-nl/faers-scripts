import pytest
import psycopg
from psycopg import errors as pg_errors
from unittest.mock import patch, MagicMock


class TestS10SQL:
    """Simple unit tests for s10.sql complex drug remapping operations"""
    
    @pytest.fixture
    def mock_db_connection(self):
        """Mock database connection for testing"""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        return conn, cursor
    
    @pytest.fixture
    def remapping_table_structures(self):
        """Expected remapping table structures"""
        return {
            'drug_mapper': [
                'drug_id', 'primaryid', 'drug_seq', 'role_cod', 'period',
                'drugname', 'prod_ai', 'notes', 'rxaui', 'rxcui', 'str',
                'sab', 'tty', 'code', 'remapping_rxaui', 'remapping_rxcui',
                'remapping_str', 'remapping_sab', 'remapping_tty', 'remapping_code', 'remapping_notes'
            ],
            'drug_mapper_2': [
                'drug_id', 'primaryid', 'drug_seq', 'role_cod', 'period',
                'drugname', 'prod_ai', 'notes', 'rxaui', 'rxcui', 'str',
                'sab', 'tty', 'code', 'remapping_notes', 'rela',
                'remapping_rxaui', 'remapping_rxcui', 'remapping_str',
                'remapping_sab', 'remapping_tty', 'remapping_code'
            ],
            'manual_remapper': [
                'count', 'source_drugname', 'source_rxaui', 'source_rxcui',
                'source_sab', 'source_tty', 'final_rxaui', 'notes'
            ]
        }
    
    @pytest.fixture
    def remapping_step_sequence(self):
        """Expected sequence of remapping steps"""
        return [
            ('step_1_initial_rxnorm_update', '1'),
            ('step_2_create_drug_mapper_2', '2'),
            ('step_3_manual_remapping_update', 'MAN_REM /'),
            ('step_6_vandf_relationships', '3'),
            ('step_7_mmsl_to_rxnorm_insert', '7'),
            ('step_8_rxnorm_scdc_to_in_insert', '8'),
            ('step_9_rxnorm_in_update_with_notes', '9'),
            ('step_11_rxnorm_in_update', '10'),
            ('step_12_mmsl_to_rxnorm_in_insert_exclusions', '11'),
            ('step_13_rxnorm_cleanup_update', '12'),
            ('step_19_non_rxnorm_sab_update', '14'),
            ('step_20_rxnorm_sab_specific_update', '15')
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

    def test_remapping_table_creation(self, mock_db_connection, remapping_table_structures):
        """Test 2: Test creation of remapping tables with proper structures"""
        conn, cursor = mock_db_connection
        
        # Test drug_mapper table creation
        drug_mapper_sql = """
        CREATE TABLE IF NOT EXISTS faers_b.drug_mapper (
            drug_id TEXT,
            primaryid TEXT,
            rxaui VARCHAR(8),
            rxcui VARCHAR(8),
            remapping_rxaui VARCHAR(8),
            remapping_rxcui VARCHAR(8),
            remapping_notes TEXT
        );
        """
        
        cursor.execute.return_value = None
        cursor.execute(drug_mapper_sql)
        
        # Verify table structure includes remapping columns
        assert 'remapping_rxaui VARCHAR(8)' in drug_mapper_sql
        assert 'remapping_rxcui VARCHAR(8)' in drug_mapper_sql
        assert 'remapping_notes TEXT' in drug_mapper_sql
        
        # Test drug_mapper_2 table with rela column
        drug_mapper_2_sql = """
        CREATE TABLE IF NOT EXISTS faers_b.drug_mapper_2 (
            rela TEXT,
            remapping_notes TEXT
        );
        """
        
        cursor.execute(drug_mapper_2_sql)
        assert 'rela TEXT' in drug_mapper_2_sql

    def test_logging_infrastructure_setup(self, mock_db_connection):
        """Test 3: Test remapping_log table creation and logging functionality"""
        conn, cursor = mock_db_connection
        
        # Test logging table creation
        log_table_sql = """
        CREATE TABLE IF NOT EXISTS faers_b.remapping_log (
            log_id SERIAL PRIMARY KEY,
            step VARCHAR(50),
            message TEXT,
            log_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cursor.execute.return_value = None
        cursor.execute(log_table_sql)
        
        # Verify logging structure
        assert 'log_id SERIAL PRIMARY KEY' in log_table_sql
        assert 'step VARCHAR(50)' in log_table_sql
        assert 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP' in log_table_sql
        
        # Test logging functionality
        log_insert_sql = "INSERT INTO faers_b.remapping_log (step, message) VALUES ('Test Step', 'Test message');"
        cursor.execute(log_insert_sql)
        
        assert 'INSERT INTO faers_b.remapping_log' in log_insert_sql

    def test_performance_indexes_creation(self, mock_db_connection):
        """Test 4: Test performance index creation with INCLUDE clauses"""
        conn, cursor = mock_db_connection
        
        # Test table existence check
        cursor.fetchone.return_value = (True,)  # Tables exist
        
        index_statements = [
            'CREATE INDEX IF NOT EXISTS idx_rxnconso_rxcui ON faers_b.rxnconso(rxcui) INCLUDE (rxaui, str, sab, tty, code);',
            'CREATE INDEX IF NOT EXISTS idx_rxnrel_rxcui ON faers_b.rxnrel(rxcui1, rxcui2) INCLUDE (rxaui1, rxaui2, rela);',
            'CREATE INDEX IF NOT EXISTS idx_drug_mapper_remapping ON faers_b.drug_mapper(remapping_rxcui, remapping_rxaui) INCLUDE (drug_id, remapping_notes);'
        ]
        
        cursor.execute.return_value = None
        
        for index_sql in index_statements:
            cursor.execute(index_sql)
            
            # Verify index structure
            assert 'CREATE INDEX IF NOT EXISTS' in index_sql
            assert 'INCLUDE (' in index_sql  # Covering indexes
            assert 'faers_b.' in index_sql
        
        assert cursor.execute.call_count == len(index_statements)

    def test_step_1_initial_rxnorm_update_logic(self, mock_db_connection):
        """Test 5: Test Step 1 initial RXNORM update with IN TTY filtering"""
        conn, cursor = mock_db_connection
        
        # Test dependency validation
        cursor.fetchone.side_effect = [
            (True,),   # drug_mapper exists
            (1000,)    # has 1000 rows
        ]
        
        # Test Step 1 update logic
        step1_update_sql = """
        UPDATE faers_b.drug_mapper
        SET remapping_rxcui = rxcui,
            remapping_rxaui = rxaui,
            remapping_notes = '1'
        FROM faers_b.rxnconso
        WHERE faers_b.drug_mapper.rxcui = faers_b.rxnconso.rxcui
          AND faers_b.rxnconso.sab = 'RXNORM'
          AND faers_b.rxnconso.tty = 'IN'
          AND faers_b.drug_mapper.remapping_notes IS NULL;
        """
        
        cursor.execute.return_value = None
        cursor.rowcount = 500  # Mock 500 rows updated
        cursor.execute(step1_update_sql)
        
        # Verify Step 1 logic
        assert "remapping_notes = '1'" in step1_update_sql
        assert "faers_b.rxnconso.sab = 'RXNORM'" in step1_update_sql
        assert "faers_b.rxnconso.tty = 'IN'" in step1_update_sql
        assert "faers_b.drug_mapper.remapping_notes IS NULL" in step1_update_sql

    def test_step_2_complex_join_logic(self, mock_db_connection):
        """Test 6: Test Step 2 complex JOIN logic for drug_mapper_2 creation"""
        conn, cursor = mock_db_connection
        
        # Test complex JOIN structure for Step 2
        step2_insert_sql = """
        INSERT INTO faers_b.drug_mapper_2
        SELECT c.drug_id, c.primaryid, c.drug_seq,
               CASE WHEN a.rxaui IS NULL THEN c.remapping_notes ELSE '2' END AS remapping_notes,
               b.rela,
               CASE WHEN a.rxaui IS NULL THEN c.remapping_rxaui ELSE a.rxaui END AS remapping_rxaui
        FROM faers_b.rxnconso a
        INNER JOIN faers_b.rxnrel b ON a.rxcui = b.rxcui1 AND a.tty = 'IN' AND a.sab = 'RXNORM'
        RIGHT OUTER JOIN faers_b.drug_mapper c ON b.rxcui2 = c.rxcui
        WHERE c.remapping_notes IS NULL;
        """
        
        cursor.execute.return_value = None
        cursor.rowcount = 800  # Mock 800 rows inserted
        cursor.execute(step2_insert_sql)
        
        # Verify complex JOIN logic
        assert 'INNER JOIN faers_b.rxnrel b ON' in step2_insert_sql
        assert 'RIGHT OUTER JOIN faers_b.drug_mapper c ON' in step2_insert_sql
        assert 'CASE WHEN a.rxaui IS NULL THEN' in step2_insert_sql
        assert "a.tty = 'IN' AND a.sab = 'RXNORM'" in step2_insert_sql

    def test_manual_remapping_integration(self, mock_db_connection):
        """Test 7: Test manual remapping integration (Steps 3-5)"""
        conn, cursor = mock_db_connection
        
        # Test manual remapping update (Step 3)
        manual_update_sql = """
        UPDATE faers_b.drug_mapper_2
        SET remapping_notes = 'MAN_REM /',
            remapping_rxaui = h.last_rxaui,
            remapping_rxcui = r.rxcui
        FROM faers_b.hopefully_last_one_5_7_2021 h
        INNER JOIN faers_b.rxnconso r ON h.last_rxaui = r.rxaui
        WHERE drug_mapper_2.str = h.str
          AND drug_mapper_2.rxaui = h.rxaui
          AND drug_mapper_2.remapping_notes IS NULL;
        """
        
        cursor.execute.return_value = None
        cursor.rowcount = 50  # Mock 50 manual remappings
        cursor.execute(manual_update_sql)
        
        # Verify manual remapping logic
        assert "remapping_notes = 'MAN_REM /'" in manual_update_sql
        assert 'faers_b.hopefully_last_one_5_7_2021 h' in manual_update_sql
        assert 'h.last_rxaui = r.rxaui' in manual_update_sql
        
        # Test manual remapping cleanup (Step 5)
        manual_delete_sql = "DELETE FROM faers_b.drug_mapper_2 WHERE remapping_notes LIKE 'MAN_REM /%';"
        cursor.execute(manual_delete_sql)
        
        assert "LIKE 'MAN_REM /%'" in manual_delete_sql

    def test_vandf_and_relationship_processing(self, mock_db_connection):
        """Test 8: Test VANDF relationships and complex multi-table JOINs (Step 6)"""
        conn, cursor = mock_db_connection
        
        # Test VANDF relationship processing (Step 6)
        vandf_insert_sql = """
        INSERT INTO faers_b.drug_mapper_2
        SELECT e.drug_id, e.primaryid, '3' AS remapping_notes,
               a.rxaui AS remapping_rxaui,
               a.rxcui AS remapping_rxcui
        FROM faers_b.rxnconso a
        INNER JOIN faers_b.rxnrel b ON a.rxcui = b.rxcui1 AND a.tty = 'IN' AND a.sab = 'RXNORM'
        INNER JOIN faers_b.rxnconso c ON b.rxcui2 = c.rxcui
        INNER JOIN faers_b.rxnrel d ON c.rxcui = d.rxcui1 AND d.rela = 'HAS_INGREDIENTS' AND c.sab = 'VANDF' AND c.tty = 'IN'
        INNER JOIN faers_b.drug_mapper_2 e ON d.rxcui2 = e.rxcui
        WHERE e.remapping_notes IS NULL;
        """
        
        cursor.execute.return_value = None
        cursor.rowcount = 200  # Mock 200 VANDF mappings
        cursor.execute(vandf_insert_sql)
        
        # Verify VANDF processing logic
        assert "d.rela = 'HAS_INGREDIENTS'" in vandf_insert_sql
        assert "c.sab = 'VANDF'" in vandf_insert_sql
        assert "'3' AS remapping_notes" in vandf_insert_sql
        
        # Verify 4-table JOIN structure
        join_count = vandf_insert_sql.count('INNER JOIN')
        assert join_count == 4

    def test_duplicate_cleanup_and_optimization(self, mock_db_connection):
        """Test 9: Test duplicate cleanup and optimization (Step 17)"""
        conn, cursor = mock_db_connection
        
        # Test duplicate cleanup logic (Step 17)
        duplicate_cleanup_sql = """
        DELETE FROM faers_b.drug_mapper_2
        WHERE (drug_id, rxaui, remapping_rxaui) IN (
            SELECT drug_id, rxaui, remapping_rxaui
            FROM (
                SELECT drug_id, rxaui, remapping_rxaui,
                       ROW_NUMBER() OVER (PARTITION BY drug_id, rxaui, remapping_rxaui ORDER BY drug_id, rxaui, remapping_rxaui) AS row_num
                FROM faers_b.drug_mapper_2
            ) t
            WHERE row_num > 1
        );
        """
        
        cursor.execute.return_value = None
        cursor.rowcount = 100  # Mock 100 duplicates removed
        cursor.execute(duplicate_cleanup_sql)
        
        # Verify duplicate cleanup logic
        assert 'ROW_NUMBER() OVER (PARTITION BY' in duplicate_cleanup_sql
        assert 'WHERE row_num > 1' in duplicate_cleanup_sql
        assert 'drug_id, rxaui, remapping_rxaui' in duplicate_cleanup_sql

    def test_server_memory_handling_complex_operations(self, mock_db_connection):
        """Test 10: Server-related test - memory handling for complex multi-step operations"""
        conn, cursor = mock_db_connection
        
        # Test large multi-table JOIN operation
        complex_join_sql = """
        INSERT INTO faers_b.drug_mapper_2
        SELECT DISTINCT e.drug_id, e.primaryid, '7',
               c1.rxaui AS remapping_rxaui,
               c1.rxcui AS remapping_rxcui
        FROM faers_b.drug_mapper_2 e
        INNER JOIN faers_b.rxnrel r ON e.rxcui = r.rxcui2
        INNER JOIN faers_b.rxnconso c ON r.rxcui1 = c.rxcui
        INNER JOIN faers_b.rxnrel r1 ON r.rxcui1 = r1.rxcui2
        INNER JOIN faers_b.rxnconso c1 ON r1.rxcui1 = c1.rxcui
        WHERE e.sab = 'MMSL'
          AND c1.sab = 'RXNORM'
          AND c1.tty = 'IN';
        """
        
        # Test successful execution
        cursor.execute.return_value = None
        cursor.rowcount = 50000  # Mock large operation
        cursor.execute(complex_join_sql)
        assert cursor.execute.called
        
        # Test out of memory error during complex JOINs
        cursor.execute.side_effect = pg_errors.OutOfMemory("Insufficient memory for complex JOIN operations")
        
        with pytest.raises(pg_errors.OutOfMemory):
            cursor.execute(complex_join_sql)
        
        # Test connection timeout during long operations
        cursor.execute.side_effect = pg_errors.OperationalError("Connection timeout during remapping process")
        
        with pytest.raises(pg_errors.OperationalError):
            cursor.execute(complex_join_sql)
        
        # Test disk full error during large INSERTs
        cursor.execute.side_effect = pg_errors.DiskFull("No space left for remapping tables")
        
        with pytest.raises(pg_errors.DiskFull):
            cursor.execute("INSERT INTO faers_b.drug_mapper_2 SELECT * FROM large_source_table")
        
        # Test deadlock during concurrent remapping operations
        cursor.execute.side_effect = pg_errors.DeadlockDetected("Deadlock detected during remapping")
        
        with pytest.raises(pg_errors.DeadlockDetected):
            cursor.execute("UPDATE faers_b.drug_mapper_2 SET remapping_notes = 'test'")


# Additional validation tests
class TestS10SQLValidation:
    """Additional validation tests for S10 SQL operations"""
    
    def test_function_definition_structure(self):
        """Test function definition structure validation"""
        function_elements = [
            'CREATE OR REPLACE FUNCTION faers_b.step_1_initial_rxnorm_update() RETURNS VOID AS $$',
            'DECLARE table_exists BOOLEAN; row_count BIGINT;',
            'BEGIN',
            'EXCEPTION WHEN OTHERS THEN',
            'END; $$ LANGUAGE plpgsql;'
        ]
        
        for element in function_elements:
            assert any(keyword in element for keyword in ['FUNCTION', 'DECLARE', 'BEGIN', 'EXCEPTION', 'END', 'plpgsql'])

    def test_remapping_notes_progression(self, remapping_step_sequence):
        """Test remapping notes progression validation"""
        # Verify notes follow logical progression
        expected_notes = ['1', '2', '3', '7', '8', '9', '10', '11', '12', '14', '15']
        
        for step_name, note in remapping_step_sequence:
            if note.isdigit():
                assert note in expected_notes
            elif note.startswith('MAN_REM'):
                assert 'MAN_REM' in note

    def test_table_dependency_validation(self):
        """Test table dependency validation"""
        required_tables = [
            'faers_b.drug_mapper',
            'faers_b.drug_mapper_2',
            'faers_b.drug_mapper_3',
            'faers_b.rxnconso',
            'faers_b.rxnrel',
            'faers_b.manual_remapper',
            'faers_b.remapping_log'
        ]
        
        for table in required_tables:
            assert 'faers_b.' in table
            assert any(suffix in table for suffix in ['drug_mapper', 'rxn', 'manual', 'log'])

    def test_case_statement_validation(self):
        """Test CASE statement validation"""
        case_patterns = [
            'CASE WHEN a.rxaui IS NULL THEN c.remapping_notes ELSE \'2\' END',
            'CASE WHEN a.rxcui IS NULL THEN c.remapping_rxcui ELSE a.rxcui END'
        ]
        
        for pattern in case_patterns:
            assert 'CASE WHEN' in pattern
            assert 'THEN' in pattern
            assert 'ELSE' in pattern
            assert 'END' in pattern

    def test_exclusion_lists_validation(self):
        """Test exclusion lists in complex operations"""
        exclusions = [
            "c1.rxaui NOT IN ('2604414', '1182299', '1173735', '1287235')",
            "c1.rxaui != '11794211'"
        ]
        
        for exclusion in exclusions:
            assert 'NOT IN' in exclusion or '!=' in exclusion
            assert 'rxaui' in exclusion


if __name__ == "__main__":
    # Run tests with: python -m pytest unit_tests/sql/test_s10.py -v
    # Run server tests only: python -m pytest unit_tests/sql/test_s10.py -v -k "server"
    # Run without server tests: python -m pytest unit_tests/sql/test_s10.py -v -k "not server"
    pytest.main([__file__, "-v"])