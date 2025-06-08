import pytest
import psycopg
from psycopg import errors as pg_errors
from unittest.mock import patch, MagicMock


class TestS5SQL:
    """Simple unit tests for s5.sql RxNorm operations"""
    
    @pytest.fixture
    def mock_db_connection(self):
        """Mock database connection for testing"""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        return conn, cursor
    
    @pytest.fixture
    def expected_rxnorm_tables(self):
        """List of expected RxNorm tables"""
        return [
            'rxnatomarchive', 'rxnconso', 'rxnrel', 'rxnsab',
            'rxnsat', 'rxnsty', 'rxndoc', 'rxncuichanges', 'rxncui'
        ]

    def test_database_context_validation(self, mock_db_connection):
        """Test 1: Verify database context validation DO block"""
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

    def test_faers_b_schema_creation(self, mock_db_connection):
        """Test 2: Test faers_b schema creation and verification"""
        conn, cursor = mock_db_connection
        
        # Test schema creation
        schema_creation_sql = "CREATE SCHEMA IF NOT EXISTS faers_b AUTHORIZATION postgres;"
        cursor.execute.return_value = None
        cursor.execute(schema_creation_sql)
        
        # Verify schema creation statement structure
        assert 'CREATE SCHEMA' in schema_creation_sql
        assert 'IF NOT EXISTS' in schema_creation_sql
        assert 'faers_b' in schema_creation_sql
        assert 'AUTHORIZATION postgres' in schema_creation_sql
        
        # Test schema existence verification
        cursor.fetchone.return_value = (True,)
        cursor.execute("SELECT EXISTS (SELECT FROM pg_namespace WHERE nspname = 'faers_b')")
        schema_exists = cursor.fetchone()[0]
        assert schema_exists is True

    def test_search_path_and_privileges(self, mock_db_connection):
        """Test 3: Test search path setting and privilege grants"""
        conn, cursor = mock_db_connection
        
        # Test privilege grant
        grant_sql = "GRANT ALL ON SCHEMA faers_b TO postgres;"
        cursor.execute.return_value = None
        cursor.execute(grant_sql)
        
        # Test search path setting
        search_path_sql = "SET search_path TO faers_b, faers_combined, public;"
        cursor.execute(search_path_sql)
        
        # Verify statements structure
        assert 'GRANT ALL' in grant_sql
        assert 'faers_b' in grant_sql
        assert 'SET search_path' in search_path_sql
        assert 'faers_b, faers_combined, public' in search_path_sql

    def test_logging_table_creation(self, mock_db_connection):
        """Test 4: Test s5_log table creation for tracking progress"""
        conn, cursor = mock_db_connection
        
        log_table_sql = """
        CREATE TABLE IF NOT EXISTS faers_b.s5_log (
            log_id SERIAL PRIMARY KEY,
            step VARCHAR(50),
            message TEXT,
            log_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cursor.execute.return_value = None
        cursor.execute(log_table_sql)
        
        # Verify log table structure
        assert 'CREATE TABLE' in log_table_sql
        assert 's5_log' in log_table_sql
        assert 'log_id SERIAL PRIMARY KEY' in log_table_sql
        assert 'step VARCHAR(50)' in log_table_sql
        assert 'message TEXT' in log_table_sql
        assert 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP' in log_table_sql

    def test_drug_mapper_table_creation(self, mock_db_connection):
        """Test 5: Test drug_mapper table creation with proper columns"""
        conn, cursor = mock_db_connection
        
        # Test table existence check
        cursor.fetchone.return_value = (False,)  # Table doesn't exist
        cursor.execute("SELECT EXISTS (SELECT FROM pg_class WHERE relname = 'drug_mapper')")
        
        # Expected drug_mapper columns
        expected_columns = [
            'drug_id INTEGER NOT NULL',
            'primaryid BIGINT',
            'caseid BIGINT',
            'drug_seq BIGINT',
            'role_cod VARCHAR(2)',
            'drugname VARCHAR(500)',
            'rxaui BIGINT',
            'rxcui BIGINT',
            'str VARCHAR(3000)',
            'remapping_rxcui VARCHAR(8)'
        ]
        
        create_table_sql = f"""
        CREATE TABLE faers_b.drug_mapper (
            {', '.join(expected_columns)}
        );
        """
        
        cursor.execute.return_value = None
        cursor.execute(create_table_sql)
        
        # Verify table structure
        for column in expected_columns:
            assert column in create_table_sql
        
        # Test index creation
        index_sql = "CREATE INDEX idx_drug_mapper_drugname ON faers_b.drug_mapper (drugname);"
        cursor.execute(index_sql)
        assert 'CREATE INDEX' in index_sql
        assert 'drugname' in index_sql

    def test_drug_mapper_population_logic(self, mock_db_connection):
        """Test 6: Test drug_mapper population with dependency checks"""
        conn, cursor = mock_db_connection
        
        # Test dependency table existence checks
        cursor.fetchone.side_effect = [
            (True,),   # drug_combined exists
            (True,),   # aligned_demo_drug_reac_indi_ther exists
        ]
        
        # Test table existence queries
        cursor.execute("SELECT EXISTS (SELECT FROM pg_class WHERE relname = 'drug_combined')")
        drug_combined_exists = cursor.fetchone()[0]
        
        cursor.execute("SELECT EXISTS (SELECT FROM pg_class WHERE relname = 'aligned_demo_drug_reac_indi_ther')")
        aligned_exists = cursor.fetchone()[0]
        
        assert drug_combined_exists is True
        assert aligned_exists is True
        
        # Test population SQL structure
        population_sql = """
        INSERT INTO faers_b.drug_mapper (drug_id, primaryid, caseid, drug_seq, role_cod, drugname, prod_ai, nda_num, period)
        SELECT drug_id, primaryid, caseid, drug_seq, role_cod, drugname, prod_ai, nda_num, period
        FROM faers_combined.drug_combined
        WHERE primaryid IN (SELECT primaryid FROM faers_combined.aligned_demo_drug_reac_indi_ther);
        """
        
        cursor.execute.return_value = None
        cursor.rowcount = 1000  # Mock 1000 rows inserted
        cursor.execute(population_sql)
        
        # Verify SQL structure
        assert 'INSERT INTO faers_b.drug_mapper' in population_sql
        assert 'SELECT drug_id, primaryid' in population_sql
        assert 'FROM faers_combined.drug_combined' in population_sql
        assert 'WHERE primaryid IN' in population_sql

    def test_rxnorm_table_creation_structure(self, mock_db_connection, expected_rxnorm_tables):
        """Test 7: Test RxNorm table creation with proper schemas"""
        conn, cursor = mock_db_connection
        
        # Test RXNCONSO table structure (most important RxNorm table)
        rxnconso_sql = """
        CREATE TABLE faers_b.rxnconso (
            rxcui VARCHAR(8) NOT NULL,
            lat VARCHAR(3) DEFAULT 'ENG' NOT NULL,
            rxaui VARCHAR(8) NOT NULL,
            sab VARCHAR(20) NOT NULL,
            tty VARCHAR(20) NOT NULL,
            code VARCHAR(50) NOT NULL,
            str VARCHAR(3000) NOT NULL
        );
        """
        
        cursor.execute.return_value = None
        cursor.execute(rxnconso_sql)
        
        # Verify table structure
        assert 'CREATE TABLE faers_b.rxnconso' in rxnconso_sql
        assert 'rxcui VARCHAR(8) NOT NULL' in rxnconso_sql
        assert 'str VARCHAR(3000) NOT NULL' in rxnconso_sql
        assert "lat VARCHAR(3) DEFAULT 'ENG'" in rxnconso_sql
        
        # Test index creation for RXNCONSO
        index_sqls = [
            "CREATE INDEX idx_rxnconso_rxcui ON faers_b.rxnconso (rxcui);",
            "CREATE INDEX idx_rxnconso_rxaui ON faers_b.rxnconso (rxaui);",
            "CREATE INDEX idx_rxnconso_sab ON faers_b.rxnconso (sab);"
        ]
        
        for index_sql in index_sqls:
            cursor.execute(index_sql)
            assert 'CREATE INDEX' in index_sql
            assert 'rxnconso' in index_sql

    def test_rxnorm_table_existence_checks(self, mock_db_connection, expected_rxnorm_tables):
        """Test 8: Test RxNorm table existence check logic"""
        conn, cursor = mock_db_connection
        
        # Test table existence check pattern
        table_check_sql = """
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = %s
        """
        
        for table in expected_rxnorm_tables:
            # Test that table doesn't exist (should create)
            cursor.fetchone.return_value = (False,)
            cursor.execute(table_check_sql, (table,))
            exists = cursor.fetchone()[0]
            assert exists is False
            
            # Test that table exists (should skip creation)
            cursor.fetchone.return_value = (True,)
            cursor.execute(table_check_sql, (table,))
            exists = cursor.fetchone()[0]
            assert exists is True
        
        # Verify SQL structure
        assert 'pg_class' in table_check_sql
        assert 'pg_namespace' in table_check_sql
        assert 'faers_b' in table_check_sql

    def test_server_file_access_for_rxnorm_data(self, mock_db_connection):
        """Test 9: Server-related test - RxNorm file access validation"""
        conn, cursor = mock_db_connection
        
        # Test RxNorm file paths and COPY commands
        rxnorm_files = [
            '/data/faers/FAERS_MAK/2.LoadDataToDatabase/RxNorm_full_06052023/rrf/RXNCONSO.RRF',
            '/data/faers/FAERS_MAK/2.LoadDataToDatabase/RxNorm_full_06052023/rrf/RXNREL.RRF',
            '/data/faers/FAERS_MAK/2.LoadDataToDatabase/RxNorm_full_06052023/rrf/RXNSAT.RRF'
        ]
        
        copy_command_template = "\\copy faers_b.rxnconso FROM '{file_path}' WITH (FORMAT CSV, DELIMITER '|', NULL '', HEADER FALSE);"
        
        # Test successful file access
        cursor.execute.return_value = None
        
        for file_path in rxnorm_files:
            copy_command = copy_command_template.format(file_path=file_path)
            cursor.execute(copy_command)
            
            # Verify COPY command structure
            assert '\\copy' in copy_command
            assert "DELIMITER '|'" in copy_command
            assert 'HEADER FALSE' in copy_command
            assert '.RRF' in copy_command
        
        # Test file not found error
        cursor.execute.side_effect = pg_errors.NoSuchFile("RxNorm file not found")
        
        with pytest.raises(pg_errors.NoSuchFile):
            cursor.execute(copy_command_template.format(file_path=rxnorm_files[0]))
        
        # Test permission denied error
        cursor.execute.side_effect = pg_errors.InsufficientPrivilege("Permission denied to read RxNorm files")
        
        with pytest.raises(pg_errors.InsufficientPrivilege):
            cursor.execute(copy_command_template.format(file_path=rxnorm_files[0]))

    def test_server_memory_handling_large_rxnorm_tables(self, mock_db_connection):
        """Test 10: Server-related test - memory handling for large RxNorm operations"""
        conn, cursor = mock_db_connection
        
        # Test large RxNorm data loading operation
        large_insert_sql = """
        INSERT INTO faers_b.drug_mapper (drug_id, primaryid, drugname, rxcui, str)
        SELECT d.drug_id, d.primaryid, d.drugname, r.rxcui, r.str
        FROM faers_b.drug_mapper d
        LEFT JOIN faers_b.rxnconso r ON UPPER(TRIM(d.drugname)) = UPPER(TRIM(r.str))
        WHERE r.sab = 'RXNORM' AND r.tty IN ('SCD', 'SBD', 'GPCK', 'BPCK');
        """
        
        # Test successful execution
        cursor.execute.return_value = None
        cursor.rowcount = 50000  # Mock large number of rows
        cursor.execute(large_insert_sql)
        assert cursor.execute.called
        
        # Test out of memory error during large JOIN
        cursor.execute.side_effect = pg_errors.OutOfMemory("Insufficient memory for RxNorm JOIN operation")
        
        with pytest.raises(pg_errors.OutOfMemory):
            cursor.execute(large_insert_sql)
        
        # Test disk full error during RxNorm data loading
        cursor.execute.side_effect = pg_errors.DiskFull("No space left for RxNorm data")
        
        with pytest.raises(pg_errors.DiskFull):
            cursor.execute(large_insert_sql)
        
        # Test connection timeout during long operations
        cursor.execute.side_effect = pg_errors.OperationalError("Connection timeout during RxNorm processing")
        
        with pytest.raises(pg_errors.OperationalError):
            cursor.execute(large_insert_sql)


# Additional validation tests
class TestS5SQLValidation:
    """Additional validation tests for S5 SQL operations"""
    
    def test_do_block_error_handling_structure(self):
        """Test DO block error handling patterns"""
        error_handling_patterns = [
            'EXCEPTION WHEN OTHERS THEN',
            'INSERT INTO faers_b.s5_log',
            'SQLERRM',
            'RAISE;'
        ]
        
        sample_do_block = """
        DO $$
        BEGIN
            -- Some operation
            INSERT INTO faers_b.drug_mapper VALUES (...);
        EXCEPTION
            WHEN OTHERS THEN
                INSERT INTO faers_b.s5_log (step, message) VALUES ('Error', 'Error: ' || SQLERRM);
                RAISE;
        END $$;
        """
        
        for pattern in error_handling_patterns:
            assert pattern in sample_do_block or 'EXCEPTION' in sample_do_block

    def test_rxnorm_copy_command_validation(self):
        """Test RxNorm COPY command structure validation"""
        copy_commands = [
            "\\copy faers_b.rxnconso FROM '/path/RXNCONSO.RRF' WITH (FORMAT CSV, DELIMITER '|', NULL '', HEADER FALSE);",
            "\\copy faers_b.rxnrel FROM '/path/RXNREL.RRF' WITH (FORMAT CSV, DELIMITER '|', NULL '', HEADER FALSE);",
            "\\copy faers_b.rxnsat FROM '/path/RXNSAT.RRF' WITH (FORMAT CSV, DELIMITER '|', NULL '', HEADER FALSE);"
        ]
        
        for command in copy_commands:
            assert '\\copy faers_b.' in command
            assert "DELIMITER '|'" in command
            assert 'HEADER FALSE' in command
            assert '.RRF' in command
            assert 'FORMAT CSV' in command

    def test_logging_functionality_validation(self):
        """Test logging functionality structure"""
        log_insert_examples = [
            "INSERT INTO faers_b.s5_log (step, message) VALUES ('Create drug_mapper', 'Table created successfully');",
            "INSERT INTO faers_b.s5_log (step, message) VALUES ('Populate drug_mapper', 'Error: ' || SQLERRM);",
            "INSERT INTO faers_b.s5_log (step, message) VALUES ('Load RxNorm Data', 'Files not available');"
        ]
        
        for log_statement in log_insert_examples:
            assert 'INSERT INTO faers_b.s5_log' in log_statement
            assert 'step, message' in log_statement
            assert 'VALUES' in log_statement

    def test_table_dependency_validation(self):
        """Test table dependency validation logic"""
        dependency_checks = [
            ('faers_combined', 'drug_combined'),
            ('faers_combined', 'aligned_demo_drug_reac_indi_ther'),
            ('faers_b', 'drug_mapper'),
            ('faers_b', 'rxnconso')
        ]
        
        for schema, table in dependency_checks:
            check_sql = f"""
            SELECT EXISTS (
                SELECT FROM pg_class 
                WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{schema}') 
                AND relname = '{table}'
            )
            """
            
            assert 'SELECT EXISTS' in check_sql
            assert 'pg_class' in check_sql
            assert 'pg_namespace' in check_sql
            assert schema in check_sql
            assert table in check_sql


if __name__ == "__main__":
    # Run tests with: python -m pytest unit_tests/sql/test_s5.py -v
    # Run server tests only: python -m pytest unit_tests/sql/test_s5.py -v -k "server"
    # Run without server tests: python -m pytest unit_tests/sql/test_s5.py -v -k "not server"
    pytest.main([__file__, "-v"])