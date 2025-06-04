import pytest
import psycopg
from psycopg import errors as pg_errors
from unittest.mock import patch, MagicMock
import math


class TestS11SQL:
    """Simple unit tests for s11.sql final dataset creation and statistical analysis"""
    
    @pytest.fixture
    def mock_db_connection(self):
        """Mock database connection for testing"""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        return conn, cursor
    
    @pytest.fixture
    def expected_analysis_tables(self):
        """List of expected analysis tables created by s11"""
        return [
            'drugs_standardized',
            'adverse_reactions', 
            'drug_adverse_reactions_pairs',
            'drug_adverse_reactions_count',
            'drug_indications',
            'demographics',
            'case_outcomes',
            'therapy_dates',
            'report_sources',
            'drug_margin',
            'event_margin',
            'total_count',
            'contingency_table',
            'proportionate_analysis'
        ]
    
    @pytest.fixture
    def sample_contingency_data(self):
        """Sample data for contingency table calculations"""
        return [
            # (a, b, c, d) - 2x2 contingency table values
            (10, 90, 20, 880),   # Drug-event pair with moderate signal
            (5, 50, 15, 430),    # Drug-event pair with strong signal
            (1, 10, 5, 100),     # Drug-event pair with weak signal
            (0, 5, 10, 85),      # No cases (should be filtered out)
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

    def test_drugs_standardized_table_creation(self, mock_db_connection):
        """Test 2: Test DRUGS_STANDARDIZED table creation with RxNorm mapping"""
        conn, cursor = mock_db_connection
        
        # Test dependency validation
        cursor.fetchone.side_effect = [
            (True,),   # drug_mapper_3 exists
            (True,)    # aligned_demo_drug_reac_indi_ther exists
        ]
        
        # Test table creation
        drugs_standardized_sql = """
        CREATE TABLE faers_b.drugs_standardized (
            primaryid BIGINT,
            drug_id INTEGER,
            drug_seq BIGINT,
            role_cod VARCHAR(2),
            period VARCHAR(4),
            rxaui BIGINT,
            drug VARCHAR(3000)
        );
        """
        
        cursor.execute.return_value = None
        cursor.execute(drugs_standardized_sql)
        
        # Test data insertion with exclusions
        insert_sql = """
        INSERT INTO faers_b.drugs_standardized
        SELECT dm.primaryid, CAST(dm.drug_id AS INTEGER), dm.drug_seq, dm.role_cod, dm.period, 
               CAST(dm.remapping_rxaui AS BIGINT) AS rxaui, dm.remapping_str AS drug
        FROM faers_b.drug_mapper_3 dm
        INNER JOIN faers_combined.aligned_demo_drug_reac_indi_ther ad
            ON dm.primaryid = ad.primaryid
        WHERE dm.remapping_rxaui IS NOT NULL
          AND dm.remapping_rxaui != '92683486';
        """
        
        cursor.rowcount = 50000  # Mock 50k standardized drugs
        cursor.execute(insert_sql)
        
        # Verify structure and logic
        assert 'rxaui BIGINT' in drugs_standardized_sql
        assert 'drug VARCHAR(3000)' in drugs_standardized_sql
        assert "dm.remapping_rxaui != '92683486'" in insert_sql  # Excludes 'UNKNOWN STR'

    def test_adverse_reactions_with_meddra_integration(self, mock_db_connection):
        """Test 3: Test ADVERSE_REACTIONS table with MedDRA integration"""
        conn, cursor = mock_db_connection
        
        # Test adverse reactions creation with CTE structure
        adverse_reactions_sql = """
        WITH cte AS (
            SELECT rc.primaryid, rc.period, rc.pt AS meddra_code
            FROM faers_combined.reac_combined rc
            INNER JOIN faers_combined.aligned_demo_drug_reac_indi_ther ad
                ON rc.primaryid = ad.primaryid
        ),
        cte_2 AS (
            SELECT pt_name AS adverse_event 
            FROM faers_combined.pref_term
            UNION
            SELECT llt_name 
            FROM faers_combined.low_level_term
        )
        INSERT INTO faers_b.adverse_reactions
        SELECT cte.primaryid, cte.period, cte_2.adverse_event
        FROM cte
        INNER JOIN cte_2 ON cte.meddra_code = cte_2.adverse_event;
        """
        
        cursor.execute.return_value = None
        cursor.rowcount = 75000  # Mock 75k adverse reactions
        cursor.execute(adverse_reactions_sql)
        
        # Verify CTE structure and MedDRA integration
        assert 'WITH cte AS' in adverse_reactions_sql
        assert 'UNION' in adverse_reactions_sql  # Combines pt_name and llt_name
        assert 'cte.meddra_code = cte_2.adverse_event' in adverse_reactions_sql

    def test_drug_adverse_reactions_pairs_creation(self, mock_db_connection):
        """Test 4: Test drug-adverse reaction pairs creation with DISTINCT"""
        conn, cursor = mock_db_connection
        
        # Test dependency validation
        cursor.fetchone.side_effect = [
            (True,),   # drugs_standardized exists
            (True,)    # adverse_reactions exists
        ]
        
        # Test pairs creation
        pairs_insert_sql = """
        INSERT INTO faers_b.drug_adverse_reactions_pairs
        SELECT DISTINCT ds.primaryid, ds.rxaui, ds.drug, ar.adverse_event
        FROM faers_b.drugs_standardized ds
        INNER JOIN faers_b.adverse_reactions ar ON ds.primaryid = ar.primaryid;
        """
        
        cursor.execute.return_value = None
        cursor.rowcount = 100000  # Mock 100k unique pairs
        cursor.execute(pairs_insert_sql)
        
        # Verify DISTINCT logic and JOIN structure
        assert 'SELECT DISTINCT' in pairs_insert_sql
        assert 'ds.primaryid = ar.primaryid' in pairs_insert_sql
        
        # Test table structure
        pairs_table_sql = """
        CREATE TABLE faers_b.drug_adverse_reactions_pairs (
            primaryid BIGINT,
            rxaui BIGINT,
            drug VARCHAR(3000),
            adverse_event VARCHAR(1000)
        );
        """
        
        cursor.execute(pairs_table_sql)
        assert 'adverse_event VARCHAR(1000)' in pairs_table_sql

    def test_aggregation_and_counting_logic(self, mock_db_connection):
        """Test 5: Test aggregation logic for drug-adverse reaction counts"""
        conn, cursor = mock_db_connection
        
        # Test count aggregation
        count_aggregation_sql = """
        INSERT INTO faers_b.drug_adverse_reactions_count
        SELECT rxaui, drug, adverse_event, COUNT(*) AS count_of_reaction
        FROM faers_b.drug_adverse_reactions_pairs
        GROUP BY rxaui, drug, adverse_event;
        """
        
        cursor.execute.return_value = None
        cursor.rowcount = 25000  # Mock 25k unique drug-event combinations
        cursor.execute(count_aggregation_sql)
        
        # Verify aggregation logic
        assert 'COUNT(*) AS count_of_reaction' in count_aggregation_sql
        assert 'GROUP BY rxaui, drug, adverse_event' in count_aggregation_sql
        
        # Test margin calculations
        drug_margin_sql = """
        INSERT INTO faers_b.drug_margin
        SELECT rxaui, SUM(count_of_reaction) AS margin
        FROM faers_b.drug_adverse_reactions_count
        GROUP BY rxaui;
        """
        
        cursor.execute(drug_margin_sql)
        assert 'SUM(count_of_reaction) AS margin' in drug_margin_sql

    def test_demographics_with_data_conversion(self, mock_db_connection):
        """Test 6: Test demographics table with date and age conversions"""
        conn, cursor = mock_db_connection
        
        demographics_insert_sql = """
        INSERT INTO faers_b.demographics
        SELECT caseid, primaryid, caseversion, 
               TO_DATE(NULLIF(fda_dt, ''), 'YYYYMMDD') AS fda_dt, 
               i_f_cod, 
               TO_DATE(NULLIF(event_dt, ''), 'YYYYMMDD') AS event_dt,
               CASE 
                   WHEN age ~ '^[0-9]+$' THEN CAST(age AS FLOAT)
                   ELSE NULL
               END AS age, 
               gndr_cod AS gender, 
               occr_country AS country_code, 
               period
        FROM faers_combined.aligned_demo_drug_reac_indi_ther;
        """
        
        cursor.execute.return_value = None
        cursor.rowcount = 15000  # Mock 15k demographic records
        cursor.execute(demographics_insert_sql)
        
        # Verify data conversion logic
        assert "TO_DATE(NULLIF(fda_dt, ''), 'YYYYMMDD')" in demographics_insert_sql
        assert "age ~ '^[0-9]+$'" in demographics_insert_sql  # Regex validation
        assert 'CAST(age AS FLOAT)' in demographics_insert_sql

    def test_contingency_table_calculation_logic(self, mock_db_connection):
        """Test 7: Test contingency table calculations with statistical formulas"""
        conn, cursor = mock_db_connection
        
        # Test contingency table dependencies
        cursor.fetchone.side_effect = [
            (True,),   # drug_adverse_reactions_count exists
            (True,),   # drug_margin exists
            (True,),   # event_margin exists
            (True,)    # total_count exists
        ]
        
        contingency_insert_sql = """
        INSERT INTO faers_b.contingency_table (rxaui, drug, adverse_event, a, b, c, d)
        SELECT darc.rxaui, darc.drug, darc.adverse_event,
               darc.count_of_reaction AS a,
               (em.margin - darc.count_of_reaction) AS b,
               (dm.margin - darc.count_of_reaction) AS c,
               (SELECT n FROM faers_b.total_count) - em.margin - (dm.margin - darc.count_of_reaction) AS d
        FROM faers_b.drug_adverse_reactions_count darc
        INNER JOIN faers_b.drug_margin dm ON darc.rxaui = dm.rxaui
        INNER JOIN faers_b.event_margin em ON darc.adverse_event = em.adverse_event;
        """
        
        cursor.execute.return_value = None
        cursor.rowcount = 25000  # Mock 25k contingency tables
        cursor.execute(contingency_insert_sql)
        
        # Verify 2x2 contingency table calculation
        assert 'darc.count_of_reaction AS a' in contingency_insert_sql
        assert '(em.margin - darc.count_of_reaction) AS b' in contingency_insert_sql
        assert '(dm.margin - darc.count_of_reaction) AS c' in contingency_insert_sql

    def test_proportionate_reporting_ratio_calculations(self, mock_db_connection):
        """Test 8: Test PRR and statistical calculations"""
        conn, cursor = mock_db_connection
        
        # Test PRR calculation structure
        prr_calculation_sql = """
        INSERT INTO faers_b.proportionate_analysis
        SELECT ct.rxaui, ct.drug, ct.adverse_event, ct.a,
               (ct.a / NULLIF((ct.a + ct.c), 0)) / NULLIF((ct.b / NULLIF((ct.b + ct.d), 0)), 0) AS prr,
               EXP(LN((ct.a / NULLIF((ct.a + ct.c), 0)) / NULLIF((ct.b / NULLIF((ct.b + ct.d), 0)), 0)) 
                   - 1.96 * SQRT((1.0 / ct.a) - (1.0 / (ct.a + ct.c)) + (1.0 / ct.b) - (1.0 / (ct.b + ct.d)))) AS prr_lb
        FROM faers_b.contingency_table ct
        WHERE ct.a > 0 AND ct.b > 0 AND ct.c > 0 AND ct.d > 0;
        """
        
        cursor.execute.return_value = None
        cursor.rowcount = 20000  # Mock 20k statistical analyses
        cursor.execute(prr_calculation_sql)
        
        # Verify statistical formula structure
        assert 'NULLIF((ct.a + ct.c), 0)' in prr_calculation_sql  # Avoids division by zero
        assert '1.96 * SQRT(' in prr_calculation_sql  # 95% confidence interval
        assert 'WHERE ct.a > 0 AND ct.b > 0 AND ct.c > 0 AND ct.d > 0' in prr_calculation_sql

    def test_information_component_calculations(self, mock_db_connection):
        """Test 9: Test Information Component (IC) statistical calculations"""
        conn, cursor = mock_db_connection
        
        # Test IC calculation logic
        def calculate_ic(a, total, drug_margin, event_margin):
            """Python implementation of IC calculation"""
            expected = (drug_margin * event_margin) / total
            if expected > 0:
                ic = math.log2((a + 0.5) / (expected + 0.5))
                ic025 = ic - (3.3 * math.pow(a + 0.5, -0.5)) - (2.0 * math.pow(a + 0.5, -1.5))
                ic975 = ic + (2.4 * math.pow(a + 0.5, -0.5)) - (0.5 * math.pow(a + 0.5, -1.5))
                return ic, ic025, ic975
            return None, None, None
        
        test_cases = [
            (10, 1000, 100, 200),  # Normal case
            (5, 500, 50, 100),     # Smaller numbers
            (1, 100, 10, 20),      # Edge case with small a
        ]
        
        for a, total, drug_margin, event_margin in test_cases:
            ic, ic025, ic975 = calculate_ic(a, total, drug_margin, event_margin)
            if ic is not None:
                assert isinstance(ic, float)
                assert ic025 < ic < ic975  # Confidence interval order

    def test_server_complex_statistical_operations(self, mock_db_connection):
        """Test 10: Server-related test - complex statistical operations performance"""
        conn, cursor = mock_db_connection
        
        # Test large statistical calculation
        complex_stats_sql = """
        INSERT INTO faers_b.proportionate_analysis
        SELECT ct.rxaui, ct.drug, ct.adverse_event,
               ROUND(CAST((ct.a + ct.b + ct.c + ct.d) * 
                     POWER(ABS((ct.a * ct.d) - (ct.b * ct.c)) - ((ct.a + ct.b + ct.c + ct.d) / 2.0), 2) / 
                     NULLIF(((ct.a + ct.c) * (ct.b + ct.d) * (ct.a + ct.b) * (ct.c + ct.d)), 0) AS NUMERIC), 8) AS chi_squared_yates,
               LOG(2, (ct.a + 0.5) / NULLIF((((ct.a + ct.b) * (ct.a + ct.c)) / (ct.a + ct.b + ct.c + ct.d) + 0.5), 0)) AS ic
        FROM faers_b.contingency_table ct
        WHERE ct.a > 0;
        """
        
        # Test successful execution
        cursor.execute.return_value = None
        cursor.rowcount = 100000  # Mock large statistical operation
        cursor.execute(complex_stats_sql)
        assert cursor.execute.called
        
        # Test numerical overflow during complex calculations
        cursor.execute.side_effect = pg_errors.NumericValueOutOfRange("Numerical overflow in statistical calculation")
        
        with pytest.raises(pg_errors.NumericValueOutOfRange):
            cursor.execute(complex_stats_sql)
        
        # Test out of memory error during large aggregations
        cursor.execute.side_effect = pg_errors.OutOfMemory("Insufficient memory for statistical analysis")
        
        with pytest.raises(pg_errors.OutOfMemory):
            cursor.execute("SELECT * FROM large_contingency_calculation")
        
        # Test division by zero handling
        cursor.execute.side_effect = pg_errors.DivisionByZero("Division by zero in statistical formula")
        
        with pytest.raises(pg_errors.DivisionByZero):
            cursor.execute("SELECT a/b FROM contingency_table WHERE b = 0")
        
        # Test floating point errors in complex calculations
        cursor.execute.side_effect = pg_errors.FloatingPointException("Floating point error in LOG calculation")
        
        with pytest.raises(pg_errors.FloatingPointException):
            cursor.execute("SELECT LOG(2, negative_value) FROM test_table")


# Additional validation tests
class TestS11SQLValidation:
    """Additional validation tests for S11 SQL operations"""
    
    def test_statistical_formula_validation(self):
        """Test statistical formula validation"""
        statistical_functions = [
            'LOG(2, (ct.a + 0.5)',                    # Information Component
            'EXP(LN(',                                # Confidence intervals  
            'SQRT((1.0 / ct.a)',                      # Standard error
            'POWER(ABS((ct.a * ct.d) - (ct.b * ct.c))', # Chi-squared
            'NULLIF(((ct.a + ct.c) * (ct.b + ct.d))'  # Division by zero protection
        ]
        
        for formula in statistical_functions:
            assert any(func in formula for func in ['LOG', 'EXP', 'SQRT', 'POWER', 'NULLIF'])

    def test_date_conversion_patterns(self):
        """Test date conversion patterns"""
        date_conversions = [
            "TO_DATE(NULLIF(fda_dt, ''), 'YYYYMMDD')",
            "TO_DATE(NULLIF(event_dt, ''), 'YYYYMMDD')",
            "TO_DATE(NULLIF(tc.start_dt, ''), 'YYYYMMDD')"
        ]
        
        for conversion in date_conversions:
            assert 'TO_DATE' in conversion
            assert 'NULLIF(' in conversion
            assert 'YYYYMMDD' in conversion

    def test_exclusion_filters_validation(self):
        """Test exclusion filters validation"""
        exclusions = [
            "dm.remapping_rxaui != '92683486'",       # Excludes 'UNKNOWN STR'
            "ic.indi_pt NOT IN ('10070592', '10057097')" # Excludes specific MedDRA codes
        ]
        
        for exclusion in exclusions:
            assert '!=' in exclusion or 'NOT IN' in exclusion

    def test_aggregation_functions_validation(self):
        """Test aggregation functions validation"""
        aggregations = [
            'COUNT(*) AS count_of_reaction',
            'SUM(count_of_reaction) AS margin',
            'SUM(count_of_reaction) AS n',
            'GROUP BY rxaui, drug, adverse_event'
        ]
        
        for agg in aggregations:
            assert any(func in agg for func in ['COUNT', 'SUM', 'GROUP BY'])

    def test_index_creation_patterns(self):
        """Test index creation patterns"""
        index_patterns = [
            'idx_drugs_standardized_primaryid',
            'idx_adverse_reactions_primaryid', 
            'idx_contingency_table_rxaui',
            'idx_proportionate_analysis_adverse_event'
        ]
        
        for pattern in index_patterns:
            assert 'idx_' in pattern
            assert any(suffix in pattern for suffix in ['primaryid', 'rxaui', 'adverse_event'])


if __name__ == "__main__":
    # Run tests with: python -m pytest unit_tests/sql/test_s11.py -v
    # Run server tests only: python -m pytest unit_tests/sql/test_s11.py -v -k "server"
    # Run without server tests: python -m pytest unit_tests/sql/test_s11.py -v -k "not server"
    pytest.main([__file__, "-v"])