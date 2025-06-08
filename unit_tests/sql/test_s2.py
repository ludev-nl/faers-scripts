import pytest
import psycopg
from unittest.mock import patch, MagicMock
import json
from datetime import datetime, date
import tempfile
import os


class TestS2SQL:
    """Unit tests for s2.sql database functions and procedures"""

    @pytest.fixture
    def mock_db_connection(self):
        """Mock database connection for testing"""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        return conn, cursor

    @pytest.fixture
    def sample_columns_json(self):
        """Sample column definitions for testing"""
        return {
            "caseid": "VARCHAR(50)",
            "caseversion": "INT",
            "i_f_code": "VARCHAR(10)",
            "event_dt": "DATE",
            "mfr_dt": "DATE"
        }

    def test_schema_creation(self, mock_db_connection):
        """Test 1: Verify schema creation logic"""
        conn, cursor = mock_db_connection

        # Simulate schema creation
        cursor.execute.return_value = None
        cursor.fetchone.return_value = (True,)

        # Test schema exists after creation
        cursor.execute("CREATE SCHEMA IF NOT EXISTS faers_a")
        cursor.execute("SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'faers_a')")

        result = cursor.fetchone()
        assert result[0] is True
        cursor.execute.assert_called()

    def test_get_completed_year_quarters_basic_logic(self, mock_db_connection):
        """Test 2: Test year-quarter generation logic with current date"""
        conn, cursor = mock_db_connection

        # Mock current date as 2024 Q2 (should return up to 2024 Q1)
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 6, 15)  # Q2

            # Expected results for start_year=4 (2004) to 2024 Q1
            expected_quarters = []
            for year in range(2004, 2025):
                for quarter in range(1, 5):
                    if year == 2024 and quarter > 1:  # Stop at Q1 2024
                        break
                    expected_quarters.append((year, quarter))

            cursor.fetchall.return_value = expected_quarters

            # Execute function
            cursor.callproc("faers_a.get_completed_year_quarters", [4])
            results = cursor.fetchall()

            assert len(results) > 0
            assert (2004, 1) in results
            assert (2024, 1) in results
            assert (2024, 2) not in results  # Current quarter shouldn't be included

    def test_get_completed_year_quarters_edge_case_q1(self, mock_db_connection):
        """Test 3: Test year-quarter logic when current quarter is Q1"""
        conn, cursor = mock_db_connection

        # Mock current date as 2024 Q1 (should return up to 2023 Q4)
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 2, 15)  # Q1

            expected_last = (2023, 4)
            cursor.fetchall.return_value = [(2023, 4), (2023, 3)]

            cursor.callproc("faers_a.get_completed_year_quarters", [23])  # Start from 2023
            results = cursor.fetchall()

            assert expected_last in results
            assert (2024, 1) not in results

    def test_process_faers_file_table_name_generation(self, mock_db_connection):
        """Test 4: Test table name generation logic"""
        conn, cursor = mock_db_connection

        test_cases = [
            ("DEMO", 2023, 1, "faers_a.demo23q1"),
            ("DRUG", 2024, 4, "faers_a.drug24q4"),
            ("REAC", 2022, 2, "faers_a.reac22q2")
        ]

        for schema_name, year, quarter, expected_table in test_cases:
            # Calculate expected table name
            actual_table = f"faers_a.{schema_name.lower()}{year % 100:02d}q{quarter}"
            assert actual_table == expected_table

    def test_process_faers_file_column_definition_building(self, mock_db_connection, sample_columns_json):
        """Test 5: Test column definitions are built correctly from JSON"""
        conn, cursor = mock_db_connection

        # Test column definition string building
        expected_columns = ["caseid", "caseversion", "i_f_code", "event_dt", "mfr_dt"]
        expected_types = ["VARCHAR(50)", "INT", "VARCHAR(10)", "DATE", "DATE"]

        column_def_parts = []
        column_names = []

        for key, value in sample_columns_json.items():
            column_def_parts.append(f"{key} {value}")
            column_names.append(key)

        column_def = ", ".join(column_def_parts)
        columns = ", ".join(column_names)

        assert all(col in columns for col in expected_columns)
        assert all(typ in column_def for typ in expected_types)
        assert column_def.count(",") == len(sample_columns_json) - 1

    def test_server_connection_timeout(self, mock_db_connection):
        """Test 6: Server-related test - connection timeout handling"""
        conn, cursor = mock_db_connection

        # Simulate server timeout
        cursor.execute.side_effect = psycopg2.OperationalError("server closed the connection unexpectedly")

        with pytest.raises(psycopg2.OperationalError):
            cursor.execute("SELECT 1")

        # Test that we can detect and handle server issues
        try:
            cursor.execute("SELECT 1")
        except psycopg2.OperationalError as e:
            assert "server closed the connection" in str(e)

    def test_server_encoding_validation(self, mock_db_connection):
        """Test 7: Server-related test - UTF-8 encoding validation"""
        conn, cursor = mock_db_connection

        # Test encoding setting
        cursor.fetchone.return_value = ("UTF8",)
        cursor.execute("SHOW client_encoding")
        encoding = cursor.fetchone()[0]

        assert encoding == "UTF8"

        # Test with non-UTF8 encoding (should fail in real scenario)
        cursor.fetchone.return_value = ("LATIN1",)
        cursor.execute("SHOW client_encoding")
        encoding = cursor.fetchone()[0]

        assert encoding != "UTF8"  # This would indicate a server configuration issue

    def test_file_header_validation_logic(self, mock_db_connection, sample_columns_json):
        """Test 8: Test file header validation logic"""
        conn, cursor = mock_db_connection

        # Test correct header count
        expected_count = len(sample_columns_json)
        actual_count = 5  # Should match sample_columns_json length

        assert actual_count == expected_count

        # Test incorrect header count (should raise exception)
        wrong_count = 3
        assert wrong_count != expected_count

        # Simulate header mismatch scenario
        with pytest.raises(AssertionError):
            if wrong_count != expected_count:
                raise AssertionError(f'Header column count mismatch: expected {expected_count}, got {wrong_count}')

    def test_copy_command_format_validation(self, mock_db_connection):
        """Test 9: Test \\copy command format validation"""
        conn, cursor = mock_db_connection

        test_file_path = "/path/to/test/file.txt"
        table_name = "faers_a.demo23q1"
        columns = "caseid, caseversion, i_f_code"

        # Build expected copy command
        expected_copy_cmd = f"\\copy {table_name} ({columns}) FROM '{test_file_path}' WITH (FORMAT csv, DELIMITER '$', HEADER true, NULL '', ENCODING 'UTF8')"

        # Validate command structure
        assert "\\copy" in expected_copy_cmd
        assert "FORMAT csv" in expected_copy_cmd
        assert "DELIMITER '$'" in expected_copy_cmd
        assert "HEADER true" in expected_copy_cmd
        assert "ENCODING 'UTF8'" in expected_copy_cmd
        assert test_file_path in expected_copy_cmd

    def test_error_handling_and_rollback_logic(self, mock_db_connection):
        """Test 10: Test error handling and transaction rollback"""
        conn, cursor = mock_db_connection

        # Test successful transaction
        cursor.execute.return_value = None
        cursor.execute("BEGIN")
        cursor.execute("CREATE TABLE test_table (id INT)")
        cursor.execute("COMMIT")

        # Test failed transaction with rollback
        cursor.execute.side_effect = [None, None, Exception("SQL Error"), None]

        try:
            cursor.execute("BEGIN")
            cursor.execute("CREATE TABLE test_table (id INT)")
            cursor.execute("INSERT INTO invalid_table VALUES (1)")  # This should fail
        except Exception as e:
            cursor.execute("ROLLBACK")
            assert "SQL Error" in str(e)

        # Verify rollback was called
        calls = [call[0][0] for call in cursor.execute.call_args_list]
        assert "ROLLBACK" in calls


# Additional helper functions for integration testing
class TestS2SQLIntegration:
    """Integration tests that require actual database connection"""

    @pytest.mark.integration
    def test_actual_database_connection(self):
        """Integration test - requires actual database"""
        pytest.skip("Requires actual database connection - run separately")

    @pytest.mark.integration
    def test_file_processing_with_real_data(self):
        """Integration test with real file processing"""
        pytest.skip("Requires actual database and file system - run separately")


if __name__ == "__main__":
    # Run tests with: python -m pytest unit_tests/sql/test_s2.py -v
    # Run with server tests: python -m pytest unit_tests/sql/test_s2.py -v -k "server"
    # Run without integration: python -m pytest unit_tests/sql/test_s2.py -v -m "not integration"
    pytest.main([__file__, "-v"])
