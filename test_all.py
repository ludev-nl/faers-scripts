import unittest
import os
import sys
import subprocess
import io
import re

def count_pytest_tests(test_dir):
    """Count the number of collected tests from pytest"""
    try:
        result = subprocess.run(
            ["pytest", "--collect-only", "-q", test_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        lines = result.stdout.strip().split('\n')
        test_count = len([line for line in lines if line.startswith("unit_tests")])
        return test_count
    except Exception as e:
        print(f"Error during pytest collection: {e}")
        return 0

class TestResultSummary:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.skipped = 0

    def summarize_unittest(self, result):
        self.failed = len(result.failures) + len(result.errors)
        self.skipped = len(result.skipped)
        self.passed = result.testsRun - self.failed - self.skipped

    def print_summary(self, label):
        print(f"\n--- {label.upper()} TEST SUMMARY ---")
        print(f"Passed : {self.passed}")
        print(f"Failed : {self.failed}")
        print(f"Skipped: {self.skipped}")

def main():
    project_root = os.path.abspath(os.path.dirname(__file__))
    print("Project root path:", project_root)

    os.chdir(project_root)
    sys.path.insert(0, project_root)

    loader = unittest.TestLoader()

    # Discover Python tests
    print("\nDISCOVERING PYTHON TESTS...")
    python_tests = loader.discover(
        start_dir='unit_tests/python',
        pattern='test_*.py',
        top_level_dir=project_root
    )
    python_test_count = python_tests.countTestCases()
    print(f"Discovered {python_test_count} Python test(s)")

    # Discover SQL tests using pytest
    print("\nDISCOVERING SQL TESTS...")
    sql_test_count = count_pytest_tests("unit_tests/sql")
    print(f"Discovered {sql_test_count} SQL test(s)")

    print(f"\nTOTAL TESTS: {python_test_count + sql_test_count}")

    # Run Python tests
    print("\nRUNNING PYTHON TESTS")
    all_tests = unittest.TestSuite()
    all_tests.addTests(python_tests)
    result_stream = io.StringIO()
    runner = unittest.TextTestRunner(stream=result_stream, verbosity=2)
    python_result = runner.run(all_tests)
    print(result_stream.getvalue())
    
    # Summarize Python results
    python_summary = TestResultSummary()
    python_summary.summarize_unittest(python_result)
    python_summary.print_summary("Python")

    # Run SQL tests via pytest
    print("\nRUNNING SQL TESTS VIA PYTEST")
    try:
        result = subprocess.run(
            ["pytest", "unit_tests/sql", "-v", "--tb=short"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        print(result.stdout)

        # Parse summary line at the end
        sql_summary = TestResultSummary()
        for line in result.stdout.splitlines():
            match = re.search(r"(\d+) passed", line)
            if match:
                sql_summary.passed = int(match.group(1))
            match = re.search(r"(\d+) failed", line)
            if match:
                sql_summary.failed = int(match.group(1))
            match = re.search(r"(\d+) skipped", line)
            if match:
                sql_summary.skipped = int(match.group(1))
        sql_summary.print_summary("SQL")

    except Exception as e:
        print(f"Error running SQL tests: {e}")

if __name__ == "__main__":
    main()