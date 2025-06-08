import unittest
import os
import sys
import subprocess

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

def main():
    # Set project root to this script’s directory
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
    unittest.TextTestRunner(verbosity=2).run(all_tests)

    # Run SQL tests
    print("\nRUNNING SQL TESTS VIA PYTEST")
    subprocess.run(["pytest", "unit_tests/sql", "-v"])

if __name__ == "__main__":
    main()
