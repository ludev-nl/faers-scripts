import unittest
import sys
import os

def main():
    project_root = os.path.abspath(os.path.dirname(__file__))
    sys.path.insert(0, project_root)

    loader = unittest.TestLoader()
    runner = unittest.TextTestRunner(verbosity=2)

    test_dirs = [
        'tests/python',
        'tests/sql',
        'unit_tests/python',
        'unit_tests/sql',
    ]

    all_tests = unittest.TestSuite()
    total_test_count = 0

    for test_dir in test_dirs:
        if os.path.isdir(test_dir):
            suite = loader.discover(start_dir=test_dir, pattern="test*.py", top_level_dir=project_root)
            count = suite.countTestCases()
            print(f"Discovered {count} tests in {test_dir}")
            all_tests.addTests(suite)
            total_test_count += count
        else:
            print(f"Warning: {test_dir} does not exist and will be skipped.")

    print(f"\nFOUND {total_test_count} TESTS TO RUN.")
    result = runner.run(all_tests)

    # Summary
    tests_passed = result.testsRun - len(result.errors) - len(result.failures) - len(result.skipped)
    print(f"\nAll tests complete. Total tests passed: {tests_passed} of {result.testsRun}")

    if result.errors or result.failures:
        sys.exit(1)

if __name__ == "__main__":
    main()
