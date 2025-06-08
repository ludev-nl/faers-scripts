import unittest
import sys
import os

def main():
    loader = unittest.TestLoader()
    loader.workingDirectory = os.path.dirname(os.path.abspath(__file__))
    tests = loader.discover('tests/python', pattern="test_*.py")
    sql_tests = loader.discover('tests/sql', pattern="test_*.py")
    tests.addTests(sql_tests)
    runner = unittest.TextTestRunner(verbosity=2)

    print(f"FOUND {len(tests)} tests to run")

    # result = runner.run(tests)

    # Print summary
    tests_passed = (result.testsRun - len(result.errors) -
                    len(result.failures) - len(result.skipped))
    print(f"\nAll tests complete."
          f"Total tests passed: {tests_passed} of {result.testsRun}")
    if len(result.errors) + len(result.failures) > 0:
        sys.exit(1)

if __name__ == "__main__":
    main()
