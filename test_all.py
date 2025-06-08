import unittest
import sys
import os

def main():
    loader = unittest.TestLoader()
    loader.workingDirectory = os.path.dirname(os.path.abspath(__file__))
    tests = loader.discover('tests/python', pattern="test_*.py")

    # TODO these tests should all be added but do not work yet.
    # This is because the module imports are failing. This should be fixed.
    # @xocas @khayri @kai

    # sql_tests = loader.discover('tests/sql', pattern="test_*.py")
    # tests.addTests(sql_tests)
    # unit_tests = loader.discover('unit_tests/python', pattern="test_*.py")
    # tests.addTests(unit_tests)
    # sql_unit_tests = loader.discover('unit_tests/sql', pattern="test_*.py")
    # tests.addTests(sql_unit_tests)

    runner = unittest.TextTestRunner(verbosity=2)

    print(f"FOUND {tests.countTestCases()} TESTS TO RUN.")

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
