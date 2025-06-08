import pytest
import os
import tempfile
import json
import unittest
from unittest import mock

from check_jsons import check_json_configs
from error import fatal_error

class TestJsonConfigValidation(unittest.TestCase):
    """Unit tests for JSON config validation functions."""

    def test_check_json_configs_invalid_json(self):
        """Test check_json_configs with an invalid JSON file."""
        with tempfile.mkstemp(suffix='.json', dir='/tmp') as (fd, fake_json):
            try:
                with open(fd, 'w') as f:
                    f.write("{ invalid json")  # Invalid JSON content

                with mock.patch('check_directories.log') as mock_log:
                    with self.assertRaises(SystemExit) as exc_info:
                        check_json_configs('/tmp', fake_json)

                self.assertEqual(exc_info.exception.code, 1)
                mock_log.warning.assert_not_called()
            finally:
                # Ensure the temporary file is cleaned up
                if os.path.exists(fake_json):
                    os.remove(fake_json)

if __name__ == "__main__":
    unittest.main()
