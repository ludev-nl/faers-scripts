import pytest
import os
import tempfile
import json
import unittest
from unittest import mock

from option import get_option_from_json, load_options_json
from error import fatal_error

class TestJsonOptions(unittest.TestCase):

    def test_load_options_json_valid(self):
        """Test loading valid JSON."""
        fd, file_path = tempfile.mkstemp(suffix='.json')
        try:
            with os.fdopen(fd, 'w') as f:
                json.dump({"test": "value"}, f)
            result = load_options_json(file_path)
            self.assertEqual(result, {"test": "value"})
        finally:
            if os.path.exists(file_path):
                os.unlink(file_path)

    def test_load_options_json_invalid(self):
        """Test loading invalid JSON."""
        fd, file_path = tempfile.mkstemp(suffix='.json')
        try:
            with os.fdopen(fd, 'w') as f:
                f.write("{ invalid")
            result = load_options_json(file_path)
            self.assertEqual(result, {})
        finally:
            if os.path.exists(file_path):
                os.unlink(file_path)

    def test_load_options_json_file_not_found(self):
        """Test loading non-existent JSON file."""
        non_existent_path = "/nonexistent/path.json"
        result = load_options_json(non_existent_path)
        self.assertEqual(result, {})

    def test_get_string_option(self):
        """Test getting a simple string option."""
        fd, file_path = tempfile.mkstemp(suffix='.json')
        try:
            with os.fdopen(fd, 'w') as f:
                json.dump({"test": "value"}, f)
            result = get_option_from_json(file_path, "test")
            self.assertEqual(result, "value")
        finally:
            if os.path.exists(file_path):
                os.unlink(file_path)

    def test_get_option_from_json_boolean(self):
        """Test getting a boolean option."""
        fd, file_path = tempfile.mkstemp(suffix='.json')
        try:
            with os.fdopen(fd, 'w') as f:
                json.dump({"flag": True}, f)
            result = get_option_from_json(file_path, "flag")
            self.assertTrue(result)
        finally:
            if os.path.exists(file_path):
                os.unlink(file_path)

    def test_get_option_from_json_nested(self):
        """Test getting a nested option using dot notation."""
        fd, file_path = tempfile.mkstemp(suffix='.json')
        try:
            with os.fdopen(fd, 'w') as f:
                json.dump({"parent": {"child": "value"}}, f)
            result = get_option_from_json(file_path, "parent.child")
            self.assertEqual(result, "value")
        finally:
            if os.path.exists(file_path):
                os.unlink(file_path)

    @mock.patch('option.log')
    def test_load_options_json_file_not_found(self, mock_log):
        """Test loading non-existent JSON file."""
        non_existent_path = "/nonexistent/path.json"
        result = load_options_json(non_existent_path)
        self.assertEqual(result, {})
        mock_log.warning.assert_called_with(
            f"File not found here\n{non_existent_path}\nMake sure it exists."
        )

    @mock.patch('option.log')
    def test_load_options_json_missing(self, mock_log):
        """Test loading non-existent file."""
        result = load_options_json("/nonexistent/file.json")
        self.assertEqual(result, {})
        mock_log.warning.assert_called()

if __name__ == "__main__":
    unittest.main()
