import pytest
import os
import tempfile
import json
import unittest
from unittest import mock

from check_directories import get_non_existent_dirs, check_if_directories_exist
from error import fatal_error

class TestDirectoryChecks(unittest.TestCase):
    """Unit tests for directory checking functions."""

    def setUp(self):
        """Set up test environment."""
        # Setup any mocks or initialization needed for all tests
        self.mock_log = mock.patch('check_directories.log').start()
        self.addCleanup(self.mock_log.stop)

    def test_get_non_existent_dirs_missing(self):
        """Test get_non_existent_dirs with missing directories."""
        root_dir = "/tmp"
        directories = [{"name": "missing_dir", "path": "this_dir_doesnt_exist"}]
        result = get_non_existent_dirs(root_dir, directories, root_dir)
        self.assertIn("/tmp/this_dir_doesnt_exist", result)

    def test_get_non_existent_dirs_existing(self):
        """Test get_non_existent_dirs with existing directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root_dir = tmpdir
            directories = [{"name": "test_dir", "path": "test_dir"}]
            os.makedirs(os.path.join(root_dir, "test_dir"))
            result = get_non_existent_dirs(root_dir, directories, root_dir)
            self.assertEqual(len(result), 0)

    def test_get_non_existent_dirs_missing_subdir(self):
        """Test get_non_existent_dirs with missing subdirectories."""
        root_dir = "/tmp"
        directories = [{
            "name": "parent_dir",
            "path": "parent_dir",
            "subdirectories": {
                "child": "parent_dir/child_dir"
            }
        }]
        result = get_non_existent_dirs(root_dir, directories, root_dir)
        self.assertIn("/tmp/parent_dir/child_dir", result)

    @mock.patch('check_directories.get_option_from_json')
    @mock.patch('check_directories.log')
    def test_check_if_directories_exist_invalid_json(self, mock_log, mock_getopt):
        """Test check_if_directories_exist with an invalid JSON file."""
        # Create a temporary invalid JSON file
        fd, fake_json = tempfile.mkstemp(suffix='.json')
        try:
            with os.fdopen(fd, 'w') as f:
                f.write("{ invalid json")  # Invalid JSON content

            mock_getopt.return_value = fake_json
            check_if_directories_exist()

            mock_log.warning.assert_called_once_with(
                "Directory config is of an invalid JSON format"
            )
        finally:
            # Ensure the temporary file is cleaned up
            os.close(fd)
            if os.path.exists(fake_json):
                os.remove(fake_json)

    @mock.patch(
        'check_directories.get_non_existent_dirs',
        return_value=["missing_dir"]
    )
    @mock.patch(
        'check_directories.prompt',
        return_value=False
    )
    @mock.patch(
        'check_directories.sys.exit',
        side_effect=Exception("sys.exit(0) called")
    )
    def test_prompt_dir_creation_declined(self, mock_exit, mock_prompt, mock_get_dirs):
        """Test user declining directory creation."""
        # Create a temporary JSON file
        fd, fake_json = tempfile.mkstemp(suffix='.json', dir='/tmp')
        try:
            with os.fdopen(fd, 'w') as f:
                json.dump({"data_directories": []}, f)

            with mock.patch('check_directories.get_option_from_json') as mock_get_json:
                mock_get_json.side_effect = [
                    fake_json,  # dir_config_location
                    "/root_dir" # root_data_dir
                ]

                with self.assertRaises(Exception) as exc_info:
                    check_if_directories_exist()

                self.assertEqual(str(exc_info.exception), "sys.exit(0) called")
                mock_exit.assert_called_once_with(0)
                mock_prompt.assert_called_once()
        finally:
            # Clean up the temporary JSON file
            os.close(fd)
            if os.path.exists(fake_json):
                os.remove(fake_json)

if __name__ == "__main__":
    unittest.main()
