import pytest
import os
import tempfile
import json
from unittest import mock

from check_directories import get_non_existent_dirs, check_if_directories_exist
from error import fatal_error

# ---------------------------------------------------------
# Test get_non_existent_dirs with missing directories
def test_get_non_existent_dirs_missing():
    root_dir = "/tmp"
    directories = [{"name": "missing_dir", "path": "this_dir_doesnt_exist"}]
    result = get_non_existent_dirs(root_dir, directories, root_dir)
    assert "/tmp/this_dir_doesnt_exist" in result

# ---------------------------------------------------------
# Test get_non_existent_dirs with existing directories
def test_get_non_existent_dirs_existing(tmpdir):
    root_dir = str(tmpdir)
    directories = [{"name": "test_dir", "path": "test_dir"}]
    os.makedirs(os.path.join(root_dir, "test_dir"))
    result = get_non_existent_dirs(root_dir, directories, root_dir)
    assert len(result) == 0

# ---------------------------------------------------------
# Test get_non_existent_dirs with missing subdirectories
def test_get_non_existent_dirs_missing_subdir():
    root_dir = "/tmp"
    directories = [{
        "name": "parent_dir",
        "path": "parent_dir",
        "subdirectories": {
            "child": "parent_dir/child_dir"
        }
    }]
    result = get_non_existent_dirs(root_dir, directories, root_dir)
    assert "/tmp/parent_dir/child_dir" in result

# ---------------------------------------------------------
# Patch at the correct module (src.check_directories)
@mock.patch('check_directories.get_option_from_json')
@mock.patch('check_directories.log')
def test_check_if_directories_exist_invalid_json(mock_log, mock_getopt):

    # We use the simpler version of temporary file creation here,
    # which requires manual clean-up.
    fd, json_path = tempfile.mkstemp(suffix='.json')
    try:
        with os.fdopen(fd, 'w') as f:
            f.write("{ invalid json")

        mock_getopt.return_value = json_path
        check_if_directories_exist()

        mock_log.warning.assert_called_with(
            "Directory config is of an invalid JSON format"
        )
    finally:
        os.unlink(json_path)

# ---------------------------------------------------------
# Test user decline directory creation
@mock.patch(
    'check_directories.get_non_existent_dirs',
    return_value=["missing_dir"]
)
# Simulate that the user answers with No.
@mock.patch(
    'check_directories.prompt',
    return_value=False
)
@mock.patch(
    'check_directories.sys.exit',
    side_effect=Exception("sys.exit(0) called")
)
def test_prompt_dir_creation_declined(
    mock_exit,
    mock_prompt,
    mock_get_dirs
):
    # We use the simpler version of temporary file creation here,
    # which requires manual clean-up.
    fd, fake_json = tempfile.mkstemp(suffix='.json', dir='/tmp')

    try:
        with os.fdopen(fd, 'w') as f:
            json.dump({"data_directories": []}, f)

        with(
            mock.patch('check_directories.get_option_from_json')
            as
            mock_get_json
        ):
            mock_get_json.side_effect = [
                fake_json,            # dir_config_location
                "/root_dir"           # root_data_dir
            ]

            with pytest.raises(Exception) as exc_info:
                check_if_directories_exist()

            assert str(exc_info.value) == "sys.exit(0) called"
            mock_exit.assert_called_with(0)
            mock_prompt.assert_called_once()
    finally:
        if os.path.exists(fake_json):
            os.unlink(fake_json)
