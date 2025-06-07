"""
Part of script 1: Check if data directories specified
in the configuration exists, and if not, prompt
to create them.
"""

import os
import sys
import json

from constants import OPTIONS_DIR
from option import get_option_from_json
from prompt import prompt
from error import fatal_error, get_logger
log = get_logger()

def get_non_existent_dirs(root_dir, directories, root_data_dir):
    """
    Return a list of data directories not present.
    """
    non_existent_dirs = []
    for dir_info in directories:
        dir_name = dir_info["name"]
        dir_path = dir_info["path"]
        full_path = os.path.join(root_dir, dir_path)

        if not os.path.exists(full_path):
            non_existent_dirs.append(full_path)

        there_are_subdirs = (
            "subdirectories" in dir_info
            and
            dir_info["subdirectories"] is not None
        )
        if there_are_subdirs:
            for sub_name, sub_path in dir_info["subdirectories"].items():
                sub_full_path = os.path.join(root_dir, sub_path)
                if not os.path.exists(sub_full_path):
                    non_existent_dirs.append(sub_full_path)

    return non_existent_dirs

def prompt_for_dir_creation(non_existent_dirs):
    """
    Ask the user if non-existent data directories should be created.
    If the user answers no, we exit the program.
    """
    if len(non_existent_dirs) == 0:
        log.info("Directories correctly initialised.")
        return False

    print("These directories are specified in the configs, but do not exist:")
    for d in non_existent_dirs:
        print(d)

    if prompt(f"Do you want to create them automatically?"):
       return True

    log.info("No automatic folder creation requested.")
    log.info("Exiting.")
    sys.exit(0)
    return False

def create_directories(directories):
    """
    Given an array of directories, make them.
    """
    for dir in directories:
        try:
            os.makedirs(dir)
        except Exception as e:
            raise fatal_error(f"Cannot make directory '{dir}'", e, 1)

def check_if_directories_exist():
    """
    Main function of this file.
    """
    dir_config_location = get_option_from_json(
        OPTIONS_DIR,
        "location_data_directories"
    )
    config = {}
    try:
        with open(dir_config_location) as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        log.warning("Directory config is of an invalid JSON format")
        # Fallback: empty dictionary
        config['data_directories'] = {}
    except FileNotFoundError as e:
        raise fatal_error(
            f"There exists no JSON file at {dir_config_location}", e, 1)
    except Exception as e:
        raise fatal_error(
            f"Failed to read JSON file", e, 1)

    root_data_dir = get_option_from_json(
        OPTIONS_DIR,
        "root_data_dir"
    )

    non_existent_dirs = get_non_existent_dirs(
         root_data_dir,
         config['data_directories'],
         root_data_dir)
    if prompt_for_dir_creation(non_existent_dirs):
        create_directories(non_existent_dirs)

__all__ = ["check_if_directories_exist"]
