import os
import sys
import json
from .prompt import prompt

def get_non_existent_dirs(root_dir, directories, root_data_dir):
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
    if len(non_existent_dirs) == 0:
        print("Directories correctly initialised.")
        return False

    print("These directories are specified in the configs, but do not exist:")
    for d in non_existent_dirs:
        print(d)

    if prompt(f"Do you want to create them automatically?"):
       return True

    print("No automatic folder creation requested.")
    print("Exiting.")
    sys.exit(0)
    return False

def create_directories(directories):
    for dir in directories:
        try:
            os.makedirs(dir)
        except OSError as e:
            print(f"Failed to create dir '{dir}': {e}")

def check_if_directories_exist():
    try:
        with open('config/directories.json') as f:
            config = json.load(f)
    except json.JSONDecodeError as se:
        print(f"Config file is of an invalid JSON format:\n{e}")
    root_dir = config["root_data_dir"]
    non_existent_dirs = get_non_existent_dirs(
         root_dir,
         config['data_directories'],
         root_dir)
    if prompt_for_dir_creation(non_existent_dirs):
        create_directories(non_existent_dirs)

__all__ = ["check_if_directories_exist"]
