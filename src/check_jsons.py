import os
import json

def check_json_configs(rootdir):
    config_dir = os.path.join(rootdir, 'config')

    if not os.path.exists(config_dir) or not os.path.isdir(config_dir):
        print(f"Error: 'config' directory not found in {rootdir}")
        return 1

    # we already validate directories.json
    # in check_directories.
    json_files = [
        f for f in os.listdir(config_dir)
        if f.endswith('.json') and f != 'directories.json'
    ]

    error_found = False

    for json_file in json_files:
        file_path = os.path.join(config_dir, json_file)

        try:
            with open(file_path, 'r') as f:
                json.load(f)
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON format in {json_file}")
            print(f"Error message: {e}")
            error_found = True

    if not error_found:
        print("Configuration for the rest correctly initialised.")
    else:
        return 1
    return 0

__all__ = ["check_json_configs"]
