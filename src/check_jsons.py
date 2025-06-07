"""
Part of script 1: Check if all configuration files
are of a valid JSON format.
"""

import os
import json

from error import fatal_error, get_logger
log = get_logger()

def check_json_configs(config_dir):
    """
    Main function of this script.
    """
    if not os.path.exists(config_dir) or not os.path.isdir(config_dir):
        log.warning(f"Unable to find configuration directory in {config_dir}")

    # we already validate directories.json
    # in check_directories.
    json_files = [
        f for f in os.listdir(config_dir)
        if f.endswith('.json') and f != 'directories.json'
    ]

    for json_file in json_files:
        file_path = os.path.join(config_dir, json_file)

        try:
            with open(file_path, 'r') as f:
                json.load(f)
        except json.JSONDecodeError as e:
            raise(f"Invalid JSON format in {json_file}", e , 1)

    log.info("Configuration for the rest correctly initialised.")

__all__ = ["check_json_configs"]
