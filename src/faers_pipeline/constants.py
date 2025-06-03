"""
Pipeline wide: Hard-coded constants.
"""

import os
from pathlib import Path

#from option import get_option_from_json

# These directories are hard-coded, the rest is specified
# in the options file.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / 'config'
OPTIONS_DIR = CONFIG_DIR / 'general_options.json'
LOGS_DIR = PROJECT_ROOT / 'logs'
DATA_DIR = PROJECT_ROOT / 'data'
SQL_PATH = PROJECT_ROOT / 'sql'

#TODO: make this work again
# obtain values from option file
#ROOT_DATA_DIR = get_option_from_json(
#    str(OPTIONS_DIR), 
#    "root_data_dir"
#)