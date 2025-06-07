"""
Pipeline wide: Hard-coded constants.
"""

import os
from pathlib import Path # TODO use os, not pathlib!

# These directories are hard-coded, the rest is specified
# in the options file.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / 'config'
OPTIONS_DIR = CONFIG_DIR / 'general_options.json'
LOGS_DIR = PROJECT_ROOT / 'logs'
DATA_DIR = PROJECT_ROOT / 'data'
SQL_PATH = PROJECT_ROOT / 'sql'

#TODO: MERGE THESE OPTIONS. use os.path.abspath for paths!

# These directories are hard-coded, the rest is specified
# in the options file.
CONFIG_DIR = os.path.abspath('config')
OPTIONS_DIR = os.path.abspath('config/general_options.json')
# This is unfortunately hard coded.
LOG_DIR = os.path.abspath('faers_data/logs')
