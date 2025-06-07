"""
Pipeline wide: Hard-coded constants.
"""

import os
#from pathlib import Path # TODO use os, not pathlib!

# Define the project root as the parent of the parent directory of this file
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Directory paths
CONFIG_DIR = os.path.join(PROJECT_ROOT, "config")
OPTIONS_DIR = os.path.join(CONFIG_DIR, "general_options.json")
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
SQL_PATH = os.path.join(PROJECT_ROOT, "sql")

#below is the legacy code that uses pathlib, functionality is the same as above, we kept this here as a refrnece
"""# These directories are hard-coded, the rest is specified
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
LOG_DIR = os.path.abspath('faers_data/logs')"""