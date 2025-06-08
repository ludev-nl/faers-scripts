"""
Pipeline wide: Hard-coded constants.
"""

import os

# Define the project root as the parent of the parent directory of this file
path_of_constants_py = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(path_of_constants_py, os.pardir))

# Directory paths
CONFIG_DIR = os.path.join(PROJECT_ROOT, "config")
OPTIONS_DIR = os.path.join(CONFIG_DIR, "general_options.json")
DATA_DIR = os.path.join(PROJECT_ROOT, "faers_data")
LOGS_DIR = os.path.join(DATA_DIR, "logs")
SQL_PATH = os.path.join(PROJECT_ROOT, "sql")

#below is the legacy code that uses pathlib, functionality is the same as
# above, we kept this here as a reference
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
