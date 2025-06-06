"""
Pipeline wide: Hard-coded constants.
"""

import os

# from option import get_option_from_json

# These directories are hard-coded, the rest is specified
# in the options file.
CONFIG_DIR = os.path.abspath('config')
OPTIONS_DIR = os.path.abspath('config/general_options.json')
# This is unfortunately hard coded.
LOG_DIR = os.path.abspath('faers_data/logs')

# obtain values from option file
# ROOT_DATA_DIR = get_option_from_json(
#     OPTIONS_DIR,
#     "root_data_dir"
# )
