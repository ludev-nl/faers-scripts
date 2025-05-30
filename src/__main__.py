# TODO: add pdf option functionality. SKIP
# test for all 4 schema variations
# errors often propagate up to this file, and the source is hard to find. DONE
# can we add backtraces of some kind? DONE
# add unit testing
# add error handling everywhere
# add function descriptions everywhere
# add file headers everywhere DONE
# clean up all files

import argparse
import os

from .constants import OPTIONS_DIR, CONFIG_DIR
from .option import get_option_from_json
from .error import get_logger, fatal_error
from .check_directories import check_if_directories_exist
from .check_jsons import check_json_configs
from .download_files_from_faers import (
    start_downloading_current,
    start_downloading_legacy
)

DESCRIPTION = f"""
This script serves as the start script for the
FAERS pipeline. It can be used to do the following:

1. Check for new quarterly reports to download.
2. Run the pipeline to update the database.
"""

def main():
    """
    Main function loop of the pipeline.
    """
    parser = argparse.ArgumentParser(
        description=DESCRIPTION,
        usage='%(prog)s --help for more info',
        formatter_class=argparse.RawTextHelpFormatter
    )
    args = parser.parse_args()

    log = get_logger()

    data_dir = get_option_from_json(OPTIONS_DIR, "root_data_dir")

    try:
        check_if_directories_exist()
    except Exception as e:
        raise fatal_error("Failed to check for directories", e, 1)

    try:
        check_json_configs(CONFIG_DIR)
    except Exception as e:
        raise fatal_error("Failed to validate configuration", e, 1)

    try:
        start_downloading_legacy(data_dir)
    except Exception as e:
        raise fatal_error("Failed to download legacy files", e, 1)

    try:
        start_downloading_current(data_dir)
    except Exception as e:
        raise fatal_error("Failed to download current files", e, 1)

if __name__ == "__main__":
    main()
