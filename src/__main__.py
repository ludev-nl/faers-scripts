# src/__main__.py
import argparse
import os
from .check_directories import check_if_directories_exist
from .check_jsons import check_json_configs
from .download_current_files_from_faers import start_downloading_current

DESCRIPTION = f"""
This script serves as the start script for the
FAERS pipeline. It can be used to do the following:

1. Check for new quarterly reports to download.
2. Run the pipeline to update the database.
"""

# TODO make these after check_if_directories_exist,
# so we can read out root_data_dir from directories.json
ROOTDIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# DATADIR = os.path.abspath('/faers/data/downloaded_files')
DATADIR = os.path.abspath('faers/data')

def main():
    parser = argparse.ArgumentParser(
        description=DESCRIPTION,
        usage='%(prog)s --help for more info',
        formatter_class=argparse.RawTextHelpFormatter
    )
    # parser.add_argument('--foo', nargs='?', help='foo help')
    args = parser.parse_args()

    try:
        check_if_directories_exist()
    except Exception as e:
        print(f"Failed to check for directories: {str(e)}")
        return False

    try:
        check_json_configs(ROOTDIR)
    except Exception as e:
        print(f"Failed to validate configuration: {str(e)}")
        return False

    try:
        start_downloading_current(DATADIR)
    except Exception as e:
        print(f"Failed to download files: {str(e)}")
        return False

if __name__ == "__main__":
    main()
