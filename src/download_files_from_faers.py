"""
Part of script 1: Downloads and handles files
of the FAERS dataset. Handles both legacy and current formats.
"""

import zipfile
import os
import shutil
import sys
import subprocess
from datetime import datetime
from itertools import product

from constants import OPTIONS_DIR
from prompt import prompt
from option import get_option_from_json
from error import get_logger, fatal_error
log = get_logger()

def remove_file(file_path):
    try:
        os.remove(file_path)
    except Exception as e:
        raise fatal_error(f"Unable to remove {file_path}", e, 1)

def determine_quarters(
        start_year: int,
        start_quarter: int,
        end_year: int | None = None,
        end_quarter: int | None = None
    ):
    """
    Determines the current quarter and generates a list of year-quarter pairs
    to download data from, starting at 2012Q4 (post-legacy/current).
    """
    QUARTERS = [1, 2, 3, 4]

    if end_year is None and end_quarter is None:
        # Determine the last completed quarter
        now = datetime.now()
        end_year = now.year
        end_quarter = (now.month - 1) // 3

    # Generate list of years and quarters
    years = range(start_year, end_year + 1)

    # Create year-quarter pairs with the desired format
    return iter([
        f"{y}Q{q}"
        for y, q in product(years, QUARTERS)
        if (y > start_year or q >= start_quarter)
        and (y < end_year or q <= end_quarter)
    ])

class DownloadFiles:
    def __init__(self, rootdir, legacy: bool = False):
        """
        Initializing DownloadFiles class starts the main loop.

        Attributes:
        :param root_dir: The root directory to store the downloaded files,
        defaults to faersData
        :type root_dir: str
        :param url: The url to download the files from, defaults to
        https://fis.fda.gov/content/Exports/faers_ascii_{year_quarter}.zip
        :type url: str
        :return: None
        :rtype: None
        """
        self.root_dir = rootdir

        self.url = None
        if legacy == True:
            self.url = (
            "https://fis.fda.gov/content/Exports/aers_ascii_{year_quarter}.zip"
            )
        else:
            self.url = (
            "https://fis.fda.gov/content/Exports/faers_ascii_{year_quarter}.zip"
            )

        self.current_cached = False #changed for every iteration
        self.should_prompt_for_dl = get_option_from_json(
                OPTIONS_DIR, "should_prompt_for_dl")
        self.should_cache = get_option_from_json(
                OPTIONS_DIR, "should_cache")

        self.year_quarters = None
        if legacy == True:
            self.year_quarters = determine_quarters(
                start_year = 2004,
                start_quarter = 1,
                end_year = 2012,
                end_quarter = 3
            )
        else:
            self.year_quarters = determine_quarters(
                start_year = 2012,
                start_quarter = 4
            )

        os.makedirs(self.root_dir, exist_ok=True)
        self.main_loop()

    def main_loop(self):
        """
        Iterates over the quartiles and downloads reports.
        """
        self.clean_corrupt_zip_files()

        self.current_quarter = next(self.year_quarters, None)
        while self.current_quarter is not None:
            self.current_quarter = next(self.year_quarters, None)

            self.current_zip = (
                f'{self.root_dir}/faers_ascii_{self.current_quarter}.zip'
            )
            self.target_dir = (
                f'{self.root_dir}/faers_ascii_{self.current_quarter}'
            )
            if not os.path.exists(self.current_zip):
                self.fetch_data()

            self.unzip_files()
            self.move_zip_contents()

            # reset for next quarter
            self.current_cached = False

    def clean_corrupt_zip_files(self):
        """
        Checks in the root data directory for any not properly
        downloaded zip files, and removes them to prevent errors.
        """
        for file_name in os.listdir(self.root_dir):
            if file_name.endswith(".zip"):
                file_path = os.path.join(
                    os.path.abspath(self.root_dir),
                    file_name
                )

                # remove empty zip files
                if os.path.getsize(file_path) == 0:
                    remove_file(file_path)

                # remove invalid zip files
                if not zipfile.is_zipfile(file_path):
                    remove_file(file_path)
                else:
                    with zipfile.ZipFile(file_path, 'r') as zip_ref:
                        if (testzip := zip_ref.testzip()) is not None:
                            remove_file(file_path)

    def move_zip_contents(self):
        """
        Moves the contents of the unzipped files into the respective
        data folder and
        moves the rest of the files into the rest folder.
        """
        try:
            os.makedirs(self.target_dir, exist_ok = True)
        except Exception as e:
            raise fatal_error(
                f"Error while making directory {self.target_dir}", e, 1)

        for folder in ["ascii", "ASCII", "asci", "asii"]:
            folder_path = os.path.join(self.target_dir, folder)

            # this will only happen for the correct folder,
            # so we continue inside this if statement...
            if os.path.exists(folder_path):
                for file_name in os.listdir(folder_path):
                    # where the file is in the [ascii,...] folder
                    file_path = os.path.join(folder_path, file_name)
                    # if the file already existed, it would be here
                    pos_path = os.path.join(self.target_dir, file_name)

                    # check it does not exist already
                    if not os.path.exists(pos_path):
                        shutil.move(
                            file_path,
                            os.path.join(self.target_dir,'.')
                            )
                    else:
                        remove_file(file_path)

                try:
                    os.rmdir(folder_path)
                except Exception as e:
                    raise fatal_error(
                        f"Unable to remove directory {folder_path}", e, 1)

                # ... here:
                # we now delete the pdf and doc files
                for file in os.listdir(os.path.join(self.target_dir, '.')):
                    if file.endswith(('.pdf', '.doc')):
                        remove_file(os.path.join(self.target_dir, file))

<<<<<<< HEAD
        # TODO maybe enable this at a later date. Needs
        # to have ascii_folder replaced.
=======
        # TODO
        # We sometimes found even more auxiliary files, but
        # are unable to find them later. We are leaving in some
        # code which should remove this.
        # Requires ascii_folder to be replaced.
>>>>>>> 36-bootstrapping-logging-framework

        # Rest bestanden
        # rest_folder = os.path.join(self.root_dir, "rest")
        # os.makedirs(rest_folder, exist_ok=True)
        #
        # for readme_file in ["README.doc", "Readme.doc"]:
        #     rest_folder_path = os.path.join(self.root_dir, readme_file)
        #     if os.path.exists(rest_folder_path):
        #         dest = os.path.join(
        #             rest_folder,
        #             f"{readme_file.split('.')[0]}{self.current_quarter}.doc"
        #         )
        #         shutil.move(rest_folder_path, dest)
        #
        # asc_nts_path = os.path.join(ascii_folder, "ASC_NTS.doc")
        # if os.path.exists(asc_nts_path):
        #     dest = os.path.join(
        #         rest_folder,
        #         f"ASC_NTS{self.current_quarter}.doc"
        #     )
        #     shutil.move(asc_nts_path, dest)
        #
        # # Onnodige bestanden verwijderen
        # for ext in ["*.pdf", "*.PDF", "*.doc", "*.zip"]:
        #     for file_path in glob.glob(os.path.join(self.root_dir, ext)):
        #         os.remove(file_path)
        #
        # for folder in ["deleted", "DELETED", "Deleted"]:
        #     folder_path = os.path.join(self.root_dir, folder)
        #     if os.path.exists(folder_path):
        #         os.rmdir(folder_path)

    def check_if_wget_installed(self):
        """
        Checks if wget is installed.
        """
        return shutil.which('wget') is not None

    def path_of_wget(self):
        """
        Returns where wget resides in PATH.
        """
        return shutil.which('wget')

    def fetch_data(self):
        """
        Uses wget to download the data from the FDA website into
        the data directory.
        """
        if self.should_prompt_for_dl:
            download_confirm = f"Start downloading from {self.current_quarter}?"
            if not prompt(download_confirm):
                log.info("Downloading cancelled.")
                log.info("Exiting.")
                sys.exit(0)

        # check if not already cached

        cache_location = os.path.join(
            self.root_dir,
            "cache",
            f"faers_ascii_{self.current_quarter}.zip"
            )
        if os.path.isfile(cache_location):
            self.current_cached = True
            log.info(
                    f"{self.current_quarter} is cached, no downloading needed.")
            try:
                shutil.copy(
                    cache_location,
                    self.root_dir
                    )
                return
            except Exception as e:
                raise fatal_error("Error retrieving cached file", e, 1)

        # file was not cached, go fetch it

        if not self.check_if_wget_installed():
            log.warning("wget not found. Is it installed?")

        url = self.url.format(year_quarter=self.current_quarter)
        zip_file_should_go_here = os.path.abspath(self.current_zip)
        command = f"{self.path_of_wget()} -O {zip_file_should_go_here} {url}"

        try:
            subprocess.run(command,
                           shell=True,
                           check=True,
                           cwd=os.path.abspath(self.root_dir)
                           )
            log.info(f"Downloaded: {self.current_zip}")
        except subprocess.CalledProcessError as e:
            raise fatal_error(
                 f"wget failed for {self.current_quarter}", e,1)
        except Exception as e:
            raise fatal_error(
                f"Download failed for {self.current_quarter}", e, 1)

    def unzip_files(self):
        """
        Unzips the downloaded data into the target directory.
        """
        zip_location = "EMPTY FILE PATH"

        if self.current_cached == True:
            zip_location = os.path.join(
                self.root_dir,
                'cache',
                os.path.basename(self.current_zip))
        else:
            zip_location = self.current_zip

        if not os.path.isfile(zip_location):
            raise fatal_error(
                f"Zip file is missing at: {zip_location}", Exception(), 1)

        try:
            os.makedirs(self.target_dir, exist_ok = True)
        except Exception as e:
            raise fatal_error(
                f"Unable to create dir {self.target_dir}.", e, 1)

        try:
            with zipfile.ZipFile(zip_location, 'r') as zip_ref:
                zip_ref.extractall(self.target_dir)
        except Exception as e:
            raise fatal_error(
                f"Error extracting zip file: {zip_location}", e, 1)

        if self.should_cache == True:
            try:
                shutil.move(
                    self.current_zip,
                    os.path.join(
                        self.root_dir,
                        'cache',
                        os.path.basename(self.current_zip)
                    ))
            except Exception as e:
                raise fatal_error("Error caching file", e, 1)
        else:
            try:
                os.remove(self.current_zip)
            except Exception as e:
                raise fatal_error("Error removing zip file", e, 1)

def start_downloading_current(rootdir):
    """
    Main function to start downloading from 2012
    quarter 4 onwards.
    """
    DownloadFiles( rootdir, legacy = False)

def start_downloading_legacy(rootdir):
    """
    Main function to start downloading from 2004 quarter 1
    up to (and incl.) 2012 quarter 3.
    """
    DownloadFiles(rootdir, legacy = True)

__all__ = ["start_downloading_current, start_downloading_legacy"]
