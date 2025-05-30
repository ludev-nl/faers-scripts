import zipfile
import os
import shutil
import glob
import sys
import subprocess
from datetime import datetime
from itertools import product

from .prompt import prompt

#TODO: Check if directories from config.json exist, otherwise create them
# -->do this in create_folders.py
#TODO: Indication of download progress, such as in bash

# Current / non-legacy starts at 2012Q4
START_YEAR = 2012
START_QUARTER = 4
QUARTERS = [1, 2, 3, 4]

"""
Downloads the latest files from the FDA website and unzips them into the root
directory. Moves the contents of the unzipped files into the ascii folder and
moves the rest of the files into the rest folder. This class replaces the old
bash script download_current_files_from_faers.sh.
"""

def determine_quarters():
    """
    Determines the current quarter and generates a list of year-quarter pairs
    to download data from, starting at 2012Q4 (post-legacy/current).
    """
    # Determine the last completed quarter
    now = datetime.now()
    end_year = now.year
    end_quarter = (now.month - 1) // 3

    # Generate list of years and quarters
    years = range(START_YEAR, end_year + 1)

    # Create year-quarter pairs with the desired format
    return iter([
        f"{y}Q{q}"
        for y, q in product(years, QUARTERS)
        if (y > START_YEAR or q >= START_QUARTER)
        and (y < end_year or q <= end_quarter)
    ])

class DownloadFiles:
    def __init__(self, rootdir):
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
        self.url = (
        "https://fis.fda.gov/content/Exports/faers_ascii_{year_quarter}.zip"
        )
        self.year_quarters = determine_quarters()

        os.makedirs(self.root_dir, exist_ok=True)
        self.main_loop()

    def main_loop(self):
        """
        Iterates over the quartiles and downloads reports.
        """
        while True:
            try:
                self.current_quarter = next(self.year_quarters)
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

            except StopIteration:
                break

    def move_zip_contents(self):
        """
        Moves the contents of the unzipped files into the ascii folder and
        moves the rest of the files into the rest folder.
        """
        ascii_folder = os.path.join(self.root_dir, "ascii")
        os.makedirs(ascii_folder, exist_ok=True)

        # ASCII-mappen verplaatsen
        for folder in ["ascii", "ASCII", "asci", "asii"]:
            source_path_ = os.path.join(self.target_dir, folder)
            if os.path.exists(source_path_):
                for file_name in os.listdir(source_path_):
                    shutil.move(
                        os.path.join(
                            source_path_,
                            file_name
                        ), ascii_folder
                    )
                os.rmdir(source_path_)
                break

        # Rest bestanden
        rest_folder = os.path.join(self.root_dir, "rest")
        os.makedirs(rest_folder, exist_ok=True)

        for readme_file in ["README.doc", "Readme.doc"]:
            rest_folder_path = os.path.join(self.root_dir, readme_file)
            if os.path.exists(rest_folder_path):
                dest = os.path.join(
                    rest_folder,
                    f"{readme_file.split('.')[0]}{self.current_quarter}.doc"
                )
                shutil.move(rest_folder_path, dest)

        asc_nts_path = os.path.join(ascii_folder, "ASC_NTS.doc")
        if os.path.exists(asc_nts_path):
            dest = os.path.join(
                rest_folder,
                f"ASC_NTS{self.current_quarter}.doc"
            )
            shutil.move(asc_nts_path, dest)

        # Onnodige bestanden verwijderen
        for ext in ["*.pdf", "*.PDF", "*.doc", "*.zip"]:
            for file_path in glob.glob(os.path.join(self.root_dir, ext)):
                os.remove(file_path)

        for folder in ["deleted", "DELETED", "Deleted"]:
            folder_path = os.path.join(self.root_dir, folder)
            if os.path.exists(folder_path):
                os.rmdir(folder_path)

    def check_if_wget_installed(self):
        return shutil.which('wget') is not None

    def path_of_wget(self):
        return shutil.which('wget')

    def fetch_data(self):
        """
        Uses wget to download the data from the FDA website into
        the data directory.
        """
        download_confirm = f"Start downloading from {self.current_quarter}?"
        if not prompt(download_confirm):
            print("Downloading cancelled.")
            sys.exit(0)

        if not self.check_if_wget_installed():
            print("wget not found. Is it installed?")

        url = self.url.format(year_quarter=self.current_quarter)
        command = f"{self.path_of_wget()} -O {self.current_zip} {url}"

        try:
            subprocess.run(command, shell=True, check=True,
                           cwd=os.path.dirname(self.current_zip))
            print(f"Downloaded: {self.current_zip}")
        except subprocess.CalledProcessError as e:
            print(f"wget failed for {self.current_quarter}:\n {e}")
        except Exception as e:
            print(f"Download failed for {self.current_quarter}:\n {e}")


    def unzip_files(self):
        """
        Unzips the downloaded data into the target directory.
        """
        if os.path.exists(self.current_zip):
            os.makedirs(self.target_dir, exist_ok=True)
            with zipfile.ZipFile(self.current_zip, 'r') as zip_ref:
                zip_ref.extractall(self.target_dir)
            os.remove(self.current_zip)
        else:
            print(f"Zip_file ascii_{self.current_quarter} is missing")

def start_downloading_current(rootdir):
    download_files = DownloadFiles(rootdir)

__all__ = ["start_downloading_current"]
