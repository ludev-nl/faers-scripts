"""
Pipeline wide: Logging and
fatal error handling.
"""
import sys
import traceback
import logging
import os
import json
from datetime import datetime
import zoneinfo

from constants import LOG_DIR
rootLogger = None

class InfoWarningFilter(logging.Filter):
    """
    Defines a custom filter to log events
    with level below ERROR.
    """
    def filter(self, record):
        return record.levelno < logging.ERROR

def make_logs_folder_on_our_own():
    """
    Checks if the logs directory exists, so we can start the logger.
    This does not use the logging framework, since it is intended
    to be run before the logger has been initialised and the program
    actually starts running.
    """
    config_path = os.path.join('config', 'general_options.json')

    if not os.path.exists(config_path):
        print(f"The configuration file {config_path} does not exist.")
        print(f"Please make sure you are calling from the right folder.")
        return

    try:
        with open(config_path, 'r') as config_file:
            config_data = json.load(config_file)
            root_data_dir = config_data.get('root_data_dir', None)

            if root_data_dir is None:
                print(f"The 'root_data_dir' key"
                      f"is not present in the configuration file.")
                return

            logs_dir = os.path.join(root_data_dir, 'logs')

            if not os.path.exists(logs_dir):
                os.makedirs(logs_dir, exist_ok=True)
                print(f"Created logs directory at: {logs_dir}")
            # Otherwise we fail silently. There
            # is not problem if the logs folder already exists.

    except json.JSONDecodeError:
        print(f"Failed to parse the JSON in {config_path}.")
    except PermissionError:
        print(f"Permission denied when"
              f"accessing or creating the logs directory.")
    except OSError as e:
        print(f"An error occurred while"
              f"checking or creating the logs directory: {e}")

def format_log_filename() -> str:
    """
    Formats the filename of the log file.
    """
    amsterdam_tz = zoneinfo.ZoneInfo('Europe/Amsterdam')
    utc_time_with_timezone = datetime.now(amsterdam_tz)
    return utc_time_with_timezone.strftime('%Y-%m-%d_%H-%M-%S')

def setup_logger():
    """
    Setup of the root logger with handlers:
        - write >= DEBUG to a log file
        - print >= CRITICAL to stderr
    """
    global rootLogger
    if rootLogger is None:
        # Ensure the log directory exists
        make_logs_folder_on_our_own()
        log_file_name = format_log_filename()

        rootLogger = logging.getLogger()
        rootLogger.setLevel(logging.DEBUG)

    if not rootLogger.hasHandlers():
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(
            logging.Formatter(
                "%(message)s")
        )
        consoleHandler.setLevel(logging.DEBUG)
        consoleHandler.addFilter(InfoWarningFilter())
        rootLogger.addHandler(consoleHandler)

        fileHandler = logging.FileHandler(
            "{0}/{1}.log".format(LOG_DIR, log_file_name))
        fileHandler.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)-8.8s] %(message)s")
        )
        fileHandler.setLevel(logging.DEBUG)
        rootLogger.addHandler(fileHandler)

    return rootLogger

def get_logger():
    """
    Returns the logger object which you can use for logging.
    Initialises the logger if it does not already exist.
    Example usage:
    | from .error import get_logger
    | log = get_logger()
    | log.info("This is a message at the info level.")
    """
    if rootLogger is None:
        setup_logger()
    return rootLogger

def fatal_error(msg: str, err: Exception, exit_code: int = 1):
    """
    Handle fatal errors. This class should be raised, i.e.
    | except Exception from e:
    |     raise fatal_error(e, msg = "error", exit_code = 1)
    """
    log_file_name = format_log_filename()
    log = setup_logger()

    log.error(msg)
    log.exception(err)

    red = "\33[31m"
    esc = "\33[0m"
    print(
        f"{red}Fatal error occurred{esc}:\n"
        f"{msg}\n"
        f"For more information, see the log file here:\n"
        f"{os.path.join(log_file_name + '.log')}"
    )
    sys.exit(exit_code)

__all__ = ["handle_fatal_error, get_logger", "log"]
