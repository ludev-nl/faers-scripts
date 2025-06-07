"""
Pipeline wide: Reading
out options from the options file.
"""

import json

from error import get_logger, fatal_error
log = get_logger()

def load_options_json(file_path: str) -> dict:
    """
    Loads the options file.
    """
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError:
        log.warning(f"Invalid JSON in \n{file_path}\nPlease fix this file.")
        return {}
    except FileNotFoundError:
        log.warning(f"File not found here\n{file_path}\nMake sure it exists.")
        return {}
    except Exception as e:
        raise fatal_error(f"Error while decoding {file_path}", e, 1)

def get_option_from_json(file_path: str, key: str) -> bool | str | None:
    """
    Reads out an option value from the options file.
    """
    options = load_options_json(file_path)

    if options is {}:
        # TODO: return False if bool can we detect that even?
        log.warning(
            f"Continuing with default options, {key} = None.")
        return None

    try:
        value = options
        for k in key.split('.'):
            value = value[k]
        if isinstance(value, bool):
            return value
        elif isinstance(value, str):
            return value
        else:
            log.warning(
                f"Value for {key} is not a boolean or string."
                f"Use true|false without double quotes, or a string."
                )
            return None
    except (KeyError, TypeError):
        log.warning(f"Option {key} not found or incorrect type.")
        return None

__all__ = ["get_option_from_json"]
