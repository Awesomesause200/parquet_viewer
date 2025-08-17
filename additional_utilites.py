import pandas as pd
import pyarrow


def merge_settings(defaults, saved) -> dict:
    """
    This function is a helper function to read the configuration file, handling each case individually

    :param defaults: Default configuration
    :param saved: Saved configuration
    :return: merged: dictionary with a dictionary of configurations
    """
    merged: dict = {}
    for key, default_val in defaults.items():
        if isinstance(default_val, dict):
            saved_val = saved.get(key, {}) if isinstance(saved.get(key), dict) else {}
            merged[key] = {**default_val, **saved_val}
        else:
            merged[key] = saved.get(key, default_val)
    return merged


def read_data(reader, stream) -> pd.DataFrame | None:
    """
    This function helps us by handling errors automatically. While the errors aren't handled directly, we can use them
    to debug behavior

    :param reader: function to read (can be python function or pandas methods)
    :param stream: file contents to input within the reader
    :return:
    """
    try:
        return reader(stream)
    except pyarrow.lib.ArrowInvalid:
        ...
    except pd.errors.EmptyDataError:
        ...
    return None
