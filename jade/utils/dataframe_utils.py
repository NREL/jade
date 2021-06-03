"""Contains pandas dataframe utility functions."""

import gzip
import logging
import os
import re
import shutil

import pandas as pd

from jade.exceptions import InvalidParameter
from jade.utils.timing_utils import timed_debug


logger = logging.getLogger(__name__)


@timed_debug
def read_dataframe(filename, index_col=None, columns=None, parse_dates=False, **kwargs):
    """Convert filename to a dataframe. Supports .csv, .json, .feather, .h5.
    Handles compressed files.

    Parameters
    ----------
    filename : str
    index_col : str | int | None
        Index column name or index
    columns : list or None
        Use these columns if the file is CSV and does not define them.
    parse_dates : bool
    kwargs : kwargs
        Passed to underlying library for dataframe conversion.
        Consider setting parse_dates=True if the index is a timestamp.

    Returns
    -------
    pd.DataFrame

    Raises
    ------
    FileNotFoundError
        Raised if the file does not exist.

    """
    if not os.path.exists(filename):
        raise FileNotFoundError("filename={} does not exist".format(filename))

    needs_new_index = False
    ext = os.path.splitext(filename)
    if ext[1] == ".gz":
        ext = os.path.splitext(ext[0])[1]
        open_func = gzip.open
    else:
        ext = ext[1]
        open_func = open

    if ext == ".csv":
        df = pd.read_csv(
            filename, index_col=index_col, usecols=columns, parse_dates=parse_dates, **kwargs
        )
    elif ext == ".json":
        df = pd.read_json(filename, **kwargs)
    elif ext == ".feather":
        needs_new_index = True
        with open_func(filename, "rb") as f_in:
            df = pd.read_feather(f_in, **kwargs)
    elif ext == ".h5":
        # This assumes that the file has a single dataframe, and so the
        # key name is not relevant.
        df = pd.read_hdf(filename, **kwargs)
        needs_new_index = True
    else:
        raise InvalidParameter(f"unsupported file extension {ext}")

    if index_col is not None and needs_new_index:
        df.set_index(index_col, inplace=True)
        if parse_dates:
            df.set_index(pd.to_datetime(df.index), inplace=True)

    return df


def read_dataframe_handle_missing(filename, index_col=None, columns=None):
    """Convert filename to a dataframe. Returns None if the file is missing.

    Parameters
    ----------
    filename : str
    index_col : str | int | None
        Index column name or index
    columns : list or None
        Use these columns if the file is CSV and does not define them.

    Returns
    -------
    pd.DataFrame | None

    """
    if not os.path.exists(filename):
        directory = os.path.split(filename)[0]
        if os.path.exists(directory) and not os.listdir(directory):
            logger.warning("missing data %s", filename)
            return None

        raise InvalidParameter(f"directory={directory} does not exist.")

    return read_dataframe(filename, index_col=index_col, columns=columns)


def read_dataframe_by_substring(directory, substring, index_col=None, parse_dates=False, **kwargs):
    """Return a dataframe for the file containing substring.

    Parameters
    ----------
    directory : str
    substring : str
        identifier for output file, must be unique in directory
    index_col : str | int | None
        Index column name or index
    kwargs : kwargs
        Passed to underlying library for dataframe conversion.

    Returns
    -------
    pd.DataFrame

    """
    files = [x for x in os.listdir(directory) if substring in x]

    # Exclude any files that may have rolled, such as
    # Circuits-Losses-1-2.feather.1.gz
    regex = re.compile(r"\.\w+\.\d+(?:\.\w+)?$")
    files = [x for x in files if regex.search(x) is None]

    if not files:
        return None

    assert len(files) == 1, f"found multiple {substring} files in {directory}"

    filename = files[0]
    return read_dataframe(
        os.path.join(directory, filename), index_col=index_col, parse_dates=parse_dates, **kwargs
    )


def read_dataframes_by_substrings(
    directory, substrings, index_col=None, parse_dates=False, **kwargs
):
    """Return dataframes for files in the directory.

    Parameters
    ----------
    directory : str
    substrings : tuple
        File substring (str) for output files
    index_col : str | int | None
        Index column name or index
    kwargs : kwargs
        Passed to underlying library for dataframe conversion.

    Returns
    -------
    dict
        dictionary of substring to pd.DataFrame

    """
    dfs = {}

    for substring in substrings:
        df = read_dataframe_by_substring(
            directory,
            substring,
            index_col=index_col,
            parse_dates=parse_dates,
            **kwargs,
        )
        if df is not None:
            dfs[substring] = df

    return dfs


@timed_debug
def write_dataframe(df, file_path, compress=False, keep_original=False, **kwargs):
    """Write the dataframe to a file with in a format matching the extension.

    Note that the feather and h5 formats do not support row indices.
    Index columns will be lost for those formats. If the dataframe has an index
    then it should be converted to a column before calling this function.

    This function only supports storing a single dataframe inside an HDF5 file.
    It always uses the key 'data'.

    Parameters
    ----------
    df : pd.DataFrame
    file_path : str
    compress : bool
    keep_original : bool
    kwargs : pass keyword arguments to underlying library

    Raises
    ------
    InvalidParameter if the file extension is not supported.
    InvalidParameter if the DataFrame index is set.

    """
    if not isinstance(df.index, pd.RangeIndex) and not isinstance(
        df.index, pd.core.indexes.base.Index
    ):
        raise InvalidParameter("DataFrame index must not be set")

    ext = os.path.splitext(file_path)[1]

    if ext == ".csv":
        df.to_csv(file_path, **kwargs)
    elif ext == ".feather":
        df.to_feather(file_path, **kwargs)
    elif ext == ".h5":
        # HDF5 supports built-in compression, levels 1-9
        if "complevel" in kwargs:
            complevel = kwargs["complevel"]
        elif compress:
            complevel = 9
        else:
            complevel = 0
        df.to_hdf(file_path, "data", mode="w", complevel=complevel, **kwargs)
    elif ext == ".json":
        df.to_json(file_path, **kwargs)
    else:
        raise InvalidParameter(f"unsupported file extension {ext}")

    logger.debug("Created %s", file_path)

    if compress and ext != ".h5":
        zipped_path = file_path + ".gz"
        with open(file_path, "rb") as f_in:
            with gzip.open(zipped_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        if not keep_original:
            os.remove(file_path)

        file_path = zipped_path
        logger.debug("Compressed %s", zipped_path)

    return file_path


# TODO: broken with newer versions of pandas because of mishandling of indices
# def convert_csvs_to_feather(directory, compress=False, column_map=None,
#                            exclude_substrings=None, keep_original=False):
#    """Load CSV files as dataframes and convert them to feather files.
#
#    Parameters
#    ----------
#    directory : str
#        Convert all CSV files in the directory.
#    compress : bool
#        Compress the feather files.
#    column_map : dict
#        For CSV files without a header, pass basename-to-columns mapping
#    exclude_substrings : list
#        List of filename substrings to exclude.
#
#    """
#    for filename in os.listdir(directory):
#        skip = False
#        for substring in exclude_substrings:
#            if substring is not None and substring in filename:
#                skip = True
#                break
#        if skip:
#            continue
#        if os.path.splitext(filename)[1] == ".csv":
#            convert_csv_to_feather(os.path.join(directory, filename),
#                                   compress=compress,
#                                   column_map=column_map,
#                                   keep_original=keep_original)
#
#
# def convert_csv_to_feather(file_path, compress=False, column_map=None,
#                           keep_original=False):
#    """Load CSV files as dataframes and convert them to feather files.
#
#    Parameters
#    ----------
#    directory : str
#    filename : str
#    compress : bool
#        Compress the feather files.
#    column_map: dict
#        For CSV files without a header, pass basename-to-columns mapping
#
#    """
#    directory = os.path.dirname(file_path)
#    filename = os.path.basename(file_path)
#
#    assert os.path.splitext(filename)[1] == ".csv"
#
#    columns = None
#    if column_map is not None:
#        columns = column_map.get(os.path.basename(filename))
#
#    df = read_dataframe(os.path.join(directory, filename), columns=columns)
#    new_filename = os.path.splitext(filename)[0] + ".feather"
#    write_dataframe(df, os.path.join(directory, new_filename),
#                    compress=compress)
#
#    if not keep_original:
#        os.remove(os.path.join(directory, filename))
#
#    logger.debug("Converted %s: %s to %s", directory, filename, new_filename)
