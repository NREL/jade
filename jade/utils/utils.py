"""Utility functions for the jade package."""

from datetime import datetime, date
from pathlib import PosixPath, WindowsPath
from typing import Union
import enum
import functools
import gzip
import logging
import json
import os
import re
import shutil
import stat
import sys
from dateutil.parser import parse

import toml
from pydantic import BaseModel

from jade.exceptions import InvalidParameter
from jade.utils.timing_utils import timed_debug


MAX_PATH_LENGTH = 255

logger = logging.getLogger(__name__)


def create_chunks(items, size):
    """Returns a generator dividing items into chunks.

    items : list
    batch_size : int

    Returns
    -------
    generator

    """
    for i in range(0, len(items), size):
        yield items[i : i + size]


def create_script(filename, text, executable=True):
    """Creates a script with the given text.

    Parameters
    ----------
    text : str
        body of script
    filename : str
        file to create
    executable : bool
        if True, set as executable

    """
    # Permissions issues occur when trying to overwrite and then make
    # executable another user's file.
    if os.path.exists(filename):
        os.remove(filename)

    with open(filename, "w") as f_out:
        logger.info("Writing %s", filename)
        f_out.write(text)

    if executable:
        curstat = os.stat(filename)
        os.chmod(filename, curstat.st_mode | stat.S_IEXEC)


def make_directory_read_only(directory):
    """Set all files in the directory to be read-only.

    Parameters
    ----------
    directory : str

    """
    for filename in os.listdir(directory):
        make_file_read_only(os.path.join(directory, filename))

    logger.debug("Made all files in %s read-only", directory)


def make_file_read_only(filename):
    """Set the file to be read-only.

    Parameters
    ----------
    filename : str

    """
    os.chmod(filename, 0o444)
    logger.debug("Set %s as read-only", filename)


def _get_module_from_extension(filename, **kwargs):
    ext = os.path.splitext(filename)[1].lower()
    if ext == ".json":
        mod = json
    elif ext == ".toml":
        mod = toml
    # elif ext in (".yml", ".yaml"):
    #    mod = yaml
    elif "mod" in kwargs:
        mod = kwargs["mod"]
    else:
        raise InvalidParameter(f"Unsupported extension {filename}")

    return mod


@timed_debug
def dump_data(data, filename, **kwargs):
    """Dump data to the filename.
    Supports JSON, TOML, YAML, or custom via kwargs.

    Parameters
    ----------
    data : dict
        data to dump
    filename : str
        file to create or overwrite

    """
    mod = _get_module_from_extension(filename, **kwargs)
    with open(filename, "w") as f_out:
        mod.dump(data, f_out, **kwargs)

    logger.debug("Dumped data to %s", filename)


@timed_debug
def load_data(filename, **kwargs):
    """Load data from the file.
    Supports JSON, TOML, or custom via kwargs.
    YAML support could easily be added.

    Parameters
    ----------
    filename : str

    Returns
    -------
    dict

    """
    # TODO:  YAMLLoadWarning: calling yaml.load() without Loader=... is deprecated,
    #  as the default Loader is unsafe. Please read https://msg.pyyaml.org/load for full details.
    mod = _get_module_from_extension(filename, **kwargs)
    with open(filename) as f_in:
        try:
            data = mod.load(f_in)
        except Exception:
            logger.exception(f"Failed to load {filename}")
            raise

    logger.debug("Loaded data from %s", filename)
    return data


def aggregate_data_from_files(directory, end_substring, **kwargs):
    """Aggregate objects from files in directory matching end_substring.
    Refer to :func:`~jade.utils.utils.load_data` for supported file formats.

    Parameters
    ----------
    directory : str
    substring : str

    Returns
    -------
    list of dict

    """
    data = []
    for filename in os.listdir(directory):
        if filename.endswith(end_substring):
            path = os.path.join(directory, filename)
            data.append(load_data(path, **kwargs))

    return data


def rmtree(path):
    """Deletes the directory tree if it exists.

    Parameters
    ----------
    path : str

    """
    if os.path.exists(path):
        shutil.rmtree(path)

    logger.debug("Deleted %s", path)


def modify_file(filename, line_func, *args, **kwargs):
    """Modifies a file by running a function on each line.

    Parameters
    ----------
    filename : str
    line_func : callable
        Should return the line to write to the modified file.

    """
    tmp = filename + ".tmp"
    assert not os.path.exists(tmp), f"filename={filename} tmp={tmp}"

    with open(filename) as f_in:
        with open(tmp, "w") as f_out:
            for line in f_in:
                line = line_func(line, *args, **kwargs)
                f_out.write(line)

    shutil.move(tmp, filename)


def get_cli_string():
    """Return the command-line arguments issued.

    Returns
    -------
    str

    """
    return os.path.basename(sys.argv[0]) + " " + " ".join(sys.argv[1:])


def handle_file_not_found(func):
    """Decorator to catch FileNotFoundError exceptions."""

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
        except FileNotFoundError:
            msg = "one or more input parameters do not exist"
            logger.debug(msg, exc_info=True)
            raise InvalidParameter("{}: {}".format(msg, args[1:]))

        return result

    return wrapped


def handle_key_error(func):
    """Decorator to catch KeyError exceptions that happen because the user
    performs invalid actions."""

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
        except KeyError as err:
            msg = "invalid parameter: {}".format(err)
            logger.debug(msg, exc_info=True)
            raise InvalidParameter(msg)

        return result

    return wrapped


def decompress_file(filename):
    """Decompress a file.

    Parameters
    ----------
    filename : str

    Returns
    -------
    str
        Returns the new filename.

    """
    assert os.path.splitext(filename)[1] == ".gz"

    new_filename = filename[:-3]
    with open(new_filename, "wb") as f_out:
        with gzip.open(filename, "rb") as f_in:
            shutil.copyfileobj(f_in, f_out)

    os.remove(filename)
    logger.debug("Decompressed %s", new_filename)
    return new_filename


def get_directory_size_bytes(directory, recursive=True):
    """Return the total space consumed by all files in directory.

    Parameters
    ----------
    directory : str
    recursive : bool

    Returns
    -------
    int
        size in bytes

    """
    total = 0
    for dirpath, _, filenames in os.walk(directory):
        for filename in filenames:
            total += os.stat(os.path.join(dirpath, filename)).st_size
        if not recursive:
            break

    return total


def get_filenames_in_path(directory, filename, is_regex=False):
    """Return all files in directory matching filename, searching recursively.

    Parameters
    ----------
    directory : str
    ext : str
        file extension, ex: .log
    is_regex : bool
        Treat filename as regular expression.

    Returns
    -------
    generator

    """
    for dirpath, _, filenames in os.walk(directory):
        for _filename in filenames:
            matched = False
            if is_regex:
                matched = filename.search(_filename)
            else:
                matched = _filename == filename
            if matched:
                yield os.path.join(dirpath, _filename)


def get_filenames_by_ext(directory, ext):
    """Return filenames in directory, recursively, with file extension.

    Parameters
    ----------
    directory : str
    ext : str
        file extension, ex: .log

    Returns
    -------
    generator

    """
    for dirpath, _, filenames in os.walk(directory):
        for filename in filenames:
            if ext in filename:
                yield os.path.join(dirpath, filename)


def interpret_datetime(timestamp):
    """Return a datetime object from a timestamp string.

    Parameters
    ----------
    timestamp : str

    Returns
    -------
    datetime.datetime

    """
    formats = (
        "%Y-%m-%d_%H:%M:%S.%f",
        "%Y-%m-%d_%H-%M-%S-%f",
        "%Y-%m-%dT%H:%M:%SZ",
    )

    for i, fmt in enumerate(formats):
        try:
            return datetime.strptime(timestamp, fmt)
        except ValueError:
            if i == len(formats) - 1:
                raise
            continue


def standardize_timestamp(timestamp: Union[str, datetime]) -> str:
    """Validate string timestamp and output standard format."""
    stdfmt = "%Y-%m-%dT%H:%M:%S.%f"
    if isinstance(timestamp, datetime):
        return timestamp.strftime(stdfmt)

    dt = None
    formats = ("%Y-%m-%d_%H:%M:%S.%f", "%Y-%m-%d_%H-%M-%S-%f")
    for fmt in formats:
        try:
            dt = datetime.strptime(timestamp, fmt)
        except ValueError:
            continue

    if not dt:
        dt = parse(timestamp)

    return dt.strftime(stdfmt)


def rotate_filenames(directory, ext):
    """Rotates filenames in directory, recursively, to .1, .2, .etc.

    Parameters
    ----------
    directory : str
    ext : str
        file extension, ex: .log

    """
    regex_has_num = re.compile(r"(\d+)(?:\.[a-zA-Z]+)?$")

    for dirpath, _, filenames in os.walk(directory):
        files = []
        for filename in filter(lambda x: ext in x, filenames):
            new_names = set()
            match = regex_has_num.search(filename)
            if match:
                cur = int(match.group(1))
                new = cur + 1
                new_name = filename.replace(f"{ext}.{cur}", f"{ext}.{new}")
            else:
                cur = 0
                if not filename.endswith(ext):
                    # such as compressed files
                    split_ext = os.path.splitext(filename)
                    new_name = split_ext[0] + ".1" + split_ext[1]
                else:
                    new_name = filename + ".1"
            assert new_name not in new_names
            new_names.add(new_name)
            files.append((filename, cur, new_name))

        files.sort(key=lambda x: x[1], reverse=True)
        for filename in files:
            old = os.path.join(dirpath, filename[0])
            new = os.path.join(dirpath, filename[2])
            os.rename(old, new)
            logger.info("Renamed %s to %s", old, new)


def check_filename(name):
    """
    Validates that a name is valid for use as a filename or directory.
    Valid characters:  letters, numbers, underscore, hyphen, period

    Parameters
    ----------
    string: str,
        A given string.

    Raises
    ------
    InvalidParameter
        Raised if the name contains illegal characters or is too long.

    """
    if not re.search(r"^[\w\.-]+$", name):
        raise InvalidParameter(f"{name} contains illegal characters.")

    if len(name) > MAX_PATH_LENGTH:
        raise InvalidParameter(f"length of {name} is greater than the limit of {MAX_PATH_LENGTH}.")


def output_to_file(data, filename=None, stream=sys.stdout, indent=2):
    if filename is None and stream is None:
        raise InvalidParameter("must set either filename or stream")

    if filename is not None:
        ext = os.path.splitext(filename)[1]
        if ext not in (".json", ".toml"):
            raise InvalidParameter("Only .json and .toml are supported")

        with open(filename, "w") as f_out:
            _write_file(data, f_out, fmt=ext)
    else:
        _write_file(data)

    logger.info("Dumped configuration to %s", filename)


def _write_file(data, stream=sys.stdout, fmt=".json", indent=2):
    # Note: the default is JSON here because parsing 100 MB .toml files
    # is an order of magnitude slower.
    if fmt == ".json":
        json.dump(data, stream, indent=indent)
    elif fmt == ".toml":
        toml.dump(data, stream)
    else:
        assert False, fmt


class ExtendedJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, enum.Enum):
            return obj.value

        if isinstance(obj, PosixPath) or isinstance(obj, WindowsPath):
            return str(obj)

        if isinstance(obj, (datetime, date)):
            return standardize_timestamp(obj)

        if isinstance(obj, set):
            return list(obj)

        if isinstance(obj, BaseModel):
            return obj.dict()

        return json.JSONEncoder.default(self, obj)
