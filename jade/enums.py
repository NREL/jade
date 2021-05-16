"""Enums for the jade package."""

import enum
import re

import toml


class Status(enum.Enum):
    """Return status."""

    GOOD = 0
    ERROR = 1
    IN_PROGRESS = 2


class Mode(enum.Enum):
    """Possible values for computational sequencing mode"""

    PARALLEL = "parallel"
    SEQUENTIAL = "sequential"


class JobCompletionStatus(enum.Enum):
    """Possible values for job completion status"""

    FINISHED = "finished"  # Job ran. return code could be 0 or 1
    CANCELED = "canceled"  # Job never ran.
    MISSING = "missing"  # No result recorded. Happens with walltime timeout.


PUBLIC_ENUMS = {
    "Mode": Mode,
    "JobCompletionStatus": JobCompletionStatus,
}


_REGEX_ENUM = re.compile(r"^(\w+)\.(\w+)$")


def get_enum_from_str(string):
    """Converts a enum that's been written as a string back to an enum.

    Parameters
    ----------
    string : str
        string to convert

    Returns
    -------
    tuple
        converted, converted enum or original

    """
    converted = False
    match = _REGEX_ENUM.search(string)
    if match and match.group(1) in PUBLIC_ENUMS:
        obj = getattr(PUBLIC_ENUMS[match.group(1)], match.group(2))
        converted = True
    else:
        obj = string

    return converted, obj


def get_enum_from_value(cls, value):
    """Gets the enum for the given value."""
    for enum_ in cls:
        if enum_.value == value:
            return enum_
    raise Exception("Unknown value: {} {}".format(cls, value))


class EnumEncoder(toml.TomlEncoder):
    """Custom encoder for enums."""

    def __init__(self, _dict=dict, preserve=False):
        super(EnumEncoder, self).__init__(_dict, preserve)
        self.dump_funcs[enum.Enum] = EnumEncoder.dump_enum

    @staticmethod
    def dump_enum(val):
        """Return the enum value converted to string."""
        return str(val.value)
