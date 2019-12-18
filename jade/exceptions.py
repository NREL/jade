"""Common exceptions for jade package"""


class ExecutionError(Exception):
    """Raised when execution fails."""


class InvalidConfiguration(Exception):
    """Raised when the configuration is invalid."""


class InvalidParameter(Exception):
    """Raised when bad user input is detected."""


class JobAlreadyInProgress(Exception):
    """Raised when a local job is started while another is in progress."""


class UserAbort(Exception):
    """Raised when the user has aborted the operation."""


class InvalidExtension(Exception):
    """Raise when extension does not exist in EXTENSIONS"""
