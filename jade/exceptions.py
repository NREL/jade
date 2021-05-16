"""Common exceptions for jade package"""


class JadeBaseException(Exception):
    """All JADE exceptions should derive from this class."""


class ExecutionError(JadeBaseException):
    """Raised when execution fails."""


class InvalidConfiguration(JadeBaseException):
    """Raised when the configuration is invalid."""


class InvalidParameter(JadeBaseException):
    """Raised when bad user input is detected."""


class JobAlreadyInProgress(JadeBaseException):
    """Raised when a local job is started while another is in progress."""


class UserAbort(JadeBaseException):
    """Raised when the user has aborted the operation."""


class InvalidExtension(JadeBaseException):
    """Raise when extension does not exist in EXTENSIONS"""
