"""Contains logging configuration data."""

import logging
import logging.config

from jade.extensions.registry import Registry


def setup_logging(
    name, filename, console_level=logging.INFO, file_level=logging.INFO, mode="w", packages=None
):
    """Configures logging to file and console.

    Parameters
    ----------
    name : str
        logger name
    filename : str | None
        log filename
    console_level : int, optional
        console log level
    file_level : int, optional
        file log level
    packages : list, optional
        enable logging for these package names

    """
    log_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "basic": {"format": "%(message)s"},
            "short": {
                "format": "%(asctime)s - %(levelname)s [%(name)s "
                "%(filename)s:%(lineno)d] : %(message)s",
            },
            "detailed": {
                "format": "%(asctime)s - %(levelname)s [%(name)s "
                "%(filename)s:%(lineno)d] : %(message)s",
            },
        },
        "handlers": {
            "console": {
                "level": console_level,
                "formatter": "short",
                "class": "logging.StreamHandler",
            },
            "file": {
                "class": "logging.FileHandler",
                "level": file_level,
                "filename": filename,
                "mode": mode,
                "formatter": "detailed",
            },
            "structured_file": {
                "class": "logging.FileHandler",
                "level": file_level,
                "filename": filename,
                "mode": "a",
                "formatter": "basic",
            },
        },
        "loggers": {
            name: {"handlers": ["console", "file"], "level": "DEBUG", "propagate": False},
            "event": {
                "handlers": ["console", "structured_file"],
                "level": "DEBUG",
                "propagate": False,
            },
        },
        # "root": {
        #    "handlers": ["console", "file"],
        #    "level": "WARN",
        # },
    }

    logging_packages = set(Registry().list_loggers())
    if packages is not None:
        for package in packages:
            logging_packages.add(package)

    for package in logging_packages:
        log_config["loggers"][package] = {
            "handlers": ["console", "file"],
            "level": "DEBUG",
            "propagate": False,
        }

    if filename is None:
        log_config["handlers"].pop("file")
        log_config["loggers"][name]["handlers"].remove("file")
        for package in logging_packages:
            if "file" in log_config["loggers"][package]["handlers"]:
                log_config["loggers"][package]["handlers"].remove("file")

    # For event logging
    if name == "event":
        log_config["handlers"].pop("file")
        for package in logging_packages:
            log_config["loggers"].pop(package)
    else:
        log_config["handlers"].pop("structured_file")
        log_config["loggers"]["event"]["handlers"].remove("structured_file")

    logging.config.dictConfig(log_config)
    logger = logging.getLogger(name)

    return logger


def log_event(event):
    """
    Log a structured job event into log file

    Parameters
    ----------
    event: :obj:`StructuredLogEvent`
        An instance of :obj:`StructuredLogEvent`

    """
    logger = logging.getLogger("event")
    logger.info(event)
