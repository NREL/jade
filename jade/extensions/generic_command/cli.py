"""Generic command CLI"""

import os

from jade.extensions.generic_command import GenericCommandConfiguration


def auto_config(inputs, **kwargs):
    """Create a configuration file for generic_command.

    Parameters
    ----------
    inputs : str
        Input file containing commands, one line per command

    """
    if not os.path.exists(inputs):
        raise OSError(f"Inputs path '{inputs}' does not exist.")

    return GenericCommandConfiguration.auto_config(inputs, **kwargs)


def run(config_file, name, output, output_format, verbose):
    """Run auto regression analysis through command line"""
    assert False
