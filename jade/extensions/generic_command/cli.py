"""Generic command CLI"""

import os

from jade.jobs.job_configuration_factory import create_config_from_file
from jade.extensions.generic_command.generic_command_configuration import GenericCommandConfiguration
from jade.extensions.generic_command.generic_command_inputs import GenericCommandInputs
from jade.extensions.generic_command.generic_command_execution import GenericCommandExecution


def auto_config(inputs):
    """Create a configuration file for generic_command.
    
    Parameters
    ----------
    inputs : str
        Input file containing commands, one line per command

    """
    if not os.path.exists(inputs):
        raise OSError(f"Inputs path '{inputs}' does not exist.")

    job_inputs = GenericCommandInputs(inputs)
    config = GenericCommandConfiguration(job_inputs=job_inputs)
    for job_param in config.inputs.iter_jobs():
        config.add_job(job_param)

    return config


def run(config_file, name, output, output_format, verbose):
    """Run auto regression analysis through command line"""
    assert False
