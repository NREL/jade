"""
Demo cli
"""
import os
from jade.jobs.job_configuration_factory import create_config_from_file
from jade.extensions.demo.autoregression_configuration import AutoRegressionConfiguration
from jade.extensions.demo.autoregression_inputs import AutoRegressionInputs
from jade.extensions.demo.autoregression_execution import AutoRegressionExecution


def auto_config(inputs, **kwargs):
    """
    Create a configuration file for demo

    :param inputs: str, the path to directory containing autoregression data.

    :return: None
    """
    if not os.path.exists(inputs):
        raise OSError(f"Inputs path '{inputs}' does not exist.")

    job_inputs = AutoRegressionInputs(inputs)
    config = AutoRegressionConfiguration(**kwargs)
    for job_param in job_inputs.iter_jobs():
        config.add_job(job_param)

    return config


def run(config_file, name, output, output_format, verbose):
    """Run auto regression analysis through command line"""
    os.makedirs(output, exist_ok=True)

    config = create_config_from_file(config_file)
    job = config.get_job(name)

    execution = AutoRegressionExecution(job=job, output=output, output_format=output_format)
    execution.run()
