"""
Batch post-process extension cli
"""
import logging
import os

from jade.exceptions import ExecutionError
from jade.jobs.job_configuration_factory import create_config_from_file
from jade.extensions.batch_post_process.batch_post_process_execution import \
    BatchPostProcessExecution


logger = logging.getLogger(__name__)


def auto_config(inputs, **kwargs):
    pass


def run(config_file, name, output, output_format, verbose):
    """Run batch post-processing through command line"""
    os.makedirs(output, exist_ok=True)

    config = create_config_from_file(config_file)
    job = config.get_job(name)

    try:
        execution = BatchPostProcessExecution(job=job, output=output)
        ret = execution.run()
    except ExecutionError as e:
        logger.exception(e)
        raise

    return ret
