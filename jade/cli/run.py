"""CLI to run hosting capacity analysis on results for a feeder."""

import logging
import os
import sys

import click

from jade.common import OUTPUT_DIR
from jade.loggers import setup_logging
from jade.jobs.job_post_process import JobPostProcess
from jade.utils.utils import get_cli_string, load_data
from jade.exceptions import InvalidExtension, ExecutionError
from jade.extensions.registry import Registry, ExtensionClassType


@click.argument("extension")
@click.option(
    "-n", "--name",
    required=True,
    type=str,
    help="The name of the job that needs to run.",
)
@click.option(
    "-o", "--output",
    default=OUTPUT_DIR,
    show_default=True,
    help="Output directory."
)
@click.option(
    "--config-file",
    required=True,
    help="Job configuration file"
)
@click.option(
    "-f", "--output-format",
    default="csv",
    show_default=True,
    help="Output format for data (csv or json)."
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable verbose log output."
)
@click.command()
def run(extension, **kwargs):
    """Runs individual job."""
    registry = Registry()
    if not registry.is_registered(extension):
        raise InvalidExtension(f"Extension '{extension}' is not registered.")

    # Parse Argument
    config_file = kwargs["config_file"]
    name = kwargs["name"]
    output = kwargs["output"]
    output_format = kwargs["output_format"]
    verbose = kwargs["verbose"]
    level = logging.DEBUG if verbose else logging.INFO

    # Create directory for current job
    job_dir = os.path.join(output, name)
    os.makedirs(job_dir, exist_ok=True)
    # General logging setup
    log_file = os.path.join(job_dir, "run.log")
    general_logger = setup_logging(extension, log_file, console_level=level, file_level=level)
    general_logger.info(get_cli_string())

    # Structural logging setup
    event_file = os.path.join(job_dir, "events.log")
    setup_logging("event", event_file, console_level=level, file_level=level)

    # Create config for run
    try:
        cli = registry.get_extension_class(extension, ExtensionClassType.CLI)
        ret = cli.run(config_file, name, output, output_format, verbose)
    except ExecutionError as err:
        msg = f"unexpected exception in run '{extension}' job={name} - {err}"
        general_logger.exception(msg)
        ret = 1

    if ret == 0:
        try:
            config = load_data(config_file)
            if "job_post_process_config" in config.keys():
                post_process = JobPostProcess(*config['job_post_process_config'].values(),
                                            job_name=name, output=output)
                post_process.run(config_file=config_file, output=output)
        except ExecutionError as err:
            msg = f"unexpected exception in post-process '{extension}' job={name} - {err}"
            general_logger.exception(msg)
            ret = 1

    sys.exit(ret)
