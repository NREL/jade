"""CLI to run hosting capacity analysis on results for a feeder."""

import importlib
import logging
import os
import sys

import click

from jade.common import OUTPUT_DIR
from jade.loggers import setup_logging
from jade.utils.utils import get_cli_string
from jade.exceptions import InvalidExtension, ExecutionError
from jade.extensions.registry import is_registered


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
    """Runs hosting capacity analysis on results for a feeder."""
    if not is_registered(extension):
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
        cli = importlib.import_module(f"jade.extensions.{extension}.cli")
        ret = cli.run(config_file, name, output, output_format, verbose)
    except ExecutionError as err:
        msg = f"unexpected exception in run '{extension}' job={name} - {err}"
        general_logger.exception(msg)
        ret = 1

    sys.exit(ret)
