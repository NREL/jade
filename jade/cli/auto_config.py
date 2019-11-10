"""CLI to automatically create a configuration."""

import importlib
import logging
import click

from jade.common import CONFIG_FILE
from jade.loggers import setup_logging
from jade.exceptions import InvalidExtension
from jade.extensions.registry import is_registered


# TODO: need one group command for auto-config; this should be a subcommand.


@click.option(
    "-v", "--verbose",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable verbose log output."
)
@click.option(
    "-c", "--config-file",
    default=CONFIG_FILE,
    show_default=True,
    help="config file to generate."
)
@click.argument("inputs")
@click.argument("extension")
@click.command()
def auto_config(extension, inputs, config_file, verbose):
    """Automatically create a configuration."""
    level = logging.DEBUG if verbose else logging.WARNING
    setup_logging("auto_config", None, console_level=level)

    # User extension
    if not is_registered(extension):
        raise InvalidExtension(f"Extension '{extension}' is not registered.")
    cli = importlib.import_module(f"jade.extensions.{extension}.cli")
    config = cli.auto_config(inputs)
    print(f"Created configuration with {config.get_num_jobs()} jobs.")
    config.dump(config_file)
    print(f"Dumped configuration to {config_file}.")
