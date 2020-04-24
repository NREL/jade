"""CLI to show extensions."""

import logging
import os
import sys

import click

from jade.common import OUTPUT_DIR
from jade.loggers import setup_logging
from jade.jobs.pipeline_manager import PipelineManager
from jade.utils.utils import get_cli_string


logger = logging.getLogger(__name__)


@click.group()
def pipeline():
    """Manage JADE execution pipeline."""
    setup_logging("pipeline", None)


@click.command()
@click.argument(
    "auto-config-cmds",
    nargs=-1,
)
@click.option(
    "-p", "--submit-params",
    type=click.STRING,
    help="optional params in jade submit-jobs."
)
@click.option(
    "-c", "--config-file",
    type=click.Path(),
    default="pipeline.toml",
    show_default=True,
    help="pipeline config file."
)
def create(auto_config_cmds, config_file, submit_params):
    """Create a pipeline with multiple Jade configurations."""
    PipelineManager.create(auto_config_cmds, config_file, submit_params)


@click.command()
@click.argument("config-file")
@click.option(
    "-o", "--output",
    default=OUTPUT_DIR,
    show_default=True,
    help="Output directory."
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable verbose log output."
)
def submit(config_file, output, verbose=False):
    """Submit the pipeline for execution."""
    global logger
    os.makedirs(output, exist_ok=True)
    filename = os.path.join(output, "pipeline_submit.log")
    level = logging.DEBUG if verbose else logging.INFO
    logger = setup_logging(__name__, filename, file_level=level,
                           console_level=level)

    logger.info(get_cli_string())

    mgr = PipelineManager(config_file, output)
    try:
        mgr.submit(verbose=verbose)
    except Exception:
        logger.exception("Pipeline execution failed")
        sys.exit(1)

    sys.exit(0)


pipeline.add_command(create)
pipeline.add_command(submit)
