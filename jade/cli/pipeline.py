"""CLI to show extensions."""

import logging
import os
import sys

import click

from jade.common import OUTPUT_DIR
from jade.loggers import setup_logging
from jade.jobs.pipeline_manager import PipelineManager
from jade.models import HpcConfig, LocalHpcConfig
from jade.models.submitter_params import DEFAULTS, SubmitterParams
from jade.utils.utils import get_cli_string, load_data


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
    "-b", "--per-node-batch-size",
    default=DEFAULTS["per_node_batch_size"],
    show_default=True,
    help="Number of jobs to run on one node in one batch."
)
@click.option(
    "-c", "--config-file",
    type=click.Path(),
    default=PipelineManager.CONFIG_FILENAME,
    show_default=True,
    help="pipeline config file."
)
@click.option(
    "-h", "--hpc-config",
    type=click.Path(),
    default=DEFAULTS["hpc_config_file"],
    show_default=True,
    help="HPC config file."
)
@click.option(
    "-l", "--local",
    is_flag=True,
    default=False,
    show_default=True,
    help="Run locally even if on HPC."
)
@click.option(
    "-n", "--max-nodes",
    default=DEFAULTS["max_nodes"],
    show_default=True,
    help="Max number of node submission requests to make in parallel."
)
@click.option(
    "-p", "--poll-interval",
    default=DEFAULTS["poll_interval"],
    type=float,
    show_default=True,
    help="Interval in seconds on which to poll jobs for status."
)
@click.option(
    "-q", "--num-processes",
    default=None,
    show_default=False,
    type=int,
    help="Number of processes to run in parallel; defaults to num CPUs."
)
@click.option(
    "--reports/--no-reports",
    is_flag=True,
    default=True,
    show_default=True,
    help="Generate reports after execution."
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable verbose log output."
)
def create(auto_config_cmds, per_node_batch_size, config_file, hpc_config,
        local, max_nodes, poll_interval, num_processes, reports, verbose):
    """Create a pipeline with multiple Jade configurations."""
    if local:
        hpc_config = HpcConfig(hpc_type="local", hpc=LocalHpcConfig())
    else:
        hpc_config = HpcConfig(**load_data(hpc_config))

    submit_params = SubmitterParams(
        generate_reports=reports,
        hpc_config=hpc_config,
        max_nodes=max_nodes,
        num_processes=num_processes,
        per_node_batch_size=per_node_batch_size,
        poll_interval=poll_interval,
        verbose=verbose,
    )
    PipelineManager.create_config(auto_config_cmds, config_file, submit_params)


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

    mgr = PipelineManager.create(config_file, output)
    try:
        mgr.submit_next_stage(0)
    except Exception:
        logger.exception("Pipeline execution failed")
        raise

    sys.exit(0)


@click.command()
@click.argument("output")
@click.option(
    "--stage-index",
    required=True,
    type=int,
    help="stage index to submit",
)
@click.option(
    "--return-code",
    required=True,
    type=int,
    help="return code of stage index that just completed",
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable verbose log output."
)
def try_submit(output, stage_index, return_code, verbose=False):
    """Submit the pipeline for execution."""
    filename = os.path.join(output, "pipeline_submit.log")
    level = logging.DEBUG if verbose else logging.INFO
    logger = setup_logging(__name__, filename, file_level=level,
                           console_level=level, mode="a")

    mgr = PipelineManager.load(output)
    try:
        mgr.submit_next_stage(stage_index, return_code=return_code)
    except Exception:
        logger.exception("Pipeline execution failed")
        raise

    sys.exit(0)


pipeline.add_command(create)
pipeline.add_command(submit)
pipeline.add_command(try_submit)
