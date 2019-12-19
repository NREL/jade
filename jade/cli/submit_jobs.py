"""CLI to run a scenario."""

import logging
import os
import sys

import click

from jade.jobs.job_submitter import DEFAULTS, JobSubmitter
from jade.loggers import setup_logging
from jade.utils.utils import makedirs, rotate_filenames, get_cli_string


logger = logging.getLogger(__name__)


@click.argument(
    "config-file",
    type=str,
)
@click.option(
    "-b", "--per-node-batch-size",
    default=DEFAULTS["per_node_batch_size"],
    show_default=True,
    help="Number of jobs to run on one node in one batch."
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
    "-o", "--output",
    default=DEFAULTS["output"],
    show_default=True,
    help="Output directory."
)
@click.option(
    "-p", "--poll-interval",
    default=DEFAULTS["poll_interval"],
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
    "--rotate-logs/--no-rotate-logs",
    default=True,
    show_default=True,
    help="Rotate log files so that they aren't overwritten."
)
@click.option(
    "--rotate-tomls/--no-rotate-tomls",
    default=True,
    show_default=True,
    help="Rotate config and results files so that they aren't overwritten."
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable verbose log output."
)
@click.command()
def submit_jobs(
        config_file, per_node_batch_size, hpc_config, local, max_nodes,
        output, poll_interval, num_processes, rotate_logs, rotate_tomls,
        verbose):
    """Submits jobs for execution, locally or on HPC."""
    makedirs(output)
    if rotate_logs:
        rotate_filenames(output, ".log")
    if rotate_tomls:
        rotate_filenames(output, ".toml")

    filename = os.path.join(output, "submit_jobs.log")
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging(__name__, filename, file_level=level, console_level=level)
    logger.info(get_cli_string())

    mgr = JobSubmitter(config_file, hpc_config=hpc_config, output=output)
    ret = mgr.submit_jobs(
        per_node_batch_size=per_node_batch_size,
        max_nodes=max_nodes,
        force_local=local,
        verbose=verbose,
        poll_interval=poll_interval,
        num_processes=num_processes,
    )
    sys.exit(ret.value)
