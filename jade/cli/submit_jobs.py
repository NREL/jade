"""CLI to run jobs."""

import logging
import os
import sys

import click

from jade.common import OUTPUT_DIR
from jade.enums import Status
from jade.jobs.job_submitter import JobSubmitter
from jade.loggers import setup_logging
from jade.models import HpcConfig, LocalHpcConfig
from jade.models.submitter_params import DEFAULTS, SubmitterParams
from jade.jobs.cluster import Cluster
from jade.utils.utils import get_cli_string, load_data, rotate_filenames


logger = logging.getLogger(__name__)


@click.command()
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
    envvar="LOCAL_SUBMITTER",
    help="Run on local system. Optionally, set the environment variable "
         "LOCAL_SUBMITTER=1."
)
@click.option(
    "-n", "--max-nodes",
    default=DEFAULTS["max_nodes"],
    show_default=True,
    help="Max number of node submission requests to make in parallel."
)
@click.option(
    "-o", "--output",
    default=OUTPUT_DIR,
    show_default=True,
    help="Output directory."
)
@click.option(
    "-p", "--poll-interval",
    default=DEFAULTS["poll_interval"],
    type=float,
    show_default=True,
    help="Interval in seconds on which to poll jobs for status."
)
@click.option(
    "-r", "--resource-monitor-interval",
    default=DEFAULTS["resource_monitor_interval"],
    type=int,
    show_default=True,
    help="interval in seconds on which to collect resource stats."
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
    "--verbose",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable verbose log output."
)
@click.option(
    "--restart-failed",
    is_flag=True,
    default=False,
    show_default=True,
    help="Restart only failed jobs."
)
@click.option(
    "--restart-missing",
    is_flag=True,
    default=False,
    show_default=True,
    help="Restart only missing jobs."
)
@click.option(
    "--reports/--no-reports",
    is_flag=True,
    default=True,
    show_default=True,
    help="Generate reports after execution."
)
@click.option(
    "--try-add-blocked-jobs/--no-try-add-blocked-jobs",
    is_flag=True,
    default=True,
    show_default=True,
    help="Add blocked jobs to a node's batch if they are blocked by jobs "
         "already in the batch."
)
def submit_jobs(
        config_file, per_node_batch_size, hpc_config, local, max_nodes,
        output, poll_interval, resource_monitor_interval, num_processes, rotate_logs,
        verbose, restart_failed, restart_missing, reports, try_add_blocked_jobs):
    """Submits jobs for execution, locally or on HPC."""
    os.makedirs(output, exist_ok=True)
    if rotate_logs:
        rotate_filenames(output, ".log")

    filename = os.path.join(output, "submit_jobs.log")
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging(__name__, filename, file_level=level, console_level=level, mode="w")
    logger.info(get_cli_string())

    event_file = os.path.join(output, "submit_jobs_events.log")
    # This effectively means no console logging.
    setup_logging("event", event_file, console_level=logging.ERROR,
                  file_level=logging.INFO)

    if local:
        hpc_config = HpcConfig(hpc_type="local", hpc=LocalHpcConfig())
    else:
        if not os.path.exists(hpc_config):
            print(f"{hpc_config} does not exist. Generate it with 'jade config hpc' "
                   "or run in local mode with '-l'")
            sys.exit(1)
        hpc_config = HpcConfig(**load_data(hpc_config))

    params = SubmitterParams(
        generate_reports=reports,
        hpc_config=hpc_config,
        max_nodes=max_nodes,
        num_processes=num_processes,
        per_node_batch_size=per_node_batch_size,
        poll_interval=poll_interval,
        resource_monitor_interval=resource_monitor_interval,
        try_add_blocked_jobs=try_add_blocked_jobs,
        verbose=verbose,
    )

    ret = JobSubmitter.run_submit_jobs(
        config_file,
        output,
        params,
        restart_failed,
        restart_missing,
    )
    sys.exit(ret)
