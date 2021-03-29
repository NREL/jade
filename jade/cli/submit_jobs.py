"""CLI to run jobs."""

import logging
import os
import shutil
import sys

import click

from jade.common import OUTPUT_DIR
from jade.enums import Status
from jade.jobs.job_submitter import JobSubmitter
from jade.loggers import setup_logging
from jade.models import HpcConfig, LocalHpcConfig
from jade.models.submitter_params import DEFAULTS, SubmitterParams
from jade.jobs.cluster import Cluster
from jade.utils.utils import get_cli_string, load_data


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
    "-f", "--force",
    default=False,
    is_flag=True,
    show_default=True,
    help="Delete output directory if it exists."
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
    is_eager=True,
    help="Number of processes to run in parallel; defaults to num CPUs."
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable verbose log output."
)
@click.option(
    "--reports/--no-reports",
    is_flag=True,
    default=True,
    show_default=True,
    help="Generate reports after execution."
)
@click.option(
    "-t", "--time-based-batching",
    is_flag=True,
    default=False,
    show_default=True,
    help="Use estimated runtimes to create batches. Each job must have its estimated runtime "
         "defined. Also requires --num-processes to be set. Overrides --per-node-batch-size."
)
@click.option(
    "--try-add-blocked-jobs/--no-try-add-blocked-jobs",
    is_flag=True,
    default=True,
    show_default=True,
    help="Add blocked jobs to a node's batch if they are blocked by jobs "
         "already in the batch."
)
@click.option(
    "-x", "--node-setup-script",
    help="Script to run on each node before starting jobs (download input files)."
)
@click.option(
    "-y", "--node-shutdown-script",
    help="Script to run on each after completing jobs (upload output files)."
)
def submit_jobs(
        config_file, per_node_batch_size, force, hpc_config, local, max_nodes,
        output, poll_interval, resource_monitor_interval, num_processes,
        verbose, reports, try_add_blocked_jobs, time_based_batching, node_setup_script,
        node_shutdown_script):
    """Submits jobs for execution, locally or on HPC."""
    if os.path.exists(output):
        if force:
            shutil.rmtree(output)
        else:
            print(f"{output} already exists. Delete it or use '--force' to overwrite.")
            sys.exit(1)

    os.makedirs(output)

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

    if time_based_batching and per_node_batch_size != DEFAULTS["per_node_batch_size"]:
        # This doesn't catch the case where the user passes --per-node-batch-size=default, but
        # I don't see that click provides a way to detect that condition.
        print("--per-node-batch-size and --time-based-batching are mutually exclusive")
        sys.exit(1)

    if time_based_batching:
        # From this point on, this parameter is overloaded. 0 means time-based-batching.
        per_node_batch_size = 0

    params = SubmitterParams(
        generate_reports=reports,
        hpc_config=hpc_config,
        max_nodes=max_nodes,
        num_processes=num_processes,
        per_node_batch_size=per_node_batch_size,
        node_setup_script=node_setup_script,
        node_shutdown_script=node_shutdown_script,
        poll_interval=poll_interval,
        resource_monitor_interval=resource_monitor_interval,
        try_add_blocked_jobs=try_add_blocked_jobs,
        verbose=verbose,
    )

    ret = JobSubmitter.run_submit_jobs(config_file, output, params)
    sys.exit(ret)
