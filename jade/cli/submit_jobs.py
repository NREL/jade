"""CLI to run jobs."""

import logging
import os
import shutil
import sys

import click

from jade.common import OUTPUT_DIR
from jade.cli.common import COMMON_SUBMITTER_OPTIONS, add_options, make_submitter_params
from jade.jobs.job_submitter import JobSubmitter
from jade.loggers import setup_logging
from jade.hpc.common import HpcType
from jade.models.hpc import SlurmConfig
from jade.models.submitter_params import SubmitterParams
from jade.jobs.cluster import Cluster
from jade.utils.utils import get_cli_string, load_data


logger = logging.getLogger(__name__)


@click.command()
@click.argument(
    "config-file",
    type=str,
)
@click.option(
    "-f",
    "--force",
    default=False,
    is_flag=True,
    show_default=True,
    help="Delete output directory if it exists.",
)
@click.option("-o", "--output", default=OUTPUT_DIR, show_default=True, help="Output directory.")
@click.option(
    "-s",
    "--submitter-params",
    default=None,
    show_default=False,
    type=str,
    help="Filename with submitter parameters. Supercedes other CLI parameters. Generate defaults "
    "with 'jade config submitter-params'",
)
@add_options(COMMON_SUBMITTER_OPTIONS)
def submit_jobs(
    config_file=None,
    per_node_batch_size=None,
    dry_run=None,
    force=None,
    hpc_config=None,
    local=None,
    max_nodes=None,
    output=None,
    poll_interval=None,
    resource_monitor_interval=None,
    num_processes=None,
    verbose=None,
    reports=None,
    try_add_blocked_jobs=None,
    time_based_batching=None,
    node_setup_script=None,
    node_shutdown_script=None,
    submitter_params=None,
):
    """Submits jobs for execution, locally or on HPC."""
    if os.path.exists(output):
        if force:
            shutil.rmtree(output)
        else:
            print(f"{output} already exists. Delete it or use '--force' to overwrite.")
            sys.exit(1)

    if submitter_params is not None:
        params = SubmitterParams(**load_data(submitter_params))
    else:
        params = make_submitter_params(
            per_node_batch_size=per_node_batch_size,
            dry_run=dry_run,
            hpc_config=hpc_config,
            local=local,
            max_nodes=max_nodes,
            poll_interval=poll_interval,
            resource_monitor_interval=resource_monitor_interval,
            num_processes=num_processes,
            verbose=verbose,
            reports=reports,
            try_add_blocked_jobs=try_add_blocked_jobs,
            time_based_batching=time_based_batching,
            node_setup_script=node_setup_script,
            node_shutdown_script=node_shutdown_script,
        )

    if params.time_based_batching and params.num_processes is None:
        print("Error: num_processes must be set with time-based batching")
        sys.exit(1)

    os.makedirs(output)
    filename = os.path.join(output, "submit_jobs.log")
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging(__name__, filename, file_level=level, console_level=level, mode="w")
    logger.info(get_cli_string())

    event_file = os.path.join(output, "submit_jobs_events.log")
    # This effectively means no console logging.
    setup_logging("event", event_file, console_level=logging.ERROR, file_level=logging.INFO)

    ret = JobSubmitter.run_submit_jobs(config_file, output, params)
    sys.exit(ret)
