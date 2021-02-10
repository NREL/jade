"""CLI to run jobs."""

import logging
import os
import sys

import click

from jade.enums import Status
from jade.jobs.job_submitter import DEFAULTS, JobSubmitter
from jade.jobs.job_configuration_factory import create_config_from_previous_run
from jade.loggers import setup_logging
from jade.models import HpcConfig, LocalHpcConfig, SubmitterOptions
from jade.models.submitter_options import DEFAULTS as SUBMITTER_DEFAULTS
from jade.result import ResultsSummary
from jade.jobs.cluster import Cluster
from jade.utils.utils import rotate_filenames, get_cli_string, load_data


logger = logging.getLogger(__name__)


@click.command()
@click.argument(
    "config-file",
    type=str,
)
@click.option(
    "-b", "--per-node-batch-size",
    default=SUBMITTER_DEFAULTS["per_node_batch_size"],
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
    default=SUBMITTER_DEFAULTS["max_nodes"],
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
    default=SUBMITTER_DEFAULTS["poll_interval"],
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
def submit_jobs(
        config_file, per_node_batch_size, hpc_config, local, max_nodes,
        output, poll_interval, num_processes, rotate_logs,
        verbose, restart_failed, restart_missing, reports):
    """Submits jobs for execution, locally or on HPC."""
    os.makedirs(output, exist_ok=True)

    previous_results = []

    if restart_failed:
        failed_job_config = create_config_from_previous_run(config_file, output,
                                                            result_type='failed')
        previous_results = ResultsSummary(output).get_successful_results()
        config_file = "failed_job_inputs.json"
        failed_job_config.dump(config_file)

    if restart_missing:
        missing_job_config = create_config_from_previous_run(config_file, output,
                                                             result_type='missing')
        config_file = "missing_job_inputs.json"
        missing_job_config.dump(config_file)
        previous_results = ResultsSummary(output).list_results()

    if rotate_logs:
        rotate_filenames(output, ".log")

    filename = os.path.join(output, "submit_jobs.log")
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging(__name__, filename, file_level=level, console_level=level)
    logger.info(get_cli_string())

    event_file = os.path.join(output, "submit_jobs_events.log")
    # This effectively means no console logging.
    setup_logging("event", event_file, console_level=logging.ERROR,
                  file_level=logging.INFO)

    if local:
        hpc_config = HpcConfig(hpc_type="local", hpc=LocalHpcConfig())
    else:
        hpc_config = HpcConfig(**load_data(hpc_config))

    options = SubmitterOptions(
        generate_reports=reports,
        hpc_config=hpc_config,
        max_nodes=max_nodes,
        num_processes=num_processes,
        per_node_batch_size=per_node_batch_size,
        poll_interval=poll_interval,
        verbose=verbose,
    )
    mgr = JobSubmitter.create(config_file, output=output)
    cluster = Cluster.create(output, options, mgr.config)

    ret = 1
    try:
        status = mgr.submit_jobs(
            cluster,
            force_local=local,
            previous_results=previous_results,
        )
        if status == Status.IN_PROGRESS:
            check_cmd = f"jade show-status {output}"
            print(f"Jobs are in progress. Run '{check_cmd}' for updates.")
            ret = 0
        else:
            ret = status.value
    finally:
        cluster.demote_from_submitter()

    sys.exit(ret)
