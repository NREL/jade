"""CLI to run jobs."""

import logging
import os
import shutil
import sys

import click

from jade.common import OUTPUT_DIR
from jade.cli.common import COMMON_SUBMITTER_OPTIONS, add_options, make_submitter_params
from jade.jobs.job_configuration_factory import create_config_from_file
from jade.jobs.job_submitter import JobSubmitter
from jade.loggers import setup_logging, setup_event_logging
from jade.hpc.common import HpcType
from jade.models.hpc import SlurmConfig
from jade.models.submitter_params import SubmitterParams
from jade.jobs.cluster import Cluster
from jade.utils.utils import get_cli_string, load_data


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
    resource_monitor_type=None,
    resource_monitor_stats=None,
    num_parallel_processes_per_node=None,
    verbose=None,
    reports=None,
    enable_singularity=None,
    container=None,
    try_add_blocked_jobs=None,
    time_based_batching=None,
    node_setup_script=None,
    node_shutdown_script=None,
    submitter_params=None,
    no_distributed_submitter=None,
):
    """Submits jobs for execution, locally or on HPC."""
    if os.path.exists(output):
        if force:
            shutil.rmtree(output)
        else:
            print(
                f"{output} already exists. Delete it or use '--force' to overwrite.",
                file=sys.stderr,
            )
            sys.exit(1)

    os.makedirs(output)
    filename = os.path.join(output, "submit_jobs.log")
    event_filename = os.path.join(output, "submit_jobs_events.log")
    level = logging.DEBUG if verbose else logging.INFO
    # For some reason event logging must be setup before general logging.
    # Otherwise, the first event doesn't show up in the log.
    setup_event_logging(event_filename, mode="a")
    logger = setup_logging(__name__, filename, file_level=level, console_level=level, mode="a")
    logger.info(get_cli_string())

    config = create_config_from_file(config_file)
    if config.submission_groups:
        params = config.submission_groups[0].submitter_params
    elif submitter_params is not None:
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
            resource_monitor_type=resource_monitor_type,
            resource_monitor_stats=resource_monitor_stats,
            num_parallel_processes_per_node=num_parallel_processes_per_node,
            verbose=verbose,
            reports=reports,
            enable_singularity=enable_singularity,
            container=container,
            try_add_blocked_jobs=try_add_blocked_jobs,
            time_based_batching=time_based_batching,
            node_setup_script=node_setup_script,
            node_shutdown_script=node_shutdown_script,
            no_distributed_submitter=no_distributed_submitter,
        )

    if not config.submission_groups:
        config.assign_default_submission_group(params)

    for group in config.submission_groups:
        params = group.submitter_params
        if params.resource_monitor_interval is not None:
            if params.resource_monitor_interval < params.poll_interval:
                logger.warning(
                    "resource_monitor_interval cannot be less than poll_interval. "
                    "Reducing poll_interval"
                )
                params.poll_interval = params.resource_monitor_interval

    try:
        ret = JobSubmitter.run_submit_jobs(
            config,
            output,
            local=params.hpc_config.hpc_type == HpcType.LOCAL,
            dry_run=params.dry_run,
        )
        sys.exit(ret)
    except Exception:
        logger.exception("Failed to run submit_jobs")
        raise
    finally:
        logging.shutdown()
