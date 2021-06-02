"""Cluster-related CLI commands"""

import logging
import sys

import click

from jade.common import OUTPUT_DIR
from jade.exceptions import InvalidConfiguration
from jade.jobs.cluster import Cluster
from jade.hpc.hpc_manager import HpcManager
from jade.loggers import setup_logging
from jade.models.submission_group import make_submission_group_lookup


logger = logging.getLogger(__name__)


@click.command()
@click.argument("output_dir", type=click.Path(exists=True))
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def am_i_manager(output_dir, verbose):
    """Print 'true' or 'false' depending on whether the current node is the manager node."""
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging(__name__, None, console_level=level)
    try:
        cluster, _ = Cluster.deserialize(output_dir, deserialize_jobs=True)
    except InvalidConfiguration:
        print(f"{output_dir} is not a JADE output directory used in cluster mode")
        sys.exit(1)

    if cluster.is_complete():
        print("All jobs are already complete.")
        sys.exit()

    groups = make_submission_group_lookup([cluster.config.submission_groups[0]])
    hpc_mgr = HpcManager(groups, output_dir)
    am_master = hpc_mgr.am_i_manager()
    print(str(am_master).lower(), end="")


@click.command()
@click.argument("output_dir", type=click.Path(exists=True))
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def hostnames(output_dir, verbose):
    """Show the hostnames of active nodes participating in the batch."""
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging(__name__, None, console_level=level)
    try:
        cluster, _ = Cluster.deserialize(output_dir, deserialize_jobs=True)
    except InvalidConfiguration:
        print(f"{output_dir} is not a JADE output directory used in cluster mode")
        sys.exit(1)

    if cluster.is_complete():
        print("All jobs are already complete.")
        sys.exit()

    groups = make_submission_group_lookup([cluster.config.submission_groups[0]])
    hpc_mgr = HpcManager(groups, output_dir)
    nodes = []
    for job_id in cluster.job_status.hpc_job_ids:
        nodes += hpc_mgr.list_active_nodes(job_id)

    if not nodes:
        print("No nodes were detected.")
        sys.exit(1)

    print(" ".join(nodes))


@click.group()
def cluster():
    pass


cluster.add_command(am_i_manager)
cluster.add_command(hostnames)
