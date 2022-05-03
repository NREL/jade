"""CLI to show HPC job information."""

import logging
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

import click
from prettytable import PrettyTable

from jade.exceptions import InvalidConfiguration
from jade.jobs.cluster import Cluster
from jade.hpc.common import HpcJobStats, HpcJobStatus
from jade.hpc.hpc_manager import HpcManager
from jade.loggers import setup_logging
from jade.models.submission_group import make_submission_group_lookup


logger = logging.getLogger(__name__)


def _check_output_dirs(_, __, output_dirs):
    if not output_dirs:
        print("output_dirs cannot be empty", file=sys.stderr)
        sys.exit(1)

    return output_dirs


@click.command()
@click.argument("output-dirs", nargs=-1, callback=_check_output_dirs)
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def show_times(output_dirs, verbose):
    """Show the run times of all allocated jobs."""
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging(__name__, None, console_level=level)

    job_ids = []
    for output in output_dirs:
        path = Path(output)
        try:
            cluster, _ = Cluster.deserialize(path, deserialize_jobs=False)
        except InvalidConfiguration:
            print(f"{output} is not a JADE output directory used in cluster mode", file=sys.stderr)
            sys.exit(1)

        job_ids += [x.name.split("_")[2].replace(".e", "") for x in path.glob("*.e")]

    job_ids.sort(key=lambda x: int(x))
    groups = make_submission_group_lookup([cluster.config.submission_groups[0]])
    hpc_mgr = HpcManager(groups, output)

    total_duration = timedelta(seconds=0)
    table = PrettyTable()
    table.field_names = HpcJobStats._fields

    total_aus = 0
    if os.environ.get("NREL_CLUSTER") == "eagle":
        au_parser = get_nrel_eagle_aus
    else:
        au_parser = None

    for job_id in job_ids:
        stats = hpc_mgr.get_job_stats(job_id)
        if stats is None:
            continue
        duration = stats.end - stats.start
        if stats.state == HpcJobStatus.COMPLETE and isinstance(stats.end, datetime):
            total_duration += duration
        data = stats._asdict()
        data["state"] = data["state"].value
        if au_parser is not None:
            total_aus += au_parser(duration, stats.qos)
        table.add_row(data.values())

    print(table)
    print(f"\nTotal duration = {total_duration}")
    print("Total hours = {:.2f}".format(total_duration.total_seconds() / 3600))
    if au_parser is not None:
        print("Total AUs = {:.2f}".format(total_aus))


def get_nrel_eagle_aus(duration, qos):
    _duration = duration.total_seconds() / 3600
    if qos == "normal":
        val = _duration * 3.0
    elif qos == "standby":
        val = 0.0
    elif qos == "high":
        val = _duration * 3.0 * 2
    else:
        assert f"qos={qos} is not supported"

    return val


@click.command()
@click.argument("output-dirs", nargs=-1, callback=_check_output_dirs)
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def list_active_ids(output_dirs, verbose):
    """List the HPC job IDs that are pending or running."""
    # TODO: add flag for only pending or only running
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging(__name__, None, console_level=level)

    job_ids = []
    for output in output_dirs:
        path = Path(output)
        try:
            cluster, _ = Cluster.deserialize(path, deserialize_jobs=True)
        except InvalidConfiguration:
            print(f"{output} is not a JADE output directory used in cluster mode", file=sys.stderr)
            sys.exit(1)

        if not cluster.is_complete():
            job_ids += list(cluster.iter_hpc_job_ids())

    job_ids.sort(key=lambda x: int(x))
    print(" ".join(job_ids))


@click.group()
def hpc_jobs():
    """Subcommands related to HPC jobs"""
    pass


hpc_jobs.add_command(list_active_ids)
hpc_jobs.add_command(show_times)
