"""CLI to show job status."""

import logging
import os
import sys

import click
from prettytable import PrettyTable

from jade.common import OUTPUT_DIR
from jade.enums import Status
from jade.jobs.cluster import Cluster
from jade.jobs.job_configuration_factory import create_config_from_previous_run
from jade.jobs.job_submitter import DEFAULTS, JobSubmitter
from jade.loggers import setup_logging
from jade.result import ResultsSummary
from jade.models import HpcConfig, SubmitterOptions
from jade.utils.utils import rotate_filenames, get_cli_string, load_data


logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "-o", "--output",
    default=OUTPUT_DIR,
    show_default=True,
    help="directory containing submission output",
)
@click.option(
    "-j", "--job-status",
    is_flag=True,
    default=False,
    show_default=True,
    help="include individual job status",
)
def show_status(output, job_status):
    """Shows the status of active HPC jobs."""
    setup_logging(__name__, None, console_level=logging.INFO)
    cluster, _ = Cluster.deserialize(output, deserialize_jobs=job_status)
    summary = cluster.get_status_summary(include_jobs=job_status)
    print(f"Summary of jobs running in path={output}:")
    for key, val in summary.items():
        if key == "job_status":
            print(f"  active_hpc_job_ids: {val['hpc_job_ids']}")
        else:
            print(f"  {key}: {val}")

    if job_status and summary["job_status"]["jobs"]:
        print("\nJob Status:")
        jobs = summary["job_status"]["jobs"]
        table = PrettyTable()
        table.field_names = list(jobs[0].keys())
        for job in jobs:
            row = []
            for name in table.field_names:
                if name == "blocked_by":
                    row.append(" ".join(job[name]))
                elif name == "state":
                    row.append(job[name].value)
                else:
                    row.append(job[name])
            table.add_row(row)
        print(table)
