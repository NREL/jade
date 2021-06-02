"""CLI to show job status."""

import logging
import os
import sys

import click
from prettytable import PrettyTable

from jade.common import OUTPUT_DIR
from jade.exceptions import InvalidConfiguration
from jade.jobs.cluster import Cluster
from jade.hpc.common import HpcJobStatus
from jade.hpc.hpc_manager import HpcManager
from jade.loggers import setup_logging
from jade.models.submission_group import make_submission_group_lookup
from jade.utils.subprocess_manager import run_command


logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "-o",
    "--output",
    default=OUTPUT_DIR,
    show_default=True,
    type=click.Path(exists=True),
    help="directory containing submission output",
)
@click.option(
    "-j",
    "--job-status",
    is_flag=True,
    default=False,
    show_default=True,
    help="include individual job status",
)
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def show_status(output, job_status, verbose):
    """Shows the status of active HPC jobs."""
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging(__name__, None, console_level=level)
    try:
        cluster, _ = Cluster.deserialize(output, deserialize_jobs=True)
    except InvalidConfiguration:
        print(f"{output} is not a JADE output directory used in cluster mode")
        sys.exit(1)

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

    if not cluster.is_complete():
        # Check if the last job got killed or timed-out and try to restart.
        run_new_submitter = False
        if cluster.job_status.hpc_job_ids:
            groups = make_submission_group_lookup([cluster.config.submission_groups[0]])
            hpc_mgr = HpcManager(groups, output)
            all_jobs_are_none = True
            for job_id in cluster.job_status.hpc_job_ids:
                status = hpc_mgr.check_status(job_id=job_id)
                if status != HpcJobStatus.NONE:
                    all_jobs_are_none = False
                    break
            if all_jobs_are_none:
                logger.warn("HPC job statuses may be out-of-date.")
                run_new_submitter = True
        else:
            logger.error("Jobs are not complete but there no active HPC jobs.")
            run_new_submitter = True
        if run_new_submitter:
            try_submit_cmd = f"jade try-submit-jobs {output}"
            if verbose:
                try_submit_cmd += " --verbose"
            run_command(try_submit_cmd)
