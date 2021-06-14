"""CLI to resubmit failed and missing jobs."""

import logging
import os
import sys
from pathlib import Path

import click

from jade.common import CONFIG_FILE
from jade.enums import Status
from jade.jobs.cluster import Cluster
from jade.jobs.job_configuration_factory import create_config_from_file
from jade.jobs.results_aggregator import ResultsAggregator
from jade.jobs.job_submitter import JobSubmitter
from jade.loggers import setup_logging
from jade.result import ResultsSummary
from jade.utils.utils import rotate_filenames


logger = logging.getLogger(__name__)


@click.command()
@click.argument(
    "output",
    type=click.Path(exists=True),
)
@click.option(
    "--failed/--no-failed",
    is_flag=True,
    default=True,
    show_default=True,
    help="Resubmit failed and canceled jobs.",
)
@click.option(
    "--missing/--no-missing",
    is_flag=True,
    default=True,
    show_default=True,
    help="Resubmit missing jobs.",
)
@click.option(
    "--rotate-logs/--no-rotate-logs",
    default=True,
    show_default=True,
    help="Rotate log files so that they aren't overwritten.",
)
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def resubmit_jobs(output, failed, missing, rotate_logs, verbose):
    """Resubmit failed and missing jobs."""
    if rotate_logs:
        rotate_filenames(output, ".log")

    filename = os.path.join(output, "submit_jobs.log")
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging(__name__, filename, file_level=level, console_level=level, mode="a")

    event_file = os.path.join(output, "submit_jobs_events.log")
    # This effectively means no console logging.
    setup_logging("event", event_file, console_level=logging.ERROR, file_level=logging.INFO)

    cluster, promoted = Cluster.deserialize(
        output,
        try_promote_to_submitter=True,
        deserialize_jobs=True,
    )
    if not cluster.is_complete():
        cluster.demote_from_submitter()
        print("resubmit-jobs requires that the existing submission be complete")
        sys.exit(1)
    assert promoted

    jobs_to_resubmit = _get_jobs_to_resubmit(cluster, output, failed, missing)
    updated_blocking_jobs_by_name = _update_with_blocking_jobs(jobs_to_resubmit, output)
    _reset_results(output, jobs_to_resubmit)
    cluster.prepare_for_resubmission(jobs_to_resubmit, updated_blocking_jobs_by_name)

    ret = 1
    try:
        mgr = JobSubmitter.load(output)
        status = mgr.submit_jobs(cluster)
        if status == Status.IN_PROGRESS:
            print(f"Resubmitted {len(jobs_to_resubmit)} jobs in {output}")
            ret = 0
        else:
            ret = status.value
    finally:
        cluster.demote_from_submitter()

    sys.exit(ret)


def _get_jobs_to_resubmit(cluster, output, failed, missing):
    results = ResultsSummary(output)
    jobs_to_resubmit = []
    if failed:
        jobs_to_resubmit += results.get_canceled_results()
        jobs_to_resubmit += results.get_failed_results()
    if missing:
        jobs_to_resubmit += results.get_missing_jobs(cluster.iter_jobs())

    return {x.name for x in jobs_to_resubmit}


def _update_with_blocking_jobs(jobs_to_resubmit, output):
    config = create_config_from_file(Path(output) / CONFIG_FILE)

    # Any job that was blocked by any of these jobs must also be resubmitted.
    # Same for any job blocked by one of those.
    # Account for abnormal ordering where a lower-ID'd job is blocked by a later one.
    updated_blocking_jobs_by_name = {}
    max_iter = config.get_num_jobs()
    for i in range(max_iter):
        first = len(jobs_to_resubmit)
        for job in config.iter_jobs():
            blocking_jobs = job.get_blocking_jobs().intersection(jobs_to_resubmit)
            updated_blocking_jobs_by_name[job.name] = blocking_jobs
            if blocking_jobs:
                jobs_to_resubmit.add(job.name)
        num_added = len(jobs_to_resubmit) - first
        if num_added == 0:
            break
        assert i < max_iter - 1, f"max_iter={max_iter} num_added={num_added} first={first}"

    return updated_blocking_jobs_by_name


def _reset_results(output, jobs_to_resubmit):
    aggregator = ResultsAggregator.load(output)
    aggregator.clear_results_for_resubmission(jobs_to_resubmit)
