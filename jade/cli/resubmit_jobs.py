"""CLI to resubmit failed and missing jobs."""

import logging
import os
import sys

import click

from jade.enums import Status
from jade.jobs.cluster import Cluster
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

    results = ResultsSummary(output)
    jobs_to_resubmit = []
    if failed:
        jobs_to_resubmit += results.get_canceled_results()
        jobs_to_resubmit += results.get_failed_results()
        # Clear these results.
        aggregator = ResultsAggregator.load(output)
        aggregator.clear_unsuccessful_results()
    if missing:
        jobs_to_resubmit += results.get_missing_jobs(cluster.iter_jobs())

    # Note: both jobs and results have `.name`
    jobs_to_resubmit = {x.name for x in jobs_to_resubmit}
    cluster.prepare_for_resubmission(jobs_to_resubmit)

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
