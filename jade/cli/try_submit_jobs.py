"""CLI to try to submit new jobs for an existing submission."""

import logging
import os
import sys

import click

from jade.enums import Status
from jade.jobs.cluster import Cluster
from jade.jobs.job_submitter import JobSubmitter
from jade.loggers import setup_logging
from jade.utils.utils import get_cli_string


logger = logging.getLogger(__name__)


@click.command()
@click.argument(
    "output",
    type=click.Path(exists=True),
)
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def try_submit_jobs(output, verbose):
    """Internal command to try to submit new jobs for an existing submission."""
    filename = os.path.join(output, "submit_jobs.log")
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging(__name__, filename, file_level=level, console_level=level, mode="a")
    logger.info(get_cli_string())

    event_file = os.path.join(output, "submit_jobs_events.log")
    # This effectively means no console logging.
    setup_logging("event", event_file, console_level=logging.ERROR, file_level=logging.INFO)

    cluster, promoted = Cluster.deserialize(
        output,
        try_promote_to_submitter=True,
        deserialize_jobs=True,
    )
    if not promoted:
        print("Another node is already the submitter.")
        sys.exit(0)
    elif cluster.is_complete():
        cluster.demote_from_submitter()
        logger.info("All jobs are already finished.")
        sys.exit(0)

    ret = 1
    try:
        mgr = JobSubmitter.load(output)
        status = mgr.submit_jobs(cluster)
        if status == Status.IN_PROGRESS:
            check_cmd = f"jade show-status -o {output}"
            print(f"Jobs are in progress. Run '{check_cmd}' for updates.")
            ret = 0
        else:
            ret = status.value
    except Exception:
        logger.exception("Failed to try-submit-jobs")
        raise
    finally:
        cluster.demote_from_submitter()

    sys.exit(ret)
