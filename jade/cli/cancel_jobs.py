"""CLI to run jobs."""

import logging
import os
import sys
import time

import click

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
def cancel_jobs(output, verbose):
    """Cancels jobs."""
    filename = os.path.join(output, "cancel_jobs.log")
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging(__name__, filename, file_level=level, console_level=level, mode="w")
    logger.info(get_cli_string())

    for _ in range(60):
        cluster, promoted = Cluster.deserialize(
            output,
            try_promote_to_submitter=True,
            deserialize_jobs=True,
        )
        if not promoted:
            logger.info("Did not get promoted. Sleep.")
            time.sleep(1)
            continue
        if cluster.is_complete():
            cluster.demote_from_submitter()
            logger.info("All jobs are already finished.")
            sys.exit(0)
        submitter = JobSubmitter.load(output)
        submitter.cancel_jobs(cluster)
        sys.exit(0)

    logger.error("Failed to get promoted to submitter.")
    sys.exit(1)
