"""CLI to run jobs."""

import logging
import os
import sys
import time

import click

from jade.jobs.cluster import Cluster
from jade.jobs.job_submitter import JobSubmitter
from jade.loggers import setup_logging
from jade.utils.run_command import run_command
from jade.utils.utils import get_cli_string


logger = logging.getLogger(__name__)


@click.command()
@click.argument(
    "output",
    type=click.Path(exists=True),
)
@click.option(
    "--complete/--no-complete",
    default=True,
    is_flag=True,
    show_default=True,
    help="Run completion operations. This can take some time. Use --no-complete if you plan to "
    "discard the results.",
)
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def cancel_jobs(output, complete, verbose):
    """Cancels jobs."""
    filename = os.path.join(output, "cancel_jobs.log")
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging(__name__, filename, file_level=level, console_level=level, mode="a")
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
        cluster.demote_from_submitter()
        ret = 0
        if complete:
            delay = 15
            print(f"Delaying {delay} seconds to let the nodes complete.")
            time.sleep(delay)
            ret = run_command(f"jade try-submit-jobs {output}")
        sys.exit(ret)

    logger.error("Failed to get promoted to submitter.")
    sys.exit(1)
