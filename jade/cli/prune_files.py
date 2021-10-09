"""CLI to run jobs."""

import itertools
import logging
from pathlib import Path

import click

from jade.loggers import setup_logging


logger = logging.getLogger(__name__)


@click.command()
@click.argument(
    "output",
    type=click.Path(exists=True),
)
def prune_files(output):
    """Deletes intermediate scripts, config files, and log files that are not needed if the job
    results were successful."""
    setup_logging(__name__, None, console_level=logging.INFO)
    base_path = Path(output)

    count = 0
    for path in itertools.chain(
        # Keep submit_jobs.log* files because there aren't many of them and they are useful.
        base_path.glob("submit_jobs_events*.log*"),
        base_path.glob("run_jobs_batch*.log*"),
        base_path.glob("config_batch*.json"),
        base_path.glob("*.o"),
        base_path.glob("*.e"),
        base_path.glob("*.sh"),
    ):
        path.unlink()
        count += 1
    print(f"Deleted {count} files from {output}.")
