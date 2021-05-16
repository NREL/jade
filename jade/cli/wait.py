"""CLI to wait for completion of jobs."""

import sys
import time

import click

from jade.common import OUTPUT_DIR
from jade.exceptions import InvalidConfiguration
from jade.jobs.cluster import Cluster


@click.command()
@click.option(
    "-o",
    "--output",
    default=OUTPUT_DIR,
    show_default=True,
    help="directory containing submission output",
)
@click.option(
    "-p",
    "--poll-interval",
    default=10.0,
    show_default=True,
    help="poll interval in minutes",
)
def wait(output, poll_interval):
    """Wait for a JADE submission to complete."""
    while True:
        try:
            cluster, _ = Cluster.deserialize(output)
        except InvalidConfiguration:
            print(f"{output} is not a JADE output directory used in cluster mode")
            sys.exit(1)
        if cluster.is_complete():
            print("All jobs are complete")
            break
        time.sleep(poll_interval * 60)
