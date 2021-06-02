"""Main CLI command for jade."""

import logging


import click

from jade.cli.run_jobs import run_jobs
from jade.cli.run_multi_node_job import run_multi_node_job
from jade.cli.run import run


logger = logging.getLogger(__name__)


@click.group()
def cli():
    """Entry point"""


cli.add_command(run)
cli.add_command(run_jobs)
cli.add_command(run_multi_node_job)
