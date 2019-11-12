"""Main CLI command for jade."""

import logging


import click

from jade.cli.auto_config import auto_config
from jade.cli.show_events import show_events
from jade.cli.extensions import extensions
from jade.cli.show_results import show_results
from jade.cli.submit_jobs import submit_jobs


logger = logging.getLogger(__name__)


@click.group()
def cli():
    """JADE commands"""


cli.add_command(extensions)
cli.add_command(auto_config)
cli.add_command(show_events)
cli.add_command(show_results)
cli.add_command(submit_jobs)
