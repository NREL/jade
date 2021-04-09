"""Main CLI command for jade."""

import logging


import click

from jade.cli.auto_config import auto_config
from jade.cli.cancel_jobs import cancel_jobs
from jade.cli.config import config
from jade.cli.extensions import extensions
from jade.cli.pipeline import pipeline
from jade.cli.resubmit_jobs import resubmit_jobs
from jade.cli.show_events import show_events
from jade.cli.show_results import show_results
from jade.cli.show_status import show_status
from jade.cli.stats import stats
from jade.cli.submit_jobs import submit_jobs
from jade.cli.try_submit_jobs import try_submit_jobs
from jade.cli.wait import wait


logger = logging.getLogger(__name__)


@click.group()
def cli():
    """JADE commands"""


cli.add_command(extensions)
cli.add_command(auto_config)
cli.add_command(cancel_jobs)
cli.add_command(config)
cli.add_command(pipeline)
cli.add_command(resubmit_jobs)
cli.add_command(show_events)
cli.add_command(show_results)
cli.add_command(show_status)
cli.add_command(stats)
cli.add_command(submit_jobs)
cli.add_command(try_submit_jobs)
cli.add_command(wait)
