"""
CLI to show events of a scenario.
"""

import logging
import click
from jade.common import OUTPUT_DIR
from jade.loggers import setup_logging
from jade.events import EventsSummary


@click.option(
    "-o", "--output",
    default=OUTPUT_DIR,
    show_default=True,
    help="Output directory."
)
@click.option(
    "--json",
    help="Print event in JSON format, instead of table"
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable verbose event outputs."
)
@click.command()
def show_events(output, json, verbose):
    """Shows the events after jobs run."""
    level = logging.DEBUG if verbose else logging.WARNING
    setup_logging("show_results", None, console_level=level)
    results = EventsSummary(output)
    results.show_events()
