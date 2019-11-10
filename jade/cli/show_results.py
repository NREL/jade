"""CLI to show results of a scenario."""

import logging

import click


from jade.common import OUTPUT_DIR
from jade.loggers import setup_logging
from jade.result import ResultsSummary


@click.option(
    "-f", "--failed",
    is_flag=True,
    default=False,
    show_default=True,
    help="Show failed results only."
)
@click.option(
    "-o", "--output",
    default=OUTPUT_DIR,
    show_default=True,
    help="Output directory."
)
@click.option(
    "-s", "--successful",
    is_flag=True,
    default=False,
    show_default=True,
    help="Show successful results only."
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable verbose log output."
)
@click.command()
def show_results(failed, output, successful, verbose):
    """Shows the results of a batch of jobs."""
    level = logging.DEBUG if verbose else logging.WARNING
    setup_logging("show_results", None, console_level=level)
    results = ResultsSummary(output)
    results.show_results(only_failed=failed, only_successful=successful)
