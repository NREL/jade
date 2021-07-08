"""CLI to show results of a scenario."""

import logging
import sys
import click

from jade.common import OUTPUT_DIR
from jade.exceptions import InvalidConfiguration
from jade.jobs.job_post_process import JobPostProcess
from jade.loggers import setup_logging
from jade.result import ResultsSummary


@click.option(
    "-f",
    "--failed",
    is_flag=True,
    default=False,
    show_default=True,
    help="Show failed results only.",
)
@click.option("-o", "--output", default=OUTPUT_DIR, show_default=True, help="Output directory.")
@click.option(
    "-s",
    "--successful",
    is_flag=True,
    default=False,
    show_default=True,
    help="Show successful results only.",
)
@click.option(
    "-p",
    "--post-process",
    is_flag=True,
    default=False,
    show_default=True,
    help="Show post-process results.",
)
@click.option("-j", "--job-name", default=None, help="Specific job to show post-process results.")
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
@click.command()
def show_results(failed, output, successful, post_process, job_name, verbose):
    """Shows the results of a batch of jobs."""
    level = logging.DEBUG if verbose else logging.WARNING
    setup_logging("show_results", None, console_level=level)

    if post_process:
        JobPostProcess.show_results(output, job_name)
        sys.exit(0)

    try:
        results = ResultsSummary(output)
    except InvalidConfiguration:
        print(
            f"No results are available in {output}. To check status of in-progress jobs run "
            f"'jade show-status -o {output}'"
        )
        sys.exit(1)

    results.show_results(only_failed=failed, only_successful=successful)
