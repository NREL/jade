"""
CLI to show events of a scenario.
"""

import logging

import click

from jade.common import OUTPUT_DIR
from jade.loggers import setup_logging
from jade.events import EventsSummary
from jade.resource_monitor import CpuStatsViewer, DiskStatsViewer, \
    MemoryStatsViewer


@click.option(
    "-o", "--output",
    default=OUTPUT_DIR,
    show_default=True,
    help="Output directory."
)
@click.option(
    "--json",
    is_flag=True,
    default=False,
    show_default=True,
    help="Print event in JSON format, instead of table"
)
@click.option(
    "--cpu",
    is_flag=True,
    default=False,
    show_default=True,
    help="Print CPU stats"
)
@click.option(
    "--disk",
    is_flag=True,
    default=False,
    show_default=True,
    help="Print disk stats"
)
@click.option(
    "--mem",
    is_flag=True,
    default=False,
    show_default=True,
    help="Print memory stats"
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable verbose event outputs."
)
@click.command()
def show_events(output, json=False, cpu=False, disk=False, mem=False, verbose=False):
    """Shows the events after jobs run."""
    level = logging.DEBUG if verbose else logging.WARNING
    setup_logging("show_results", None, console_level=level)
    results = EventsSummary(output)
    if cpu or disk or mem:
        if cpu:
            viewer = CpuStatsViewer(results.events)
            viewer.show_stats()
        if disk:
            viewer = DiskStatsViewer(results.events)
            viewer.show_stats()
        if mem:
            viewer = MemoryStatsViewer(results.events)
            viewer.show_stats()
    else:
        results.show_events()
