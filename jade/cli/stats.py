"""
CLI to show events of a scenario.
"""

import datetime
import logging
import sys

import click
from psutil._common import bytes2human

from jade.common import OUTPUT_DIR
from jade.loggers import setup_logging
from jade.events import EventsSummary
from jade.resource_monitor import CpuStatsViewer, DiskStatsViewer, \
    MemoryStatsViewer, NetworkStatsViewer


STATS = (
    "cpu", "disk", "mem", "net"
)

@click.group()
def stats():
    """View stats from a run."""
    setup_logging("stats", None)

@click.argument("stats", nargs=-1)
@click.option(
    "-o", "--output",
    default=OUTPUT_DIR,
    show_default=True,
    help="Output directory."
)
@click.command()
def show(stats, output):
    """Shows stats from a run.

    \b
    Examples:
    jade stats
    jade stats cpu
    jade stats disk
    jade stats mem
    jade stats net
    jade stats cpu disk mem
    """
    results = EventsSummary(output)

    if not stats:
        stats = STATS

    for stat in stats:
        if stat == "cpu":
            viewer = CpuStatsViewer(results.events)
        elif stat == "disk":
            viewer = DiskStatsViewer(results.events)
        elif stat == "mem":
            viewer = MemoryStatsViewer(results.events)
        elif stat == "net":
            viewer = NetworkStatsViewer(results.events)
        else:
            print(f"Invalid stat={stat}")
            sys.exit(1)
        viewer.show_stats()


@click.option(
    "--human-readable/--no-human-readable",
    is_flag=True,
    default=True,
    show_default=True,
    help="Output directory."
)
@click.option(
    "-o", "--output",
    default=OUTPUT_DIR,
    show_default=True,
    help="Output directory."
)
@click.command()
def bytes_consumed(output, human_readable): 
    events = EventsSummary(output)
    consumed = events.get_bytes_consumed()
    if human_readable:
        print(bytes2human(consumed))
    else:
        print(consumed)


@click.option(
    "--human-readable/--no-human-readable",
    is_flag=True,
    default=True,
    show_default=True,
    help="Output directory."
)
@click.option(
    "-o", "--output",
    default=OUTPUT_DIR,
    show_default=True,
    help="Output directory."
)
@click.command()
def exec_time(output, human_readable):
    events = EventsSummary(output)
    config_exec_time = events.get_config_exec_time()
    if human_readable:
        print(datetime.timedelta(seconds=config_exec_time))
    else:
        print(config_exec_time)


stats.add_command(bytes_consumed)
stats.add_command(exec_time)
stats.add_command(show)
