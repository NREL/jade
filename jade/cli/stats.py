"""
CLI to show events of a scenario.
"""

import datetime
import json
import os
import sys
from pathlib import Path

import click
from psutil._common import bytes2human

from jade.cli.collect_stats import collect
from jade.common import OUTPUT_DIR
from jade.loggers import setup_logging
from jade.events import EventsSummary
from jade.resource_monitor import (
    CpuStatsViewer,
    DiskStatsViewer,
    MemoryStatsViewer,
    NetworkStatsViewer,
)


STATS = ("cpu", "disk", "mem", "net")


@click.group()
def stats():
    """Collect new stats or view stats from an existing run."""
    setup_logging("stats", None)


@click.argument("stats", nargs=-1)
@click.option(
    "-o",
    "--output",
    default=OUTPUT_DIR,
    show_default=True,
    help="JADE submission output directory.",
)
@click.command()
def plot(stats, output):
    """Plot stats from a run to files.

    \b
    Examples:
    jade stats plot
    jade stats plot cpu
    jade stats plot disk
    jade stats plot mem
    jade stats plot net
    jade stats plot cpu disk mem
    """
    events = EventsSummary(output)

    if not stats:
        stats = STATS

    plot_dir = Path(output) / "stats"
    os.makedirs(plot_dir, exist_ok=True)
    for stat in stats:
        if stat == "cpu":
            viewer = CpuStatsViewer(events)
        elif stat == "disk":
            viewer = DiskStatsViewer(events)
        elif stat == "mem":
            viewer = MemoryStatsViewer(events)
        elif stat == "net":
            viewer = NetworkStatsViewer(events)
        else:
            print(f"Invalid stat={stat}")
            sys.exit(1)
        viewer.plot_to_file(plot_dir)


@click.argument("stats", nargs=-1)
@click.option(
    "-j",
    "--json-summary",
    default=False,
    is_flag=True,
    show_default=True,
    help="Only show the summary stats per node in JSON format.",
)
@click.option("-o", "--output", default=OUTPUT_DIR, show_default=True, help="Output directory.")
@click.option(
    "-s",
    "--summary-only",
    default=False,
    is_flag=True,
    show_default=True,
    help="Only show the summary stats per node.",
)
@click.command()
def show(stats, json_summary, output, summary_only):
    """Shows stats from a run.

    \b
    Examples:
    jade stats show
    jade stats show cpu
    jade stats show disk
    jade stats show mem
    jade stats show net
    jade stats show cpu disk mem
    jade stats show --summary cpu disk mem
    jade stats show --json-summary cpu disk mem
    """
    events = EventsSummary(output)

    if not stats:
        stats = STATS

    summaries_as_dicts = []
    for stat in stats:
        if stat == "cpu":
            viewer = CpuStatsViewer(events)
        elif stat == "disk":
            viewer = DiskStatsViewer(events)
        elif stat == "mem":
            viewer = MemoryStatsViewer(events)
        elif stat == "net":
            viewer = NetworkStatsViewer(events)
        else:
            print(f"Invalid stat={stat}")
            sys.exit(1)
        if json_summary:
            summaries_as_dicts += viewer.get_stats_summary()
        else:
            viewer.show_stats(show_all_timestamps=not summary_only)

    if json_summary:
        print(json.dumps(summaries_as_dicts, indent=2))


@click.option(
    "--human-readable/--no-human-readable",
    is_flag=True,
    default=True,
    show_default=True,
    help="Output directory.",
)
@click.option("-o", "--output", default=OUTPUT_DIR, show_default=True, help="Output directory.")
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
    help="Output directory.",
)
@click.option("-o", "--output", default=OUTPUT_DIR, show_default=True, help="Output directory.")
@click.command()
def exec_time(output, human_readable):
    events = EventsSummary(output)
    config_exec_time = events.get_config_exec_time()
    if human_readable:
        print(datetime.timedelta(seconds=config_exec_time))
    else:
        print(config_exec_time)


stats.add_command(bytes_consumed)
stats.add_command(collect)
stats.add_command(exec_time)
stats.add_command(plot)
stats.add_command(show)
