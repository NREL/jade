"""
CLI to show events of a scenario.
"""

import datetime
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

import click
from prettytable import PrettyTable
from psutil._common import bytes2human

from jade.cli.collect_stats import collect
from jade.common import EVENTS_DIR, OUTPUT_DIR, STATS_DIR
from jade.loggers import setup_logging
from jade.events import EventsSummary
from jade.resource_monitor import (
    CpuStatsViewer,
    DiskStatsViewer,
    MemoryStatsViewer,
    NetworkStatsViewer,
)
from jade.utils.utils import dump_data, load_data


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

    plot_dir = Path(output) / STATS_DIR
    plot_dir.mkdir(exist_ok=True)
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
            print(f"Invalid stat={stat}", file=sys.stderr)
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
    events_path = Path(output) / EVENTS_DIR
    stats_path = Path(output) / STATS_DIR
    if not events_path.exists():
        print(f"{output} does not contain JADE stats", file=sys.stderr)
        sys.exit(1)
    if not stats:
        stats = STATS
    if stats_path.exists():
        json_files = list(stats_path.glob("*.json"))
        json_files.sort()
    else:
        json_files = []
    if json_files:
        _show_summary_stats(stats, json_summary, json_files)
    else:
        _show_periodic_stats(stats, json_summary, output, summary_only)
    return 0


def _show_summary_stats(stats, json_summary, json_files):
    type_mapping = {
        CpuStatsViewer.metric(): "cpu",
        DiskStatsViewer.metric(): "disk",
        MemoryStatsViewer.metric(): "mem",
        NetworkStatsViewer.metric(): "net",
    }
    reverse_mapping = {v: k for k, v in type_mapping.items()}
    cls_mapping = {
        CpuStatsViewer.metric(): CpuStatsViewer,
        DiskStatsViewer.metric(): DiskStatsViewer,
        MemoryStatsViewer.metric(): MemoryStatsViewer,
        NetworkStatsViewer.metric(): NetworkStatsViewer,
    }
    filtered = []
    for filename in json_files:
        data = load_data(filename)
        for entry in data:
            if type_mapping[entry["type"]] in stats:
                filtered.append(entry)

    if json_summary:
        print(json.dumps(filtered, indent=2))
    else:
        by_type_and_batch = defaultdict(dict)
        for entry in filtered:
            if entry["batch"] not in by_type_and_batch[entry["type"]]:
                by_type_and_batch[entry["type"]][entry["batch"]] = []
            by_type_and_batch[entry["type"]][entry["batch"]].append(entry)
        for stat in stats:
            text = f"{reverse_mapping[stat]} statistics for each batch"
            print(f"\n{text}")
            print("=" * len(text) + "\n")

            stat_type = reverse_mapping[stat]
            stat_cls = cls_mapping[stat_type]
            for resource_type in by_type_and_batch:
                if resource_type != reverse_mapping[stat]:
                    continue
                for batch, entries in by_type_and_batch[resource_type].items():
                    for entry in entries:
                        table = PrettyTable(title=f"{stat_type} {batch} summary")
                        table.field_names = [stat] + list(entry["average"].keys())
                        row = ["Average"]
                        for field, val in entry["average"].items():
                            row.append(stat_cls.get_printable_value(field, val))
                        table.add_row(row)
                        row = ["Minimum"]
                        for field, val in entry["minimum"].items():
                            row.append(stat_cls.get_printable_value(field, val))
                        table.add_row(row)
                        row = ["Maximum"]
                        for field, val in entry["maximum"].items():
                            row.append(stat_cls.get_printable_value(field, val))
                        table.add_row(row)
                        print(table)
                        print()


def _show_periodic_stats(stats, json_summary, output, summary_only):
    events = EventsSummary(output)

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
            print(f"Invalid stat={stat}", file=sys.stderr)
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
