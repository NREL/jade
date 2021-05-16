"""CLI to run jobs."""

import logging
import os
import shutil
import sys
import time

import click

from jade.events import EventsSummary
from jade.loggers import setup_logging
from jade.resource_monitor import ResourceMonitor


logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "-d",
    "--duration",
    default=None,
    type=int,
    show_default=True,
    help="Total time to collect resource stats. Default is infinite.",
)
@click.option(
    "-f",
    "--force",
    default=False,
    is_flag=True,
    show_default=True,
    help="Delete output directory if it exists.",
)
@click.option(
    "-i",
    "--interval",
    default=1,
    type=int,
    show_default=True,
    help="Interval in seconds on which to collect resource stats.",
)
@click.option("-o", "--output", default="stats", show_default=True, help="Output directory.")
def collect(duration, force, interval, output):
    """Collect resource utilization stats."""
    if os.path.exists(output):
        if force:
            shutil.rmtree(output)
        else:
            print(f"The directory {output} already exists. Delete it or run with --force")
            sys.exit(1)

    os.makedirs(output)
    event_file = os.path.join(output, "stats_events.log")
    setup_logging("event", event_file, console_level=logging.ERROR, file_level=logging.INFO)
    monitor = ResourceMonitor("ResourceMonitor")
    start_time = time.time()

    show_cmd = f"jade stats show -o {output} [STATS]"
    print(f"Collecting stats. When complete run '{show_cmd}' to view stats.")
    try:
        while True:
            monitor.log_resource_stats()
            time.sleep(interval)
            if duration is not None and time.time() - start_time > duration:
                print(f"Exceeded {duration} seconds. Exiting.")
                EventsSummary(output)
                break
    except KeyboardInterrupt:
        # TODO: This doesn't actually work. click catches KeyboardInterrupt.
        # Need to prevent it from doing that.
        # Then always call EventsSummary(output) at the end.
        pass
