"""CLI to run jobs."""

import logging
import os
import shutil
import signal
import sys
import time

import click
import psutil

from jade.events import EventsSummary
from jade.loggers import setup_event_logging
from jade.models.submitter_params import ResourceMonitorStats
from jade.resource_monitor import ResourceMonitorLogger
from jade.utils.run_command import check_run_command


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
    "-i",
    "--interval",
    default=1,
    type=int,
    show_default=True,
    help="Interval in seconds on which to collect resource stats.",
)
@click.option("-o", "--output", default="stats", show_default=True, help="Output directory.")
@click.option(
    "--plots/--no-plots",
    default=False,
    is_flag=True,
    show_default=True,
    help="Generate plots when collection is complete.",
)
@click.option(
    "--cpu/--no-cpu", default=True, is_flag=True, show_default=True, help="Enable CPU monitoring"
)
@click.option(
    "--disk/--no-disk",
    default=True,
    is_flag=True,
    show_default=True,
    help="Enable disk monitoring",
)
@click.option(
    "--memory/--no-memory",
    default=True,
    is_flag=True,
    show_default=True,
    help="Enable memory monitoring",
)
@click.option(
    "--network/--no-network",
    default=True,
    is_flag=True,
    show_default=True,
    help="Enable network monitoring",
)
@click.option(
    "-p",
    "--process-ids",
    multiple=True,
    show_default=True,
    type=int,
    help="Process IDs to monitor. Ex: -p 3114 -p 3115",
)
@click.option(
    "-u",
    "--process-ids-from-user",
    type=str,
    help="Monitor all process IDs from this username. Can be combined with --process-ids-from-substring",
)
@click.option(
    "-P",
    "--process-ids-from-substrings",
    type=str,
    multiple=True,
    help="Monitor all process IDs with command line exec + args that include one or more substrings.",
)
@click.option(
    "-r",
    "--refresh-process-IDs",
    is_flag=True,
    default=False,
    show_default=True,
    help="Refresh the process IDs every interval. Default behavior is to only monitor the "
    "original IDs.",
)
@click.option(
    "--children/--no-children",
    default=False,
    is_flag=True,
    show_default=True,
    help="Aggregate child process utilization.",
)
@click.option(
    "--recurse-children/--no-recurse-children",
    default=False,
    is_flag=True,
    show_default=True,
    help="Search for all child processes recursively.",
)
@click.option(
    "-f",
    "--force",
    default=False,
    is_flag=True,
    show_default=True,
    help="Delete output directory if it exists.",
)
@click.pass_context
def collect(
    ctx,
    duration,
    interval,
    output,
    plots,
    cpu,
    disk,
    memory,
    network,
    process_ids,
    process_ids_from_user,
    process_ids_from_substrings,
    refresh_process_ids,
    children,
    recurse_children,
    force,
):
    """Collect resource utilization stats. Stop collection by setting duration, pressing Ctrl-c,
    or sending SIGTERM to the process ID.
    """
    if os.path.exists(output):
        if force:
            shutil.rmtree(output)
        else:
            print(
                f"The directory {output} already exists. Delete it or run with --force",
                file=sys.stderr,
            )
            sys.exit(1)

    process = bool(process_ids or process_ids_from_user or process_ids_from_substrings)
    if process_ids and (process_ids_from_user or process_ids_from_substrings):
        print("Explicit process IDs and process ID filters cannot both be passed", file=sys.stderr)
        sys.exit(1)

    os.makedirs(output)
    event_file = os.path.join(output, "stats_events.log")
    setup_event_logging(event_file, mode="a")
    stats = ResourceMonitorStats(
        cpu=cpu,
        disk=disk,
        memory=memory,
        network=network,
        process=process,
        include_child_processes=children,
        recurse_child_processes=recurse_children,
    )

    if process_ids:
        process_id_mapping = get_process_id_mapping(process_ids)
    else:
        process_id_mapping = None

    monitor = ResourceMonitorLogger("ResourceMonitor", stats)
    start_time = time.time()

    show_cmd = f"jade stats show -o {output} [STATS]"
    print(f"Collecting stats. When complete run '{show_cmd}' to view stats.", file=sys.stderr)
    signal.signal(signal.SIGTERM, sigterm_handler)
    with ctx:
        try:
            while g_collect_stats:
                if stats.process:
                    if (process_ids_from_user or process_ids_from_substrings) and (
                        process_id_mapping is None or refresh_process_ids
                    ):
                        process_id_mapping = find_process_ids(
                            process_ids_from_user, process_ids_from_substrings
                        )
                monitor.log_resource_stats(ids=process_id_mapping)
                time.sleep(interval)
                if duration is not None and time.time() - start_time > duration:
                    print(f"Exceeded {duration} seconds. Exiting.", file=sys.stderr)
                    break
        except KeyboardInterrupt:
            print(" Detected Ctrl-c, exiting", file=sys.stderr)
        if plots:
            check_run_command(f"jade stats plot -o {output}")
        else:
            # This generates parquet files for each stat.
            EventsSummary(output)


def get_process_mapping_name(process: psutil.Process):
    """Return a mapping of name to pid. The name should be suitable for plots."""
    # Including the entire command line is often way too long.
    # name() is often better than the first arg (python instead of full path to python)
    # This tries to get the best of all worlds and ensure uniqueness.
    cmdline = process.cmdline()
    if len(cmdline) > 1:
        name = process.name() + " " + " ".join(cmdline[1:])
    else:
        name = process.name()

    if len(name) > 20:
        name = name[:20] + "..."

    return name + f" ({process.pid})"


def get_process_id_mapping(process_ids):
    process_id_mapping = {}
    for pid in process_ids:
        process = psutil.Process(pid)
        process_id_mapping[get_process_mapping_name(process)] = pid

    return process_id_mapping


def find_process_ids(user, substrings):
    process_id_mapping = {}
    for process in psutil.process_iter():
        if process.pid == os.getpid():
            # Don't track this process.
            continue
        try:
            if user is not None and process.username() != user:
                continue
            found_substring = False
            for substring in substrings:
                if substring in " ".join(process.cmdline()):
                    found_substring = True
            if not found_substring:
                continue
            process_id_mapping[get_process_mapping_name(process)] = process.pid
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    return process_id_mapping


g_collect_stats = True


def sigterm_handler(signum, frame):
    global g_collect_stats
    print("Detected SIGTERM", file=sys.stderr)
    g_collect_stats = False
