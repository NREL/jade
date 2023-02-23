import abc
import logging
import sys
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from prettytable import PrettyTable
import psutil
from psutil._common import bytes2human
from tabulate import tabulate

from jade.common import STATS_DIR
from jade.events import (
    EVENT_CATEGORY_RESOURCE_UTIL,
    EVENT_NAME_CPU_STATS,
    EVENT_NAME_DISK_STATS,
    EVENT_NAME_MEMORY_STATS,
    EVENT_NAME_NETWORK_STATS,
    EVENT_NAME_PROCESS_STATS,
    StructuredLogEvent,
)
from jade.loggers import log_event
from jade.models.submitter_params import ResourceMonitorStats
from jade.utils.utils import dump_data


logger = logging.getLogger(__name__)

ONE_MB = 1024 * 1024


class ResourceMonitor:
    """Monitors resource utilization statistics"""

    DISK_STATS = (
        "read_count",
        "write_count",
        "read_bytes",
        "write_bytes",
        "read_time",
        "write_time",
    )
    NET_STATS = (
        "bytes_recv",
        "bytes_sent",
        "dropin",
        "dropout",
        "errin",
        "errout",
        "packets_recv",
        "packets_sent",
    )

    def __init__(self, name):
        self._name = name
        self._last_disk_check_time = None
        self._last_net_check_time = None
        self._update_disk_stats(psutil.disk_io_counters())
        self._update_net_stats(psutil.net_io_counters())
        self._cached_processes = {}  # pid to psutil.Process

    def _update_disk_stats(self, data):
        for stat in self.DISK_STATS:
            setattr(self, stat, getattr(data, stat, 0))
        self._last_disk_check_time = time.time()

    def _update_net_stats(self, data):
        for stat in self.NET_STATS:
            setattr(self, stat, getattr(data, stat, 0))
        self._last_net_check_time = time.time()

    def get_cpu_stats(self):
        """Gets CPU current resource stats information."""
        stats = psutil.cpu_times_percent()._asdict()
        stats["cpu_percent"] = psutil.cpu_percent()
        return stats

    def get_disk_stats(self):
        """Gets current disk stats."""
        data = psutil.disk_io_counters()
        stats = {
            "elapsed_seconds": time.time() - self._last_disk_check_time,
        }
        for stat in self.DISK_STATS:
            stats[stat] = getattr(data, stat, 0) - getattr(self, stat, 0)
        stats["read MB/s"] = self._mb_per_sec(stats["read_bytes"], stats["elapsed_seconds"])
        stats["write MB/s"] = self._mb_per_sec(stats["write_bytes"], stats["elapsed_seconds"])
        stats["read IOPS"] = float(stats["read_count"]) / stats["elapsed_seconds"]
        stats["write IOPS"] = float(stats["write_count"]) / stats["elapsed_seconds"]
        self._update_disk_stats(data)
        return stats

    def get_memory_stats(self):
        """Gets current memory resource stats."""
        return psutil.virtual_memory()._asdict()

    def get_network_stats(self):
        """Gets current network stats."""
        data = psutil.net_io_counters()
        stats = {
            "elapsed_seconds": time.time() - self._last_net_check_time,
        }
        for stat in self.NET_STATS:
            stats[stat] = getattr(data, stat, 0) - getattr(self, stat, 0)
        stats["recv MB/s"] = self._mb_per_sec(stats["bytes_recv"], stats["elapsed_seconds"])
        stats["sent MB/s"] = self._mb_per_sec(stats["bytes_sent"], stats["elapsed_seconds"])
        self._update_net_stats(data)
        return stats

    def _get_process(self, pid):
        process = self._cached_processes.get(pid)
        if process is None:
            try:
                process = psutil.Process(pid)
                # Initialize CPU utilization tracking per psutil docs.
                process.cpu_percent(interval=0.2)
                self._cached_processes[pid] = process
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                logger.debug("Tried to construct Process for invalid pid=%s", pid)
                return None

        return process

    def clear_stale_processes(self, cur_pids):
        """Remove cached process objects that are no longer running."""
        self._cached_processes = {
            pid: proc for pid, proc in self._cached_processes.items() if pid in cur_pids
        }

    def get_process_stats(self, pid, include_children=True, recurse_children=False):
        """Gets current process stats. Returns None if the pid does not exist."""
        children = []
        process = self._get_process(pid)
        if process is None:
            return None, children
        try:
            with process.oneshot():
                stats = {
                    "rss": process.memory_info().rss,
                    "cpu_percent": process.cpu_percent(),
                }
                if include_children:
                    for child in process.children(recursive=recurse_children):
                        cached_child = self._get_process(child.pid)
                        if cached_child is not None:
                            stats["cpu_percent"] += cached_child.cpu_percent()
                            stats["rss"] += cached_child.memory_info().rss
                            children.append(child.pid)
                return stats, children
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            logger.debug("Tried to get process info for invalid pid=%s", pid)
            return None, []

    @staticmethod
    def _mb_per_sec(num_bytes, elapsed_seconds):
        return float(num_bytes) / ONE_MB / elapsed_seconds

    @property
    def name(self):
        """Return the name of the monitor."""
        return self._name


class ResourceMonitorAggregator:
    """Aggregates resource utilization stats in memory."""

    def __init__(
        self,
        name,
        stats: ResourceMonitorStats,
    ):
        self._stats = stats
        self._count = 0
        self._monitor = ResourceMonitor(name)
        self._last_stats = self._get_stats()
        self._summaries = {
            "average": defaultdict(dict),
            "maximum": defaultdict(dict),
            "minimum": defaultdict(dict),
            "sum": defaultdict(dict),
        }
        for resource_type, stat_dict in self._last_stats.items():
            for stat_name in stat_dict:
                self._summaries["average"][resource_type][stat_name] = 0.0
                self._summaries["maximum"][resource_type][stat_name] = 0.0
                self._summaries["minimum"][resource_type][stat_name] = sys.maxsize
                self._summaries["sum"][resource_type][stat_name] = 0.0

        self._process_summaries = {
            "average": defaultdict(dict),
            "maximum": defaultdict(dict),
            "minimum": defaultdict(dict),
            "sum": defaultdict(dict),
        }
        self._process_sample_count = {}

    def _get_stats(self):
        data = {}
        if self._stats.cpu:
            data[CpuStatsViewer.metric()] = self._monitor.get_cpu_stats()
        if self._stats.disk:
            data[DiskStatsViewer.metric()] = self._monitor.get_disk_stats()
        if self._stats.memory:
            data[MemoryStatsViewer.metric()] = self._monitor.get_memory_stats()
        if self._stats.network:
            data[NetworkStatsViewer.metric()] = self._monitor.get_network_stats()
        return data

    def _get_process_stats(self, pids):
        stats = {}
        cur_pids = set()
        for name, pid in pids.items():
            _stats, children = self._monitor.get_process_stats(
                pid,
                include_children=self._stats.include_child_processes,
                recurse_children=self._stats.recurse_child_processes,
            )
            if _stats is not None:
                stats[name] = _stats
                cur_pids.add(pid)
                for child in children:
                    cur_pids.add(child)

        self._monitor.clear_stale_processes(cur_pids)
        return stats

    def finalize(self, output_dir):
        """Finalize the stat summaries and record the results.

        Parameters
        ----------
        output_dir : str
            Directory in which to record the results.

        """
        if self._count == 0:
            logger.info("Resource monitoring was disabled")
            return

        for resource_type, stat_dict in self._summaries["sum"].items():
            for stat_name, val in stat_dict.items():
                self._summaries["average"][resource_type][stat_name] = val / self._count

        self._summaries.pop("sum")
        stat_summaries = []
        for resource_type in (
            CpuStatsViewer.metric(),
            DiskStatsViewer.metric(),
            MemoryStatsViewer.metric(),
            NetworkStatsViewer.metric(),
        ):
            # Make each entry look like what the stat viewers produce.
            summary = {"batch": self.name, "type": resource_type}
            for stat_type in self._summaries.keys():
                summary[stat_type] = self._summaries[stat_type][resource_type]
            stat_summaries.append(summary)

        for process_name, stat_dict in self._process_summaries["sum"].items():
            for stat_name, val in stat_dict.items():
                self._process_summaries["average"][process_name][stat_name] = (
                    val / self._process_sample_count[process_name]
                )

        self._process_summaries.pop("sum")
        for process_name, samples in self._process_sample_count.items():
            summary = {
                "batch": self.name,
                "name": process_name,
                "samples": samples,
                "type": ProcessStatsViewer.metric(),
            }
            for stat_type in self._process_summaries.keys():
                summary[stat_type] = self._process_summaries[stat_type][process_name]
            stat_summaries.append(summary)

        path = Path(output_dir) / STATS_DIR
        filename = path / f"{self.name}_resource_stats.json"
        dump_data(stat_summaries, filename)

    @property
    def name(self):
        """Return the name of the monitor."""
        return self._monitor.name

    def update_resource_stats(self, ids=None):
        """Update resource stats information as structured job events for the current interval."""
        cur_stats = self._get_stats()
        for resource_type, stat_dict in self._last_stats.items():
            for stat_name, val in stat_dict.items():
                if val > self._summaries["maximum"][resource_type][stat_name]:
                    self._summaries["maximum"][resource_type][stat_name] = val
                elif val < self._summaries["minimum"][resource_type][stat_name]:
                    self._summaries["minimum"][resource_type][stat_name] = val
                self._summaries["sum"][resource_type][stat_name] += val

        if self._stats.process:
            cur_process_stats = self._get_process_stats(ids)
            for process_name, stat_dict in cur_process_stats.items():
                if process_name in self._process_summaries["maximum"]:
                    for stat_name, val in stat_dict.items():
                        if val > self._process_summaries["maximum"][process_name][stat_name]:
                            self._process_summaries["maximum"][process_name][stat_name] = val
                        elif val < self._process_summaries["minimum"][process_name][stat_name]:
                            self._process_summaries["minimum"][process_name][stat_name] = val
                        self._process_summaries["sum"][process_name][stat_name] += val
                    self._process_sample_count[process_name] += 1
                else:
                    for stat_name, val in stat_dict.items():
                        self._process_summaries["maximum"][process_name][stat_name] = val
                        self._process_summaries["minimum"][process_name][stat_name] = val
                        self._process_summaries["sum"][process_name][stat_name] = val
                    self._process_sample_count[process_name] = 1

        self._count += 1
        self._last_stats = cur_stats


class ResourceMonitorLogger:
    """Logs resource utilization stats on periodic basis."""

    def __init__(
        self,
        name,
        stats: ResourceMonitorStats,
        include_child_processes=True,
        recurse_child_processes=False,
    ):
        self._monitor = ResourceMonitor(name)
        self._stats = stats
        self._include_child_processes = include_child_processes
        self._recurse_child_processes = recurse_child_processes

    def log_cpu_stats(self):
        """Logs CPU resource stats information."""
        cpu_stats = self._monitor.get_cpu_stats()
        log_event(
            StructuredLogEvent(
                source=self.name,
                category=EVENT_CATEGORY_RESOURCE_UTIL,
                name=EVENT_NAME_CPU_STATS,
                message="Node CPU stats update",
                **cpu_stats,
            )
        )

    def log_disk_stats(self):
        """Logs disk stats."""
        stats = self._monitor.get_disk_stats()
        log_event(
            StructuredLogEvent(
                source=self.name,
                category=EVENT_CATEGORY_RESOURCE_UTIL,
                name=EVENT_NAME_DISK_STATS,
                message="Node disk stats update",
                **stats,
            )
        )

    def log_memory_stats(self):
        """Logs memory resource stats information."""
        mem_stats = self._monitor.get_memory_stats()
        log_event(
            StructuredLogEvent(
                source=self.name,
                category=EVENT_CATEGORY_RESOURCE_UTIL,
                name=EVENT_NAME_MEMORY_STATS,
                message="Node memory stats update",
                **mem_stats,
            )
        )

    def log_network_stats(self):
        """Logs memory resource stats information."""
        stats = self._monitor.get_network_stats()
        log_event(
            StructuredLogEvent(
                source=self.name,
                category=EVENT_CATEGORY_RESOURCE_UTIL,
                name=EVENT_NAME_NETWORK_STATS,
                message="Node net stats update",
                **stats,
            )
        )

    def log_process_stats(self, pids):
        """Log stats for each process.

        Parameters
        ----------
        ids : dict, defaults to None
            Maps job name to process ID.

        """
        stats = {"processes": []}
        cur_pids = set()
        for name, pid in pids.items():
            stat, children = self._monitor.get_process_stats(
                pid,
                include_children=self._stats.include_child_processes,
                recurse_children=self._stats.recurse_child_processes,
            )
            if stat is not None:  # The process could have exited.
                stat["name"] = name
                stats["processes"].append(stat)
                cur_pids.add(pid)
                for child in children:
                    cur_pids.add(child)

        self._monitor.clear_stale_processes(cur_pids)

        if stats["processes"]:
            log_event(
                StructuredLogEvent(
                    source=self.name,
                    category=EVENT_CATEGORY_RESOURCE_UTIL,
                    name=EVENT_NAME_PROCESS_STATS,
                    message="Process stats update",
                    **stats,
                )
            )

    def log_resource_stats(self, ids=None):
        """Logs resource stats information as structured job events for the current interval.

        Parameters
        ----------
        ids : dict, defaults to None
            Maps job name to process ID.

        """
        if self._stats.cpu:
            self.log_cpu_stats()
        if self._stats.disk:
            self.log_disk_stats()
        if self._stats.memory:
            self.log_memory_stats()
        if self._stats.network:
            self.log_network_stats()
        if self._stats.process and ids is not None:
            self.log_process_stats(ids)

    @property
    def name(self):
        """Return the name of the monitor."""
        return self._monitor.name


class StatsViewerBase(abc.ABC):
    """Base class for viewing statistics"""

    def __init__(self, events, event_name):
        self._event_name = event_name
        self._df_by_batch = {}

        df = events.get_dataframe(event_name)
        if not df.empty:
            for batch in df["source"].unique():
                self._df_by_batch[batch] = df.loc[df["source"] == batch]

    @staticmethod
    def get_printable_value(field, val):
        if isinstance(val, float):
            return "{:.3f}".format(val)
        return str(val)

    def get_dataframe(self, batch):
        """Return a dataframe for the batch's stats.

        Parameters
        ----------
        batch : str

        Returns
        -------
        pd.DataFrame

        """
        if batch not in self._df_by_batch:
            return pd.DataFrame()
        return self._df_by_batch[batch]

    def iter_batch_names(self):
        """Return an iterator over the batch names."""
        return self._df_by_batch.keys()

    def plot_to_file(self, output_dir):
        """Make plots of resource utilization for one node.

        Parameters
        ----------
        directory : str
            output directory

        """
        if pd.options.plotting.backend != "plotly":
            pd.options.plotting.backend = "plotly"

        exclude = self._non_plottable_columns()
        for name in self.iter_batch_names():
            df = self.get_dataframe(name)
            cols = [x for x in df.columns if x not in exclude]
            title = f"{self.__class__.__name__} {name}"
            fig = df[cols].plot(title=title)
            filename = Path(output_dir) / f"{self.__class__.__name__}__{name}.html"
            fig.write_html(str(filename))
            logger.info("Generated plot in %s", filename)

    @staticmethod
    @abc.abstractmethod
    def metric():
        """Return the metric."""

    @staticmethod
    def _non_plottable_columns():
        """Return the columns that cannot be plotted."""
        return {"source"}

    def show_stats(self, show_all_timestamps=True):
        """Show statistics"""
        text = f"{self.metric()} statistics for each batch"
        print(f"\n{text}")
        print("=" * len(text) + "\n")
        self._show_stats(show_all_timestamps=show_all_timestamps)

    def get_stats_summary(self):
        """Return a list of objects describing summaries of all stats.

        Returns
        -------
        list
            list of dicts

        """
        stats = []
        for batch, df in self._df_by_batch.items():
            if df.empty:
                continue
            entry = {
                "type": self.metric(),
                "batch": batch,
                "average": {},
                "minimum": {},
                "maximum": {},
            }
            exclude = ("timestamp", "source")
            cols = [x for x in df.columns if x not in exclude]
            entry["average"].update(df[cols].mean().to_dict())
            entry["minimum"].update(df[cols].min().to_dict())
            entry["maximum"].update(df[cols].max().to_dict())
            stats.append(entry)

        return stats

    def _show_stats(self, show_all_timestamps=True):
        avg_across_batches = pd.DataFrame()
        for batch, df in self._df_by_batch.items():
            if df.empty:
                continue
            if show_all_timestamps:
                print(tabulate(df, headers="keys", tablefmt="psql", showindex=True))
                print(f"Title = {self.metric()} {batch}\n")
                print("\n", end="")
            table = PrettyTable(title=f"{self.metric()} {batch} summary")
            row = ["Average"]
            exclude = ("timestamp", "source")
            cols = [x for x in df.columns if x not in exclude]
            table.field_names = ["stat"] + cols
            average = df[cols].mean()
            avg_across_batches[batch] = average
            for field, val in average.to_dict().items():
                if field not in exclude:
                    row.append(self.get_printable_value(field, val))
            table.add_row(row)
            row = ["Minimum"]
            for field, val in df.min().to_dict().items():
                if field not in exclude:
                    row.append(self.get_printable_value(field, val))
            table.add_row(row)
            row = ["Maximum"]
            for field, val in df.max().to_dict().items():
                if field not in exclude:
                    row.append(self.get_printable_value(field, val))
            table.add_row(row)
            print(table)
            print("\n", end="")

        table = PrettyTable(title=f"{self.metric()} averages per interval across batches")
        averages = avg_across_batches.transpose().mean().to_dict()
        table.field_names = list(averages.keys())
        row = [self.get_printable_value(k, v) for k, v in averages.items()]
        if row:
            table.add_row(row)
            print(table)
        else:
            print("No events are stored")
        print("\n", end="")

    def show_stat_totals(self, stats_to_total):
        """Print a table that shows statistic totals by batch.

        Parameters
        ----------
        stats_to_total : list

        """
        table = PrettyTable(title=f"{self.metric()} Totals")
        table.field_names = ["source"] + list(stats_to_total)
        totals = {}
        for batch, df in self._df_by_batch.items():
            row = [batch]
            for stat in stats_to_total:
                total = df[stat].sum()
                if stat not in totals:
                    totals[stat] = total
                else:
                    totals[stat] += total
                val = self.get_printable_value(stat, total)
                row.append(val)
            table.add_row(row)

        if totals:
            total_row = ["total"]
            for stat, val in totals.items():
                total_row.append(self.get_printable_value(stat, val))
            table.add_row(total_row)
            print(table)


class CpuStatsViewer(StatsViewerBase):
    """Shows CPU statistics"""

    def __init__(self, events):
        super(CpuStatsViewer, self).__init__(events, EVENT_NAME_CPU_STATS)

    @staticmethod
    def metric():
        return "CPU"


class DiskStatsViewer(StatsViewerBase):
    """Shows disk statistics"""

    def __init__(self, events):
        super(DiskStatsViewer, self).__init__(events, EVENT_NAME_DISK_STATS)

    @staticmethod
    def metric():
        return "Disk"

    @staticmethod
    def get_printable_value(field, val):
        if field in ("read_bytes", "write_bytes"):
            val = bytes2human(val)
        elif isinstance(val, float):
            val = "{:.3f}".format(val)
        return val

    def show_stats(self, show_all_timestamps=True):
        print("\nDisk statistics for each batch")
        print("==============================\n")
        self._show_stats(show_all_timestamps=show_all_timestamps)

        stats_to_total = (
            "read_bytes",
            "read_count",
            "write_bytes",
            "write_count",
            "read_time",
            "write_time",
        )
        self.show_stat_totals(stats_to_total)


class MemoryStatsViewer(StatsViewerBase):
    """Shows Memory statistics"""

    def __init__(self, events):
        super(MemoryStatsViewer, self).__init__(events, EVENT_NAME_MEMORY_STATS)

    @staticmethod
    def metric():
        return "Memory"

    @staticmethod
    def get_printable_value(field, val):
        if field == "percent":
            val = "{:.3f}".format(val)
        else:
            val = bytes2human(val)
        return val


class NetworkStatsViewer(StatsViewerBase):
    """Shows Network statistics"""

    def __init__(self, events):
        super(NetworkStatsViewer, self).__init__(events, EVENT_NAME_NETWORK_STATS)

    @staticmethod
    def metric():
        return "Network"

    @staticmethod
    def get_printable_value(field, val):
        if field in ("bytes_recv", "bytes_sent"):
            val = bytes2human(val)
        elif isinstance(val, float):
            val = "{:.3f}".format(val)
        return val

    def show_stats(self, show_all_timestamps=True):
        print("\nNetwork statistics for each batch")
        print("=================================\n")
        if not self._df_by_batch:
            print("No events are stored")
            return

        self._show_stats(show_all_timestamps=show_all_timestamps)
        stats_to_total = (
            "bytes_recv",
            "bytes_sent",
            "dropin",
            "dropout",
            "errin",
            "errout",
            "packets_recv",
            "packets_sent",
        )
        self.show_stat_totals(stats_to_total)


class ProcessStatsViewer(StatsViewerBase):
    """Shows process statistics"""

    def __init__(self, events):
        super().__init__(events, EVENT_NAME_PROCESS_STATS)

    @staticmethod
    def metric():
        return "Process"

    @staticmethod
    def _non_plottable_columns():
        """Return the columns that cannot be plotted."""
        return {"name", "source"}

    def get_stats_summary(self):
        """Return a list of objects describing summaries of all stats.

        Returns
        -------
        list
            list of dicts

        """
        stats = []
        for batch, df in self._df_by_batch.items():
            if df.empty:
                continue
            for name, df_name in df.groupby(by="name"):
                entry = {
                    "type": self.metric(),
                    "name": name,
                    "batch": batch,
                    "average": {},
                    "minimum": {},
                    "maximum": {},
                }
                exclude = ("timestamp", "source", "name")
                cols = [x for x in df_name.columns if x not in exclude]
                entry["average"].update(df_name[cols].mean().to_dict())
                entry["minimum"].update(df_name[cols].min().to_dict())
                entry["maximum"].update(df_name[cols].max().to_dict())
                stats.append(entry)

        return stats

    def plot_to_file(self, output_dir):
        if pd.options.plotting.backend != "plotly":
            pd.options.plotting.backend = "plotly"

        exclude = self._non_plottable_columns()
        figures = {}  # column to go.Figure
        for name in self.iter_batch_names():
            df = self.get_dataframe(name)
            for pname, df_name in df.groupby(by="name"):
                for col in (x for x in df_name.columns if x not in exclude):
                    if col not in figures:
                        figures[col] = go.Figure()
                    series = df_name[col]
                    trace_name = f"{name} {pname}".replace("resource_monitor_", "")
                    figures[col].add_trace(go.Scatter(x=series.index, y=series, name=trace_name))

        for col, fig in figures.items():
            title = f"{self.__class__.__name__} {col}"
            fig.update_layout(title=title)
            filename = Path(output_dir) / f"{self.__class__.__name__}__{col}.html"
            fig.write_html(str(filename))
            logger.info("Generated plot in %s", filename)

    def show_stats(self, show_all_timestamps=True):
        text = f"{self.metric()} statistics for each job"
        print(f"\n{text}")
        print("=" * len(text) + "\n")
        self._show_stats(show_all_timestamps=show_all_timestamps)

    def _show_stats(self, show_all_timestamps=True):
        avg_across_processes = pd.DataFrame()
        for batch, df in self._df_by_batch.items():
            if df.empty:
                continue
            if show_all_timestamps:
                print(tabulate(df, headers="keys", tablefmt="psql", showindex=True))
                print(f"Title = {self.metric()} {batch}\n")
                print("\n", end="")
            for name, df_name in df.groupby(by="name"):
                table = PrettyTable(title=f"{self.metric()} {name} summary")
                row = ["Average"]
                exclude = ("timestamp", "source", "name")
                cols = [x for x in df_name.columns if x not in exclude]
                table.field_names = ["stat"] + cols
                average = df_name[cols].mean()
                avg_across_processes[name] = average
                for field, val in average.to_dict().items():
                    if field not in exclude:
                        row.append(self.get_printable_value(field, val))
                table.add_row(row)
                row = ["Minimum"]
                for field, val in df_name.min().to_dict().items():
                    if field not in exclude:
                        row.append(self.get_printable_value(field, val))
                table.add_row(row)
                row = ["Maximum"]
                for field, val in df_name.max().to_dict().items():
                    if field not in exclude:
                        row.append(self.get_printable_value(field, val))
                table.add_row(row)
                print(table)
                print("\n", end="")

        table = PrettyTable(title=f"{self.metric()} averages per interval across processes")
        averages = avg_across_processes.transpose().mean().to_dict()
        table.field_names = list(averages.keys())
        row = [self.get_printable_value(k, v) for k, v in averages.items()]
        if row:
            table.add_row(row)
            print(table)
        else:
            print("No events are stored")
        print("\n", end="")
