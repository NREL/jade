import abc
import logging
import sys
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd
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
    StructuredLogEvent,
)
from jade.loggers import log_event
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

    @staticmethod
    def _mb_per_sec(num_bytes, elapsed_seconds):
        return float(num_bytes) / ONE_MB / elapsed_seconds

    @property
    def name(self):
        """Return the name of the monitor."""
        return self._name


class ResourceMonitorAggregator:
    """Aggregates resource utilization stats in memory."""

    def __init__(self, name):
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
            for stat_name, val in stat_dict.items():
                self._summaries["average"][resource_type][stat_name] = 0.0
                self._summaries["maximum"][resource_type][stat_name] = 0.0
                self._summaries["minimum"][resource_type][stat_name] = sys.maxsize
                self._summaries["sum"][resource_type][stat_name] = 0.0

    def _get_stats(self):
        return {
            CpuStatsViewer.metric(): self._monitor.get_cpu_stats(),
            DiskStatsViewer.metric(): self._monitor.get_disk_stats(),
            MemoryStatsViewer.metric(): self._monitor.get_memory_stats(),
            NetworkStatsViewer.metric(): self._monitor.get_network_stats(),
        }

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

        path = Path(output_dir) / STATS_DIR
        filename = path / f"{self.name}_resource_stats.json"
        dump_data(stat_summaries, filename)

    @property
    def name(self):
        """Return the name of the monitor."""
        return self._monitor.name

    def update_resource_stats(self):
        """Update resource stats information as structured job events for the current interval."""
        cur_stats = self._get_stats()
        for resource_type, stat_dict in self._last_stats.items():
            for stat_name, val in stat_dict.items():
                val = cur_stats[resource_type][stat_name]
                if val > self._summaries["maximum"][resource_type][stat_name]:
                    self._summaries["maximum"][resource_type][stat_name] = val
                elif val < self._summaries["minimum"][resource_type][stat_name]:
                    self._summaries["minimum"][resource_type][stat_name] = val
                self._summaries["sum"][resource_type][stat_name] += val
        self._count += 1
        self._last_stats = cur_stats


class ResourceMonitorLogger:
    """Logs resource utilization stats on periodic basis."""

    def __init__(self, name):
        self._monitor = ResourceMonitor(name)

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

    def log_resource_stats(self):
        """Logs resource stats information as structured job events for the current interval."""
        self.log_cpu_stats()
        self.log_disk_stats()
        self.log_memory_stats()
        self.log_network_stats()

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

        for name in self.iter_batch_names():
            df = self.get_dataframe(name)
            cols = [x for x in df.columns if x != "source"]
            title = f"{self.__class__.__name__} {name}"
            fig = df[cols].plot(title=title)
            filename = Path(output_dir) / f"{self.__class__.__name__}__{name}.html"
            fig.write_html(str(filename))
            logger.info("Generated plot in %s", filename)

    @staticmethod
    @abc.abstractmethod
    def metric():
        """Return the metric."""

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
                print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))
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
        table.add_row(row)
        print(table)
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
