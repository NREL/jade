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

from jade.events import (
    EVENT_CATEGORY_RESOURCE_UTIL,
    EVENT_NAME_CPU_STATS,
    EVENT_NAME_DISK_STATS,
    EVENT_NAME_MEMORY_STATS,
    EVENT_NAME_NETWORK_STATS,
    StructuredLogEvent,
)
from jade.loggers import log_event


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

    def log_resource_stats(self):
        """Logs resource stats information as structured job events."""
        self.log_cpu_stats()
        self.log_disk_stats()
        self.log_memory_stats()
        self.log_network_stats()

    def _update_disk_stats(self, data):
        for stat in self.DISK_STATS:
            setattr(self, stat, getattr(data, stat, 0))
        self._last_disk_check_time = time.time()

    def _update_net_stats(self, data):
        for stat in self.NET_STATS:
            setattr(self, stat, getattr(data, stat, 0))
        self._last_net_check_time = time.time()

    def log_cpu_stats(self):
        """Logs CPU resource stats information."""
        cpu_stats = psutil.cpu_times_percent()._asdict()
        cpu_stats["cpu_percent"] = psutil.cpu_percent()

        log_event(
            StructuredLogEvent(
                source=self._name,
                category=EVENT_CATEGORY_RESOURCE_UTIL,
                name=EVENT_NAME_CPU_STATS,
                message="Node CPU stats update",
                **cpu_stats,
            )
        )

    @staticmethod
    def _mb_per_sec(num_bytes, elapsed_seconds):
        return float(num_bytes) / ONE_MB / elapsed_seconds

    def log_disk_stats(self):
        """Logs disk stats."""
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

        log_event(
            StructuredLogEvent(
                source=self._name,
                category=EVENT_CATEGORY_RESOURCE_UTIL,
                name=EVENT_NAME_DISK_STATS,
                message="Node disk stats update",
                **stats,
            )
        )

    def log_memory_stats(self):
        """Logs memory resource stats information."""
        mem_stats = psutil.virtual_memory()._asdict()
        log_event(
            StructuredLogEvent(
                source=self._name,
                category=EVENT_CATEGORY_RESOURCE_UTIL,
                name=EVENT_NAME_MEMORY_STATS,
                message="Node memory stats update",
                **mem_stats,
            )
        )

    def log_network_stats(self):
        """Logs memory resource stats information."""
        data = psutil.net_io_counters()
        stats = {
            "elapsed_seconds": time.time() - self._last_net_check_time,
        }
        for stat in self.NET_STATS:
            stats[stat] = getattr(data, stat, 0) - getattr(self, stat, 0)
        stats["recv MB/s"] = self._mb_per_sec(stats["bytes_recv"], stats["elapsed_seconds"])
        stats["sent MB/s"] = self._mb_per_sec(stats["bytes_sent"], stats["elapsed_seconds"])
        self._update_net_stats(data)

        log_event(
            StructuredLogEvent(
                source=self._name,
                category=EVENT_CATEGORY_RESOURCE_UTIL,
                name=EVENT_NAME_NETWORK_STATS,
                message="Node net stats update",
                **stats,
            )
        )


class StatsViewerBase(abc.ABC):
    """Base class for viewing statistics"""

    def __init__(self, events, event_name):
        self._event_name = event_name
        self._events_by_batch = defaultdict(list)
        self._stat_min_by_batch = {}
        self._stat_max_by_batch = {}
        self._stat_sums_by_batch = {}
        self._stat_totals = {}
        self._num_events = 0

        for event in events.iter_events(event_name):
            self._num_events += 1
            if not self._stat_totals:
                self._stat_totals = {x: 0 for x in event.data.keys()}
            batch = event.source
            self._events_by_batch[batch].append(event)

            if batch not in self._stat_sums_by_batch:
                self._stat_sums_by_batch[batch] = {x: 0 for x in event.data.keys()}
                self._stat_min_by_batch[batch] = {x: sys.maxsize for x in event.data.keys()}
                self._stat_max_by_batch[batch] = {x: -1 for x in event.data.keys()}
            for field, val in event.data.items():
                self._stat_sums_by_batch[batch][field] += val
                if val < self._stat_min_by_batch[batch][field]:
                    self._stat_min_by_batch[batch][field] = val
                if val > self._stat_max_by_batch[batch][field]:
                    self._stat_max_by_batch[batch][field] = val
                self._stat_totals[field] += val

    def _calc_batch_averages(self, batch):
        averages = {}
        for field, val in self._stat_sums_by_batch[batch].items():
            averages[field] = float(val) / len(self._events_by_batch[batch])
        return averages

    def _calc_total_averages(self):
        averages = {}
        for field, val in self._stat_totals.items():
            averages[field] = float(val) / self._num_events
        return averages

    def _get_batch_minimums(self, batch):
        return self._stat_min_by_batch[batch]

    def _get_batch_maximums(self, batch):
        return self._stat_max_by_batch[batch]

    @staticmethod
    def _get_printable_value(field, val):
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
        records = []
        for event in self._events_by_batch[batch]:
            data = {"timestamp": event.timestamp}
            data.update(event.data)
            records.append(data)

        return pd.DataFrame.from_records(records, index="timestamp")

    def iter_batch_names(self):
        """Return an iterator over the batch names."""
        return self._events_by_batch.keys()

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
            title = f"{self.__class__.__name__} {name}"
            fig = df.plot(title=title)
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
        for batch, events in self._events_by_batch.items():
            if not events:
                continue
            entry = {
                "type": self.metric(),
                "batch": batch,
                "average": {},
                "minimum": {},
                "maximum": {},
            }
            for field, val in self._calc_batch_averages(batch).items():
                entry["average"][field] = val
            for field, val in self._get_batch_minimums(batch).items():
                entry["minimum"][field] = val
            for field, val in self._get_batch_maximums(batch).items():
                entry["maximum"][field] = val
            stats.append(entry)

        return stats

    def _show_stats(self, show_all_timestamps=True):
        for batch, events in self._events_by_batch.items():
            if not events:
                continue
            if show_all_timestamps:
                table = PrettyTable(title=f"{self.metric()} {batch}")
                table.field_names = ["timestamp"] + list(events[0].data.keys())
                for event in events:
                    row = [event.timestamp]
                    for field, val in event.data.items():
                        row.append(self._get_printable_value(field, val))
                    table.add_row(row)
                print(table)
                print("\n", end="")
            table = PrettyTable(title=f"{self.metric()} {batch} summary")
            table.field_names = ["stat"] + list(events[0].data.keys())
            row = ["Average"]
            for field, val in self._calc_batch_averages(batch).items():
                row.append(self._get_printable_value(field, val))
            table.add_row(row)
            row = ["Minimum"]
            for field, val in self._get_batch_minimums(batch).items():
                row.append(self._get_printable_value(field, val))
            table.add_row(row)
            row = ["Maximum"]
            for field, val in self._get_batch_maximums(batch).items():
                row.append(self._get_printable_value(field, val))
            table.add_row(row)
            print(table)
            print("\n", end="")

        table = PrettyTable(title=f"{self.metric()} averages per interval across batches")
        averages = self._calc_total_averages()
        table.field_names = list(averages.keys())
        row = [self._get_printable_value(k, v) for k, v in averages.items()]
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
        for batch, totals in self._stat_sums_by_batch.items():
            row = [batch]
            for stat in stats_to_total:
                val = self._get_printable_value(stat, totals[stat])
                row.append(val)
            table.add_row(row)

        if self._stat_totals:
            total_row = ["total"]
            for stat in stats_to_total:
                val = self._get_printable_value(stat, self._stat_totals[stat])
                total_row.append(val)
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
    def _get_printable_value(field, val):
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
    def _get_printable_value(field, val):
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
    def _get_printable_value(field, val):
        if field in ("bytes_recv", "bytes_sent"):
            val = bytes2human(val)
        elif isinstance(val, float):
            val = "{:.3f}".format(val)
        return val

    def show_stats(self, show_all_timestamps=True):
        print("\nNetwork statistics for each batch")
        print("=================================\n")
        if not self._events_by_batch:
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
