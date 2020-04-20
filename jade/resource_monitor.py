
import abc
import logging
import os
import time

import pandas as pd
from prettytable import PrettyTable
import psutil
from psutil._common import bytes2human

from jade.events import EVENT_CATEGORY_RESOURCE_UTIL, EVENT_CODE_CPU_STATS, \
    EVENT_CODE_DISK_STATS, EVENT_CODE_MEMORY_STATS, StructuredEvent
from jade.loggers import log_job_event


logger = logging.getLogger(__name__)


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

    def __init__(self, name):
        self._name = name
        self._last_disk_check_time = None
        self._update_disk_stats(psutil.disk_io_counters())

    def log_resource_stats(self):
        """Logs resource stats information as structured job events."""
        self.log_cpu_stats()
        self.log_disk_stats()
        self.log_memory_stats()

    def _update_disk_stats(self, data):
        for stat in self.DISK_STATS:
            setattr(self, stat, getattr(data, stat))
        self._last_disk_check_time = time.time()

    def log_cpu_stats(self):
        """Logs CPU resource stats information."""
        cpu_stats = psutil.cpu_times_percent()._asdict()
        cpu_stats["cpu_percent"] = psutil.cpu_percent()

        log_job_event(
            StructuredEvent(
                name=self._name,
                category=EVENT_CATEGORY_RESOURCE_UTIL,
                code=EVENT_CODE_CPU_STATS,
                message="Node CPU stats update",
                **cpu_stats,
            )
        )

    def log_disk_stats(self):
        """Logs disk stats."""
        data = psutil.disk_io_counters()
        disk_stats = {}
        for stat in self.DISK_STATS:
            disk_stats[stat] = getattr(data, stat) - getattr(self, stat)
        disk_stats["elapsed_seconds"] = time.time() - self._last_disk_check_time

        self._update_disk_stats(data)
        
        log_job_event(
            StructuredEvent(
                name=self._name,
                category=EVENT_CATEGORY_RESOURCE_UTIL,
                code=EVENT_CODE_DISK_STATS,
                message="Node disk stats update",
                **disk_stats,
            )
        )

    def log_memory_stats(self):
        """Logs memory resource stats information."""
        mem_stats = psutil.virtual_memory()._asdict()
        log_job_event(
            StructuredEvent(
                name=self._name,
                category=EVENT_CATEGORY_RESOURCE_UTIL,
                code=EVENT_CODE_MEMORY_STATS,
                message="Node memory stats update",
                **mem_stats,
            )
        )


class StatsViewerBase(abc.ABC):
    """Base class for viewing statistics"""
    def __init__(self, events, event_code):
        self._event_code = event_code
        self._events_by_batch = {}
        self._stat_sums_by_batch = {}
        self._stat_totals = {}
        self._num_events = 0

        for event in events:
            if event.code != event_code:
                continue
            self._num_events += 1
            if not self._stat_totals:
                self._stat_totals = {
                    x: 0 for x in event.data.keys()
                }
            batch = event.name
            if batch not in self._events_by_batch:
                self._events_by_batch[batch] = []
            self._events_by_batch[batch].append(event)

            if batch not in self._stat_sums_by_batch:
                self._stat_sums_by_batch[batch] = {
                    x: 0 for x in event.data.keys()
                }
            for field, val in event.data.items():
                self._stat_sums_by_batch[batch][field] += val
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
            data = {}
            data.update(event.data)
            data["timestamp"] = event.timestamp
            records.append(data)

        return pd.DataFrame.from_records(records, index="timestamp")

    @abc.abstractmethod
    def show_stats(self):
        """Show statistics"""

    def _show_stats(self):
        for batch, events in self._events_by_batch.items():
            print(batch)
            if not events:
                continue
            table = PrettyTable()
            table.field_names = ["timestamp"] + list(events[0].data.keys())
            for event in events:
                row = [event.timestamp]
                for field, val in event.data.items():
                    row.append(self._get_printable_value(field, val))
                table.add_row(row)
            row = ["Average"]
            for field, val in self._calc_batch_averages(batch).items():
                row.append(self._get_printable_value(field, val))
            table.add_row(row)
            print(table)
            #print(self._calc_total_averages())
            print("\n", end="")

        print("Averages across batches")
        table = PrettyTable()
        averages = self._calc_total_averages()
        table.field_names = list(averages.keys())
        row = [self._get_printable_value(k, v) for k, v in averages.items()]
        table.add_row(row)
        print(table)
        print("\n", end="")


class CpuStatsViewer(StatsViewerBase):
    """Shows CPU statistics"""
    def __init__(self, events):
        super(CpuStatsViewer, self).__init__(events, EVENT_CODE_CPU_STATS)

    def show_stats(self):
        print("\nCPU statistics for each batch\n")
        self._show_stats()


class DiskStatsViewer(StatsViewerBase):
    """Shows disk statistics"""
    def __init__(self, events):
        super(DiskStatsViewer, self).__init__(events, EVENT_CODE_DISK_STATS)

    @staticmethod
    def _get_printable_value(field, val):
        if field in ("read_bytes", "write_bytes"):
            val = bytes2human(val)
        elif field in ("read_time", "write_time"):
            seconds = float(val) / 1000
            val = "{:.3f}".format(seconds)
        return val
        
    def show_stats(self):
        if not self._events_by_batch:
            print("No events are stored")
            return

        table = PrettyTable()
        fields = ["Batch"] + list(ResourceMonitor.DISK_STATS)
        for i, field in enumerate(fields):
            if field in ("read_time", "write_time"):
                fields[i] = field + (" (s)")

        table.field_names = fields
        for batch, stats in self._stat_sums_by_batch.items():
            row = [batch]
            for stat in ResourceMonitor.DISK_STATS:
                val = self._get_printable_value(stat, stats[stat])
                row.append(val)
            table.add_row(row)

        total_row = ["Total"]
        for stat in ResourceMonitor.DISK_STATS:
            val = self._get_printable_value(stat, self._stat_totals[stat])
            total_row.append(val)

        table.add_row(total_row)
        print(table)


class MemoryStatsViewer(StatsViewerBase):
    """Shows Memory statistics"""
    def __init__(self, events):
        super(MemoryStatsViewer, self).__init__(events, EVENT_CODE_MEMORY_STATS)

    @staticmethod
    def _get_printable_value(field, val):
        if field == "percent":
            val = "{:.3f}".format(val)
        else:
            val = bytes2human(val)
        return val

    def show_stats(self):
        print("\nMemory statistics for each batch\n")
        self._show_stats()
