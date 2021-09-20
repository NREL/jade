import logging
import os
import shutil
import tempfile

from jade.events import (
    EventsSummary,
    EVENT_NAME_CPU_STATS,
    EVENT_NAME_DISK_STATS,
    EVENT_NAME_MEMORY_STATS,
    EVENT_NAME_NETWORK_STATS,
)
from jade.loggers import setup_event_logging
from jade.resource_monitor import (
    ResourceMonitorLogger,
    CpuStatsViewer,
    DiskStatsViewer,
    MemoryStatsViewer,
    NetworkStatsViewer,
)
from jade.utils.subprocess_manager import run_command


def test_resource_stats():
    with tempfile.TemporaryDirectory() as tmpdir:
        event_file = os.path.join(tmpdir, "events.log")
        setup_event_logging(event_file)

        resource_monitor = ResourceMonitorLogger("test")
        count = 2
        for _ in range(count):
            resource_monitor.log_resource_stats()

        summary = EventsSummary(tmpdir)
        assert len(summary.list_events(EVENT_NAME_CPU_STATS)) == count
        assert len(summary.list_events(EVENT_NAME_DISK_STATS)) == count
        assert len(summary.list_events(EVENT_NAME_MEMORY_STATS)) == count
        assert len(summary.list_events(EVENT_NAME_NETWORK_STATS)) == count

        viewers = [
            CpuStatsViewer(summary),
            DiskStatsViewer(summary),
            MemoryStatsViewer(summary),
            NetworkStatsViewer(summary),
        ]
        for viewer in viewers:
            df = viewer.get_dataframe("test")
            assert len(df) == 2
            if isinstance(viewer, MemoryStatsViewer):
                viewer.get_dataframe("test")
                averages = viewer._calc_batch_averages("test")
                for field, val in averages.items():
                    assert val == df[field].mean()

        output = {}
        cmd = f"jade stats show -o {tmpdir} cpu disk mem net"
        ret = run_command(cmd, output=output)
        assert ret == 0
        for term in ("IOPS", "read_bytes", "bytes_recv", "idle"):
            assert term in output["stdout"]


def test_collect_stats():
    output_dir = os.path.join(tempfile.gettempdir(), "test-stats-output")
    try:
        ret = run_command(f"jade stats collect -i1 -o {output_dir} -d 1 -f")
        assert ret == 0
        cmd = f"jade stats show -o {output_dir} cpu disk mem net"
        output = {}
        ret = run_command(cmd, output=output)
        assert ret == 0
        for term in ("IOPS", "read_bytes", "bytes_recv", "idle"):
            assert term in output["stdout"]
    finally:
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
