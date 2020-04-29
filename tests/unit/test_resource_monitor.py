
import logging
import os
import tempfile

from jade.events import EventsSummary, EVENT_NAME_CPU_STATS, \
    EVENT_NAME_DISK_STATS, EVENT_NAME_MEMORY_STATS, EVENT_NAME_NETWORK_STATS
from jade.loggers import setup_logging
from jade.resource_monitor import ResourceMonitor, CpuStatsViewer, \
    DiskStatsViewer, MemoryStatsViewer, NetworkStatsViewer
from jade.utils.subprocess_manager import run_command


def test_resource_stats():
    with tempfile.TemporaryDirectory() as tmpdir:
        event_file = os.path.join(tmpdir, "events.log")
        setup_logging("event", event_file, console_level=logging.ERROR,
                      file_level=logging.INFO)

        resource_monitor = ResourceMonitor("test")
        count = 2
        found_cpu = 0
        found_disk = 0
        found_mem = 0
        found_net = 0
        for i in range(count):
            resource_monitor.log_resource_stats()

        summary = EventsSummary(tmpdir)
        assert len(summary.events) == count * 4
        for event in summary.events:
            if event.name == EVENT_NAME_CPU_STATS:
                found_cpu += 1
            elif event.name == EVENT_NAME_DISK_STATS:
                found_disk += 1
            elif event.name == EVENT_NAME_MEMORY_STATS:
                found_mem += 1
            elif event.name == EVENT_NAME_NETWORK_STATS:
                found_net += 1
        assert found_cpu == count
        assert found_disk == count
        assert found_mem == count
        assert found_net == count

        viewers = [
            CpuStatsViewer(summary.events),
            DiskStatsViewer(summary.events),
            MemoryStatsViewer(summary.events),
            NetworkStatsViewer(summary.events),
        ]
        for viewer in viewers:
            df = viewer.get_dataframe("test")
            assert len(df) == 2
            if isinstance(viewer, MemoryStatsViewer):
                mem_df =  viewer.get_dataframe("test")
                averages = viewer._calc_batch_averages("test")
                for field, val in averages.items():
                    assert val == df[field].mean()

        output = {}
        cmd = f"jade stats show -o {tmpdir} cpu disk mem net"
        ret = run_command(cmd, output=output)
        assert ret == 0
        for term in ("IOPS", "read_bytes", "buffers", "bytes_recv", "idle"):
            assert term in output["stdout"]
