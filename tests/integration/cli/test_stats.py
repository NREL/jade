import os
import shutil

import pytest

from jade.utils.subprocess_manager import run_command


# This needs to be updated the current format of resource stats.
@pytest.mark.skip
def test_stats__plot(example_output):
    path = os.path.join(example_output, "stats")
    try:
        ret = run_command(f"jade stats plot -o {example_output}")
        assert ret == 0
        for stat in ("Cpu", "Disk", "Memory", "Network"):
            filename = os.path.join(path, stat + "StatsViewer__resource_monitor_batch_0.html")
            assert os.path.exists(filename)
    finally:
        if os.path.exists(path):
            shutil.rmtree(path)


# This needs to be updated the current format of resource stats.
@pytest.mark.skip
def test_stats__show(example_output):
    output = {}
    ret = run_command(f"jade stats show -o {example_output}", output)
    assert ret == 0
    assert len(output["stdout"]) > 0

    for text in ("Network stat", "Memory stat", "Disk stat", "CPU stat"):
        assert text in output["stdout"]


def test_stats__bytes_consumed(example_output):
    output = {}
    ret = run_command(f"jade stats bytes-consumed -o {example_output}", output)
    assert ret == 0
    assert len(output["stdout"]) > 0

    ret = run_command(f"jade stats bytes-consumed --no-human-readable -o {example_output}", output)
    assert ret == 0
    bytes_consumed = int(output["stdout"].strip())
    assert bytes_consumed > 0


def test_stats__exec_time(example_output):
    output = {}
    ret = run_command(f"jade stats exec-time -o {example_output}", output)
    assert ret == 0
    assert len(output["stdout"]) > 0

    ret = run_command(f"jade stats exec-time --no-human-readable -o {example_output}", output)
    assert ret == 0
    exec_time = float(output["stdout"].strip())
    assert exec_time > 0
