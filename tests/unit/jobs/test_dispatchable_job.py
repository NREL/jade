"""
Unit tests for dispatchable job class methods
"""
import os
import shutil
import tempfile
import time

import mock
import pytest

from jade.jobs.dispatchable_job import DispatchableJob


@pytest.fixture
def dispatchable_job():
    """Dispatchable job fixture"""
    job = mock.MagicMock()
    job.name = "Test-Job"
    cmd = "echo 'Hello World'"
    output = os.path.join(tempfile.gettempdir(), "jade-test-dispatchable-job")
    dispatchable_job = DispatchableJob(job, cmd, output)
    return dispatchable_job


def test_dispatchable_job__properties(dispatchable_job):
    """Test DispatchableJob properties"""
    assert dispatchable_job.name() == "Test-Job"
    assert dispatchable_job._cli_cmd == "echo 'Hello World'"
    assert os.path.exists(dispatchable_job._output)
    assert dispatchable_job._pipe is None
    assert os.path.exists(dispatchable_job._results_dir)
    assert dispatchable_job._suffix == ""
    assert dispatchable_job._is_pending is False


def test_dispatchable_job__run(dispatchable_job):
    """Should run job and write result into file when complete"""
    dispatchable_job.run()
    assert dispatchable_job._pipe is not None
    assert dispatchable_job._is_pending is True


def test_dispatchable_job__is_complete(dispatchable_job):
    """Should generate result file as expected"""
    dispatchable_job.set_results_filename_suffix("echo")
    dispatchable_job.run()

    result_file = os.path.join(
        dispatchable_job._results_dir,
        f"Test-Job_echo.toml"
    )
    while not dispatchable_job.is_complete():
        time.sleep(0.1)
        continue

    assert os.path.exists(result_file)
