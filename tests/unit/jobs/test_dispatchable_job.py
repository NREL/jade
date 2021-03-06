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
from jade.jobs.results_aggregator import ResultsAggregator


@pytest.fixture
def dispatchable_job():
    """Dispatchable job fixture"""
    job = mock.MagicMock()
    job.name = "Test-Job"
    cmd = "echo 'Hello World'"
    output = os.path.join(tempfile.gettempdir(), "jade-test-dispatchable-job")
    os.makedirs(output, exist_ok=True)
    results_agg = ResultsAggregator.create(output)
    dispatchable_job = DispatchableJob(job, cmd, output)
    yield dispatchable_job
    shutil.rmtree(output)


def test_dispatchable_job__properties(dispatchable_job):
    """Test DispatchableJob properties"""
    assert dispatchable_job.name == "Test-Job"
    assert dispatchable_job._cli_cmd == "echo 'Hello World'"
    assert os.path.exists(dispatchable_job._output)
    assert dispatchable_job._pipe is None
    assert dispatchable_job._is_pending is False


def test_dispatchable_job__run(dispatchable_job):
    """Should run job and write result into file when complete"""
    dispatchable_job.run()
    assert dispatchable_job._pipe is not None
    assert dispatchable_job._is_pending is True


def test_dispatchable_job__is_complete(dispatchable_job):
    """Should generate result file as expected"""
    dispatchable_job.run()

    while not dispatchable_job.is_complete():
        time.sleep(0.1)
        continue
