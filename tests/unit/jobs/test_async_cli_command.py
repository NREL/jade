"""
Unit tests for AsyncCliCommand class methods
"""
import os
import shutil
import tempfile
import time

import mock
import pytest

from jade.jobs.async_cli_command import AsyncCliCommand
from jade.jobs.results_aggregator import ResultsAggregator


@pytest.fixture
def async_cmd():
    """Async CLI command fixture"""
    job = mock.MagicMock()
    job.name = "Test-Job"
    cmd = "echo 'Hello World'"
    output = os.path.join(tempfile.gettempdir(), "jade-test-async-cli-job")
    os.makedirs(output, exist_ok=True)
    ResultsAggregator.create(output)
    cmd = AsyncCliCommand(job, cmd, output)
    yield cmd
    shutil.rmtree(output)


def test_async_cmd__properties(async_cmd):
    """Test AsyncCliCommand properties"""
    assert async_cmd.name == "Test-Job"
    assert async_cmd._cli_cmd == "echo 'Hello World'"
    assert os.path.exists(async_cmd._output)
    assert async_cmd._pipe is None
    assert async_cmd._is_pending is False


def test_async_cmd__run(async_cmd):
    """Should run job and write result into file when complete"""
    async_cmd.run()
    assert async_cmd._pipe is not None
    assert async_cmd._is_pending is True


def test_async_cmd__is_complete(async_cmd):
    """Should generate result file as expected"""
    async_cmd.run()

    while not async_cmd.is_complete():
        time.sleep(0.1)
        continue
