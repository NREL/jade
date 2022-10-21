"""
Unit tests for resubmitting failed and missing jobs
"""

import os
import shutil

import pytest

from jade.common import RESULTS_FILE
from jade.extensions.generic_command import GenericCommandInputs
from jade.extensions.generic_command import GenericCommandConfiguration
from jade.events import EventsSummary, EVENT_NAME_HPC_SUBMIT
from jade.jobs.results_aggregator import ResultsAggregator
from jade.result import Result, ResultsSummary
from jade.test_common import FAKE_HPC_CONFIG
from jade.utils.subprocess_manager import run_command
from jade.utils.utils import load_data, dump_data


TEST_FILENAME = "test-inputs.txt"
CONFIG_FILE = "test-config.json"
OUTPUT = "test-output"
SUBMIT_JOBS = f"jade submit-jobs -h {FAKE_HPC_CONFIG} -R periodic -r 1"
WAIT = "jade wait"
NUM_COMMANDS = 5


@pytest.fixture
def cleanup():
    _do_cleanup()
    commands = [
        'echo "hello"',
        "ls invalid-path",
        'echo "hello"',
        'echo "hello"',
        'echo "hello"',
        'echo "hello"',
        'echo "hello"',
        'echo "hello"',
    ]
    with open(TEST_FILENAME, "w") as f_out:
        for command in commands:
            f_out.write(command + "\n")

    inputs = GenericCommandInputs(TEST_FILENAME)
    config = GenericCommandConfiguration.auto_config(inputs, cancel_on_blocking_job_failure=True)
    config.get_job("3").set_blocking_jobs(set([2]))
    config.get_job("4").set_blocking_jobs(set([3]))
    config.get_job("5").set_blocking_jobs(set([4]))
    config.get_job("6").set_blocking_jobs(set([5]))
    config.get_job("7").set_blocking_jobs(set([6]))
    config.get_job("8").set_blocking_jobs(set([7]))
    config.dump(CONFIG_FILE)
    yield
    _do_cleanup()


def _do_cleanup():
    for path in (TEST_FILENAME, CONFIG_FILE, OUTPUT):
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)


def test_cancel_on_failure_detect_by_submitter(cleanup):
    # HpcSubmitter handles the cancellation because the blocked job will be in the 2nd batch.
    cmd = f"{SUBMIT_JOBS} {CONFIG_FILE} --output={OUTPUT} -n2 -b2"
    ret = run_command(cmd)
    assert ret == 0
    ret = run_command(f"{WAIT} --output={OUTPUT} -p 0.1 -t 2")
    assert ret == 0

    summary = ResultsSummary(OUTPUT)
    assert len(summary.get_successful_results()) == 1
    assert len(summary.get_failed_results()) == 1
    assert len(summary.get_canceled_results()) == 6
    results = summary.get_results_by_type()
    assert len(results["successful"]) == 1
    assert len(results["failed"]) == 1
    assert len(results["canceled"]) == 6


def test_cancel_on_failure_detect_by_runner(cleanup):
    # JobRunner handles the cancellation in JobQueue because the blocked job is in the batch
    # along with the blocking job.
    cmd = f"{SUBMIT_JOBS} {CONFIG_FILE} --output={OUTPUT} -n2 -b8"
    ret = run_command(cmd)
    assert ret == 0
    ret = run_command(f"{WAIT} --output={OUTPUT} -p 0.1 -t 2")
    assert ret == 0

    summary = ResultsSummary(OUTPUT)
    assert len(summary.get_successful_results()) == 1
    assert len(summary.get_failed_results()) == 1
    assert len(summary.get_canceled_results()) == 6
    results = summary.get_results_by_type()
    assert len(results["successful"]) == 1
    assert len(results["failed"]) == 1
    assert len(results["canceled"]) == 6
