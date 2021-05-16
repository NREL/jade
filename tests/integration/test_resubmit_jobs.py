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
SUBMIT_JOBS = f"jade submit-jobs -h {FAKE_HPC_CONFIG}"
RESUBMIT_JOBS = "jade resubmit-jobs"
WAIT = "jade wait"
NUM_COMMANDS = 5


@pytest.fixture
def cleanup():
    _do_cleanup()
    commands = ['echo "hello world"'] * NUM_COMMANDS
    with open(TEST_FILENAME, "w") as f_out:
        for command in commands:
            f_out.write(command + "\n")

    inputs = GenericCommandInputs(TEST_FILENAME)
    config = GenericCommandConfiguration.auto_config(inputs)
    config.dump(CONFIG_FILE)
    yield
    _do_cleanup()


def _do_cleanup():
    for path in (TEST_FILENAME, CONFIG_FILE, OUTPUT):
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)


def test_resubmit_failed(cleanup):
    cmd = f"{SUBMIT_JOBS} {CONFIG_FILE} --output={OUTPUT}"
    ret = run_command(cmd)
    assert ret == 0
    ret = run_command(f"{WAIT} --output={OUTPUT} -p 0.01")
    assert ret == 0

    agg = ResultsAggregator.load(OUTPUT)
    results = agg.get_results_unsafe()
    assert results
    for result in results:
        assert result.return_code == 0
    x = results[0]
    results[0] = Result(x.name, 1, x.status, x.exec_time_s, x.completion_time)
    agg._write_results(results)

    results_filename = os.path.join(OUTPUT, RESULTS_FILE)
    final_results = load_data(results_filename)
    final_results["results"][0]["return_code"] = 1
    final_results["results_summary"]["num_failed"] = 1
    final_results["results_summary"]["num_successful"] -= 1
    dump_data(final_results, results_filename)

    summary = ResultsSummary(OUTPUT)
    assert summary.get_failed_results()[0].name == "1"

    ret = run_command(f"{RESUBMIT_JOBS} {OUTPUT}")
    assert ret == 0
    ret = run_command(f"{WAIT} --output={OUTPUT} -p 0.01")
    assert ret == 0

    summary = ResultsSummary(OUTPUT)
    assert len(summary.get_successful_results()) == NUM_COMMANDS


def test_resubmit_missing(cleanup):
    cmd = f"{SUBMIT_JOBS} {CONFIG_FILE} --output={OUTPUT}"
    ret = run_command(cmd)
    assert ret == 0
    ret = run_command(f"{WAIT} --output={OUTPUT} -p 0.01")
    assert ret == 0

    agg = ResultsAggregator.load(OUTPUT)
    results = agg.get_results_unsafe()
    assert results
    for result in results:
        assert result.return_code == 0
    results.pop()
    agg._write_results(results)

    results_filename = os.path.join(OUTPUT, RESULTS_FILE)
    final_results = load_data(results_filename)
    missing = final_results["results"].pop()
    final_results["results_summary"]["num_missing"] = 1
    final_results["results_summary"]["num_successful"] -= 1
    final_results["missing_jobs"] = [missing["name"]]
    dump_data(final_results, results_filename)

    summary = ResultsSummary(OUTPUT)
    assert len(summary.get_failed_results()) == 0
    assert len(summary.get_successful_results()) == NUM_COMMANDS - 1

    ret = run_command(f"{RESUBMIT_JOBS} {OUTPUT}")
    assert ret == 0
    ret = run_command(f"{WAIT} --output={OUTPUT} -p 0.01")
    assert ret == 0

    summary = ResultsSummary(OUTPUT)
    assert len(summary.get_successful_results()) == NUM_COMMANDS
