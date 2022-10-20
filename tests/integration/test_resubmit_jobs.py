"""
Unit tests for resubmitting failed and missing jobs
"""

import os
import shutil
from pathlib import Path

import pytest

from jade.common import RESULTS_FILE
from jade.extensions.generic_command import GenericCommandInputs
from jade.extensions.generic_command import GenericCommandConfiguration
from jade.jobs.results_aggregator import ResultsAggregator
from jade.result import Result, ResultsSummary
from jade.test_common import FAKE_HPC_CONFIG
from jade.utils.subprocess_manager import run_command, check_run_command
from jade.utils.utils import load_data, dump_data


TEST_FILENAME = "test-inputs.txt"
CONFIG_FILE = "test-config.json"
SG_FILE = "test-submission-groups.json"
OUTPUT = "test-output"
SUBMIT_JOBS = f"jade submit-jobs -h {FAKE_HPC_CONFIG} -R none"
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


@pytest.fixture
def basic_setup():
    _do_cleanup()
    yield
    _do_cleanup()


def _do_cleanup():
    for path in (TEST_FILENAME, CONFIG_FILE, OUTPUT, SG_FILE):
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)


def test_resubmit_successful(cleanup):
    cmd = f"{SUBMIT_JOBS} {CONFIG_FILE} --output={OUTPUT} -p 0.1"
    check_run_command(cmd)
    check_run_command(f"{WAIT} --output={OUTPUT} -p 0.1 -t2")
    summary = ResultsSummary(OUTPUT)
    assert len(summary.get_failed_results()) == 0
    assert len(summary.get_successful_results()) == NUM_COMMANDS

    check_run_command(f"jade config save-submission-groups {OUTPUT} -c {SG_FILE}")
    groups = load_data(SG_FILE)
    assert groups[0]["submitter_params"]["per_node_batch_size"] > NUM_COMMANDS
    groups[0]["submitter_params"]["per_node_batch_size"] = NUM_COMMANDS
    dump_data(groups, SG_FILE)

    check_run_command(f"{RESUBMIT_JOBS} {OUTPUT} -s {SG_FILE} --successful")
    check_run_command(f"{WAIT} --output={OUTPUT} -p 0.1")
    summary = ResultsSummary(OUTPUT)
    assert len(summary.get_failed_results()) == 0
    assert len(summary.get_successful_results()) == NUM_COMMANDS

    check_run_command(f"jade config save-submission-groups {OUTPUT} --force -c {SG_FILE}")
    groups = load_data(SG_FILE)
    assert groups[0]["submitter_params"]["per_node_batch_size"] == NUM_COMMANDS


def test_resubmit_failed(cleanup):
    cmd = f"{SUBMIT_JOBS} {CONFIG_FILE} --output={OUTPUT} -p 0.1"
    ret = run_command(cmd)
    assert ret == 0
    ret = run_command(f"{WAIT} --output={OUTPUT} -p 0.1")
    assert ret == 0

    agg = ResultsAggregator.load(OUTPUT)
    results = agg.get_results_unsafe()
    assert results
    for result in results:
        assert result.return_code == 0
    x = results[0]
    results[0] = Result(x.name, 1, x.status, x.exec_time_s, x.completion_time, hpc_job_id=None)
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
    ret = run_command(f"{WAIT} --output={OUTPUT} -p 0.1")
    assert ret == 0

    summary = ResultsSummary(OUTPUT)
    assert len(summary.get_successful_results()) == NUM_COMMANDS


def test_resubmit_missing(cleanup):
    cmd = f"{SUBMIT_JOBS} {CONFIG_FILE} --output={OUTPUT} -p 0.1"
    ret = run_command(cmd)
    assert ret == 0
    ret = run_command(f"{WAIT} --output={OUTPUT} -p 0.1")
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
    ret = run_command(f"{WAIT} --output={OUTPUT} -p 0.1")
    assert ret == 0

    summary = ResultsSummary(OUTPUT)
    assert len(summary.get_successful_results()) == NUM_COMMANDS


def test_resubmit_with_blocking_jobs(basic_setup):
    num_commands = 7
    commands = ['echo "hello world"'] * num_commands
    with open(TEST_FILENAME, "w") as f_out:
        for command in commands:
            f_out.write(command + "\n")

    inputs = GenericCommandInputs(TEST_FILENAME)
    config = GenericCommandConfiguration(job_inputs=inputs)
    jobs = list(inputs.iter_jobs())
    # Set an inefficient ordering to make sure the resubmit algorithm is recursive.
    for i, job_param in enumerate(jobs):
        if i == 3:
            job_param.blocked_by = set([5])
        elif i == 4:
            job_param.blocked_by = set([7])
        elif i == 6:
            job_param.blocked_by = set([6])
        config.add_job(job_param)
    config.dump(CONFIG_FILE)
    cmd = f"{SUBMIT_JOBS} {CONFIG_FILE} --output={OUTPUT}"
    ret = run_command(cmd)
    assert ret == 0
    ret = run_command(f"{WAIT} --output={OUTPUT} -p 0.1")
    assert ret == 0

    agg = ResultsAggregator.load(OUTPUT)
    results = agg.get_results_unsafe()
    assert results
    for result in results:
        assert result.return_code == 0
    found = False
    for i, result in enumerate(results):
        if result.name == "7":
            results.pop(i)
            found = True
            break
    assert found
    agg._write_results(results)

    results_filename = os.path.join(OUTPUT, RESULTS_FILE)
    final_results = load_data(results_filename)
    missing = None
    for i, result in enumerate(final_results["results"]):
        if result["name"] == "7":
            missing = result
            final_results["results"].pop(i)
            break
    assert missing is not None
    final_results["results_summary"]["num_missing"] = 1
    final_results["results_summary"]["num_successful"] -= 1
    final_results["missing_jobs"] = [missing["name"]]
    dump_data(final_results, results_filename)

    summary = ResultsSummary(OUTPUT)
    assert len(summary.get_failed_results()) == 0
    assert len(summary.get_successful_results()) == num_commands - 1
    first_batch = load_data(Path(OUTPUT) / "config_batch_1.json")
    assert len(first_batch["jobs"]) == num_commands

    ret = run_command(f"{RESUBMIT_JOBS} {OUTPUT}")
    assert ret == 0
    ret = run_command(f"{WAIT} --output={OUTPUT} -p 0.1")
    assert ret == 0

    summary = ResultsSummary(OUTPUT)
    assert len(summary.get_successful_results()) == num_commands

    second_batch_file = Path(OUTPUT) / "config_batch_2.json"
    assert second_batch_file.exists()
    second_batch = load_data(second_batch_file)["jobs"]
    assert len(second_batch) == 3
