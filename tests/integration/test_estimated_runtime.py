import os
import shutil
from pathlib import Path

import pytest

from jade.common import RESULTS_FILE
from jade.exceptions import ExecutionError, InvalidParameter
from jade.extensions.generic_command import GenericCommandInputs
from jade.extensions.generic_command import GenericCommandConfiguration
from jade.events import EventsSummary, EVENT_NAME_HPC_SUBMIT
from jade.jobs.results_aggregator import ResultsAggregator
from jade.result import Result, ResultsSummary
from jade.test_common import FAKE_HPC_CONFIG
from jade.utils.run_command import check_run_command
from jade.utils.utils import load_data, dump_data


TEST_FILENAME = "test-inputs.txt"
CONFIG_FILE = "test-config.json"
OUTPUT = "test-output"
SUBMIT_JOBS = f"jade submit-jobs -h {FAKE_HPC_CONFIG} -R none"
RESUBMIT_JOBS = "jade resubmit-jobs"
WAIT = "jade wait"
NUM_COMMANDS = 100


@pytest.fixture
def cleanup():
    _do_cleanup()
    commands = ['echo "hello world"'] * NUM_COMMANDS
    with open(TEST_FILENAME, "w") as f_out:
        for command in commands:
            f_out.write(command + "\n")

    inputs = GenericCommandInputs(TEST_FILENAME)
    config = GenericCommandConfiguration.auto_config(inputs, minutes_per_job=10)
    config.dump(CONFIG_FILE)
    yield
    _do_cleanup()


@pytest.fixture
def job_too_long():
    _do_cleanup()
    commands = ['echo "hello world"'] * NUM_COMMANDS
    with open(TEST_FILENAME, "w") as f_out:
        for command in commands:
            f_out.write(command + "\n")

    inputs = GenericCommandInputs(TEST_FILENAME)
    config = GenericCommandConfiguration.auto_config(inputs, minutes_per_job=10)
    for i, job in enumerate(config.iter_jobs()):
        if i == 1:
            job.estimated_run_minutes = 1000
            break
    config.dump(CONFIG_FILE)
    yield
    _do_cleanup()


def _do_cleanup():
    for path in (TEST_FILENAME, CONFIG_FILE, OUTPUT):
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)


def test_estimated_run_time(cleanup):
    # walltime is 240 minutes
    # 10-minute jobs
    # Each of 4 cores can each complete 24 jobs. 4 * 24 = 96 jobs
    # 100 jobs will take two batches.
    cmd = f"{SUBMIT_JOBS} {CONFIG_FILE} --output={OUTPUT} -p 0.1 -t -n2 -q4"
    check_run_command(cmd)
    check_run_command(f"{WAIT} --output={OUTPUT} -p 0.1 -t2")

    batch_config_1 = Path(OUTPUT) / "config_batch_1.json"
    assert os.path.exists(batch_config_1)
    batch_config_2 = Path(OUTPUT) / "config_batch_2.json"
    assert os.path.exists(batch_config_2)

    config1 = load_data(batch_config_1)
    assert len(config1["jobs"]) == 96
    config2 = load_data(batch_config_2)
    assert len(config2["jobs"]) == 4


def test_estimated_run_time_too_long(job_too_long):
    cmd = f"{SUBMIT_JOBS} {CONFIG_FILE} --output={OUTPUT}"
    with pytest.raises(ExecutionError):
        check_run_command(cmd)
