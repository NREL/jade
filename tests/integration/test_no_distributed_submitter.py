"""
Unit tests for disabling the distributed submitter
"""

import os
import shutil
import time
from pathlib import Path

import pytest

from jade.common import RESULTS_DIR
from jade.extensions.generic_command import GenericCommandInputs
from jade.extensions.generic_command import GenericCommandConfiguration
from jade.test_common import FAKE_HPC_CONFIG
from jade.utils.subprocess_manager import check_run_command


TEST_FILENAME = "test-inputs.txt"
CONFIG_FILE = "test-config.json"
OUTPUT = "test-output"
SUBMIT_JOBS = f"jade submit-jobs -h {FAKE_HPC_CONFIG} -R none"
TRY_SUBMIT_JOBS = "jade try-submit-jobs"
WAIT = "jade wait -p 0.1"
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


def test_no_distributed_submitter(cleanup):
    cmd = f"{SUBMIT_JOBS} {CONFIG_FILE} --output={OUTPUT} -p 0.1 -N --no-reports"
    check_run_command(cmd)

    results_file = Path(OUTPUT) / RESULTS_DIR / "results_batch_1.csv"
    processed_results_file = Path(OUTPUT) / "processed_results.csv"
    all_jobs_complete = False
    for _ in range(10):
        if results_file.exists():
            lines = results_file.read_text().splitlines()
            # The file has an extra line for the header.
            if len(lines) == NUM_COMMANDS + 1:
                all_jobs_complete = True
                break
        time.sleep(1)

    assert all_jobs_complete
    assert len(processed_results_file.read_text().splitlines()) == 1

    check_run_command(f"{TRY_SUBMIT_JOBS} {OUTPUT}")
    check_run_command(f"{WAIT} --output={OUTPUT} -p 0.1 -t2")
    assert len(processed_results_file.read_text().splitlines()) == NUM_COMMANDS + 1
    assert not results_file.exists()
