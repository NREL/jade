"""
Unit tests for auto-regression execution class methods and properties.
"""

import os
import shutil

import pytest

from jade.extensions.generic_command.generic_command_inputs import GenericCommandInputs
from jade.extensions.generic_command.generic_command_configuration import GenericCommandConfiguration
from jade.extensions.generic_command.generic_command_execution import GenericCommandExecution
from jade.extensions.generic_command.generic_command_parameters import GenericCommandParameters
from jade.utils.subprocess_manager import run_command


TEST_FILENAME = "inputs.txt"
CONFIG_FILE = "test-config.json"
OUTPUT = "test-output"
SUBMIT_JOBS = "jade submit-jobs"


@pytest.fixture
def generic_command_fixture():
    for path in (TEST_FILENAME, CONFIG_FILE, OUTPUT):
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)


# TODO: make unit tests. This is an integration test to quickly get full
# coverage.


def test_run_generic_commands(generic_command_fixture):
    commands = [
        "ls .",
        "ls invalid-file-path",
    ]

    with open(TEST_FILENAME, "w") as f_out:
        for command in commands:
            f_out.write(command + "\n")

    inputs = GenericCommandInputs(TEST_FILENAME)
    config = GenericCommandConfiguration(inputs)
    config.dump(CONFIG_FILE)

    cmds = (
        f"{SUBMIT_JOBS} {CONFIG_FILE} --output={OUTPUT} -p 1",
        # Test with higher queue depth. This exercises the code paths but
        # doesn't actually verify the functionality.
        # The infrastructure to do that is currently lacking. TODO
        f"{SUBMIT_JOBS} {CONFIG_FILE} --output={OUTPUT} -p 1 -q 32",
    )

    for cmd in cmds:
        ret = run_command(cmd)
        assert ret == 0


def test_sorted_order(generic_command_fixture):
    with open(TEST_FILENAME, "w") as f_out:
        pass

    inputs = GenericCommandInputs(TEST_FILENAME)
    config = GenericCommandConfiguration(inputs)
    num_jobs = 20
    for i in range(num_jobs):
        job = GenericCommandParameters("echo hello")
        config.add_job(job)

    assert config.get_num_jobs() == num_jobs

    job_ids = [job.job_id for job in config.iter_jobs()]
    assert job_ids == list(range(1, num_jobs + 1))
