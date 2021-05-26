"""Unit tests for dry-run mode"""

import os
import shutil

import pytest

from jade.extensions.generic_command import GenericCommandInputs
from jade.extensions.generic_command import GenericCommandConfiguration
from jade.test_common import SLURM_HPC_CONFIG
from jade.utils.run_command import check_run_command


TEST_FILENAME = "test-inputs.txt"
CONFIG_FILE = "test-config.json"
OUTPUT = "test-output"
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


def test_dry_run(cleanup):
    cmd = f"jade submit-jobs --dry-run -h {SLURM_HPC_CONFIG} {CONFIG_FILE} --output={OUTPUT}"
    check_run_command(cmd)
