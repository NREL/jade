"""
Unit tests for auto-regression execution class methods and properties.
"""

import os
import shutil

import pytest

from jade.extensions.generic_command.generic_command_inputs import GenericCommandInputs
from jade.extensions.generic_command.generic_command_configuration import GenericCommandConfiguration
from jade.events import EventsSummary, EVENT_NAME_HPC_SUBMIT
from jade.utils.subprocess_manager import run_command


TEST_FILENAME = "inputs.txt"
CONFIG_FILE = "test-config.json"
OUTPUT = "test-output"
SUBMIT_JOBS = "jade submit-jobs"


@pytest.fixture
def cleanup():
    yield
    for path in (TEST_FILENAME, CONFIG_FILE, OUTPUT):
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)
    if "FAKE_HPC_CLUSTER" in os.environ:
        os.environ.pop("FAKE_HPC_CLUSTER")


def test_try_add_blocked_jobs(cleanup):
    num_commands = 5
    commands = ["ls ."] * num_commands
    with open(TEST_FILENAME, "w") as f_out:
        for command in commands:
            f_out.write(command + "\n")

    inputs = GenericCommandInputs(TEST_FILENAME)
    config = GenericCommandConfiguration(job_inputs=inputs)
    jobs = list(inputs.iter_jobs())
    for i, job_param in enumerate(jobs):
        if i == num_commands - 1:
            job_param.blocked_by = set([1, 2, 3, 4])
        config.add_job(job_param)
    config.dump(CONFIG_FILE)

    os.environ["FAKE_HPC_CLUSTER"] = "True"
    for option in ("--try-add-blocked-jobs", "--no-try-add-blocked-jobs"):
        cmd = f"{SUBMIT_JOBS} {CONFIG_FILE} --output={OUTPUT} -p 0.1 {option}"
        ret = run_command(cmd)
        assert ret == 0
        events_file = os.path.join(OUTPUT, "submit_jobs_events.log")
        events_summary = EventsSummary(OUTPUT, preload=True)
        submit_events = events_summary.list_events(EVENT_NAME_HPC_SUBMIT)
        if option == "--try-add-blocked-jobs":
            assert len(submit_events) == 1
            event = submit_events[0]
            assert event.data["batch_size"] == num_commands
            assert event.data["num_blocked"] == 0
            shutil.rmtree(OUTPUT)
        else:
            assert len(submit_events) == 2
            event1 = submit_events[0]
            event2 = submit_events[1]
            assert event1.data["batch_size"] == num_commands - 1
            assert event2.data["batch_size"] == 1
            assert event1.data["num_blocked"] == 1
            assert event2.data["num_blocked"] == 0
