"""
Unit tests for auto-regression execution class methods and properties.
"""

import logging
import os
import shutil
import tempfile
from pathlib import Path

import pytest

from jade.common import STATS_SUMMARY_FILE
from jade.extensions.generic_command import GenericCommandInputs
from jade.extensions.generic_command import GenericCommandConfiguration
from jade.extensions.generic_command import GenericCommandParameters
from jade.result import ResultsSummary
from jade.test_common import FAKE_HPC_CONFIG
from jade.utils.run_command import check_run_command
from jade.utils.utils import load_data


TEST_FILENAME = "inputs.txt"
CONFIG_FILE = "test-config.json"
OUTPUT = "test-output"
SUBMIT_JOBS = "jade submit-jobs -f -R aggregation"
WAIT = "jade wait"
SETUP_SCRIPT = Path(tempfile.gettempdir()) / "jade_setup.sh"
TEARDOWN_SCRIPT = Path(tempfile.gettempdir()) / "jade_teardown.sh"


@pytest.fixture
def generic_command_fixture():
    yield
    for path in (TEST_FILENAME, CONFIG_FILE, OUTPUT, SETUP_SCRIPT, TEARDOWN_SCRIPT):
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

    SETUP_SCRIPT.write_text("echo setup > $JADE_RUNTIME_OUTPUT/jade_setup.txt")
    TEARDOWN_SCRIPT.write_text("echo teardown > $JADE_RUNTIME_OUTPUT/jade_teardown.txt")

    inputs = GenericCommandInputs(TEST_FILENAME)
    config = GenericCommandConfiguration(
        setup_command=f"bash {SETUP_SCRIPT}",
        teardown_command=f"bash {TEARDOWN_SCRIPT}",
    )
    for job_param in inputs.iter_jobs():
        config.add_job(job_param)
    assert config.get_num_jobs() == 2

    config.dump(CONFIG_FILE)

    cmds = (
        f"{SUBMIT_JOBS} {CONFIG_FILE} --output={OUTPUT} -p 0.1 -h {FAKE_HPC_CONFIG}",
        # Test with higher queue depth. This exercises the code paths but
        # doesn't actually verify the functionality.
        # The infrastructure to do that is currently lacking. TODO
        f"{SUBMIT_JOBS} {CONFIG_FILE} --output={OUTPUT} -p 0.1 -q 32 -h {FAKE_HPC_CONFIG}",
    )

    for cmd in cmds:
        check_run_command(cmd)
        check_run_command(f"{WAIT} --output={OUTPUT} --poll-interval=0.1 -t2")

    assert list(Path(OUTPUT).glob("*.sh"))
    assert (Path(OUTPUT) / "jade_setup.txt").read_text().strip() == "setup"
    assert (Path(OUTPUT) / "jade_teardown.txt").read_text().strip() == "teardown"
    check_run_command(f"jade prune-files {OUTPUT}")
    assert not list(Path(OUTPUT).glob("*.sh"))


def test_generic_command_parameters():
    cmd = "bash myscript.sh"
    job = GenericCommandParameters(command=cmd)
    with pytest.raises(AttributeError):
        job.extension = "invalid"

    assert not job.append_job_name
    assert not job.append_output_dir
    job.append_job_name = True
    job.append_output_dir = True
    assert job.append_output_dir
    assert job.append_job_name

    job = GenericCommandParameters(command=cmd, use_multi_node_manager=True, blocked_by=[1])
    assert job.append_output_dir
    assert isinstance(job.blocked_by, set)
    assert next(iter(job.blocked_by)) == "1"


def test_sorted_order(generic_command_fixture):
    with open(TEST_FILENAME, "w") as f_out:
        pass

    config = GenericCommandConfiguration()
    num_jobs = 20
    for i in range(num_jobs):
        job = GenericCommandParameters(command="echo hello")
        config.add_job(job)

    assert config.get_num_jobs() == num_jobs

    job_ids = [job.job_id for job in config.iter_jobs()]
    assert job_ids == list(range(1, num_jobs + 1))


def test_job_order(generic_command_fixture):
    num_jobs = 50
    commands = ["echo hello world"] * num_jobs

    with open(TEST_FILENAME, "w") as f_out:
        for command in commands:
            f_out.write(command + "\n")

    inputs = GenericCommandInputs(TEST_FILENAME)
    config = GenericCommandConfiguration()
    for job_param in inputs.iter_jobs():
        config.add_job(job_param)
    assert config.get_num_jobs() == num_jobs
    job = config.get_job("1")
    for i in range(10, 15):
        job.blocked_by.add(i)

    config.get_job("2").blocked_by.add("1")
    config.get_job("21").blocked_by.add("30")
    config.get_job("41").blocked_by.add("50")
    config.dump(CONFIG_FILE)

    cmd = (
        f"{SUBMIT_JOBS} {CONFIG_FILE} --output={OUTPUT} "
        "--per-node-batch-size=10 "
        "--max-nodes=4 "
        "--poll-interval=0.1 "
        f"--hpc-config {FAKE_HPC_CONFIG} "
        "--num-parallel-processes-per-node=10"
    )
    check_run_command(cmd)
    check_run_command(f"{WAIT} --output={OUTPUT} --poll-interval=0.1")

    result_summary = ResultsSummary(OUTPUT)
    results = result_summary.list_results()
    assert len(results) == num_jobs
    tracker = {x.name: x for x in results}

    for i in range(10, 15):
        assert tracker["1"].completion_time > tracker[str(i)].completion_time

    assert tracker["2"].completion_time > tracker["1"].completion_time
    assert tracker["21"].completion_time > tracker["30"].completion_time
    assert tracker["41"].completion_time > tracker["50"].completion_time

    # Verify that stats are summarized correctly with aggregation mode.
    stats_text = Path(OUTPUT) / "stats.txt"
    assert stats_text.exists()
    assert "Average" in stats_text.read_text()
    stats_json = Path(OUTPUT) / STATS_SUMMARY_FILE
    assert stats_json.exists()
    stats = load_data(stats_json)
    assert stats
    assert "batch" in stats[0]
