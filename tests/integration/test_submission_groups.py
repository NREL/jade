"""Unit tests for submission groups"""

import copy
import os
import shutil
from pathlib import Path

import pytest

from jade.extensions.generic_command import GenericCommandInputs
from jade.extensions.generic_command import GenericCommandConfiguration
from jade.hpc.common import HpcType
from jade.jobs.job_configuration_factory import create_config_from_file
from jade.models import SubmissionGroup, SubmitterParams
from jade.test_common import FAKE_HPC_CONFIG
from jade.utils.run_command import check_run_command, run_command
from jade.utils.utils import load_data


TEST_FILENAME = "test-inputs.txt"
CONFIG_FILE = "test-config.json"
OUTPUT = "test-output"
SUBMIT_JOBS = "jade submit-jobs -R none"


@pytest.fixture
def cleanup():
    yield
    for path in (TEST_FILENAME, CONFIG_FILE, OUTPUT):
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)


def test_submission_groups(cleanup):
    config = create_config()
    config.dump(CONFIG_FILE)

    cmd = f"{SUBMIT_JOBS} {CONFIG_FILE} --output={OUTPUT} -h {FAKE_HPC_CONFIG} -p 0.1"
    check_run_command(cmd)

    output_path = Path(OUTPUT)
    config_batch_files = list(output_path.glob("config_batch*.json"))
    assert len(config_batch_files) == 3
    batch1 = load_data(output_path / "config_batch_1.json")
    assert len(batch1["jobs"]) == 3
    batch2 = load_data(output_path / "config_batch_2.json")
    assert len(batch2["jobs"]) == 1
    assert batch2["jobs"][0]["job_id"] == 4
    batch3 = load_data(output_path / "config_batch_3.json")
    assert len(batch3["jobs"]) == 1
    assert batch3["jobs"][0]["job_id"] == 5


def test_submission_groups_duplicate_name(cleanup):
    config = create_config()
    config.submission_groups[0].name = config.submission_groups[1].name
    config.dump(CONFIG_FILE)
    cmd = f"{SUBMIT_JOBS} {CONFIG_FILE} --output={OUTPUT} -h {FAKE_HPC_CONFIG} --dry-run"
    assert run_command(cmd) != 0


def test_submission_groups_mixed_hpc_types(cleanup):
    config = create_config()
    config.submission_groups[0].submitter_params.hpc_config.hpc_type = HpcType.SLURM
    config.dump(CONFIG_FILE)
    cmd = f"{SUBMIT_JOBS} {CONFIG_FILE} --output={OUTPUT} -h {FAKE_HPC_CONFIG} --dry-run"
    assert run_command(cmd) != 0


def test_submission_groups_mixed_max_nodes(cleanup):
    config = create_config()
    config.submission_groups[0].submitter_params.max_nodes = 5
    config.dump(CONFIG_FILE)
    cmd = f"{SUBMIT_JOBS} {CONFIG_FILE} --output={OUTPUT} -h {FAKE_HPC_CONFIG} --dry-run"
    assert run_command(cmd) != 0


def test_submission_groups_per_node_setup(cleanup):
    # TODO: this test is no longer in the right place. Belongs in file testing job_config.
    config = create_config()
    config.node_setup_command = "node_setup.sh"
    config.node_teardown_command = "node_teardown.sh"
    config.dump(CONFIG_FILE)
    cmd = f"{SUBMIT_JOBS} {CONFIG_FILE} --output={OUTPUT} -h {FAKE_HPC_CONFIG} --dry-run"
    check_run_command(cmd)
    config = create_config_from_file(Path(OUTPUT) / "config_batch_2.json")
    assert config.node_setup_command == "node_setup.sh"
    assert config.node_teardown_command == "node_teardown.sh"


def create_config():
    num_commands = 5
    commands = ['echo "hello world"'] * num_commands
    with open(TEST_FILENAME, "w") as f_out:
        for command in commands:
            f_out.write(command + "\n")

    inputs = GenericCommandInputs(TEST_FILENAME)
    config = GenericCommandConfiguration(job_inputs=inputs)
    jobs = list(inputs.iter_jobs())
    for i, job_param in enumerate(jobs):
        if i < 3:
            job_param.submission_group = "group1"
        else:
            job_param.submission_group = "group2"
        config.add_job(job_param)

    hpc_config1 = load_data(FAKE_HPC_CONFIG)
    hpc_config2 = copy.deepcopy(hpc_config1)
    hpc_config1["hpc"]["walltime"] = "1:00:00"
    hpc_config2["hpc"]["walltime"] = "5:00:00"
    params1 = SubmitterParams(hpc_config=hpc_config1, per_node_batch_size=3)
    params2 = SubmitterParams(hpc_config=hpc_config2, per_node_batch_size=1)
    group1 = SubmissionGroup(name="group1", submitter_params=params1)
    group2 = SubmissionGroup(name="group2", submitter_params=params2)
    config.append_submission_group(group1)
    config.append_submission_group(group2)
    return config
